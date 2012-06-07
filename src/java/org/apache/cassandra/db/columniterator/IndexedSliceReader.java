/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db.columniterator;

import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.sstable.IndexHelper;
import org.apache.cassandra.io.sstable.IndexHelper.IndexInfo;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileMark;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

import com.google.common.collect.AbstractIterator;

/**
 * This is a reader that finds the block for a starting column and returns blocks before/after it for each next call.
 * This function assumes that the CF is sorted by name and exploits the name index.
 */
class IndexedSliceReader extends AbstractIterator<OnDiskAtom> implements OnDiskAtomIterator
{
    private final ColumnFamily emptyColumnFamily;

    private final SSTableReader sstable;
    private final List<IndexHelper.IndexInfo> indexes;
    private final FileDataInput originalInput;
    private FileDataInput file;
    private final boolean reversed;
    private final ColumnSlice[] ranges;
    private final BlockFetcher fetcher;
    private final AbstractType<?> comparator;

    /**
     * This slice reader assumes that ranges are sorted correctly, e.g. that for forward lookup ranges are in
     * lexicographic order of start elements and that for reverse lookup they are in reverse lexicographic order of
     * finish (reverse start) elements. i.e. forward: [a,b],[d,e],[g,h] reverse: [h,g],[e,d],[b,a]. This reader also
     * assumes that validation has been performed in terms of intervals (no overlapping intervals).
     */
    public IndexedSliceReader(SSTableReader sstable, RowIndexEntry indexEntry, FileDataInput input,
            ColumnSlice[] ranges, boolean reversed)
    {

        this.sstable = sstable;
        this.originalInput = input;
        this.reversed = reversed;
        this.ranges = ranges;
        this.comparator = sstable.metadata.comparator;

        try
        {
            if (sstable.descriptor.version.hasPromotedIndexes)
            {
                this.indexes = indexEntry.columnsIndex();
                if (indexes.isEmpty())
                {
                    setToRowStart(sstable, indexEntry, input);
                    this.emptyColumnFamily = ColumnFamily.create(sstable.metadata);
                    emptyColumnFamily.delete(DeletionInfo.serializer().deserializeFromSSTable(file,
                            sstable.descriptor.version));
                    fetcher = new SimpleBlockFetcher();
                }
                else
                {
                    this.emptyColumnFamily = ColumnFamily.create(sstable.metadata);
                    emptyColumnFamily.delete(indexEntry.deletionInfo());
                    fetcher = new IndexedBlockFetcher(indexEntry);
                }
            }
            else
            {
                setToRowStart(sstable, indexEntry, input);
                IndexHelper.skipBloomFilter(file);
                this.indexes = IndexHelper.deserializeIndex(file);
                this.emptyColumnFamily = ColumnFamily.create(sstable.metadata);
                emptyColumnFamily.delete(DeletionInfo.serializer().deserializeFromSSTable(file,
                        sstable.descriptor.version));
                fetcher = indexes.isEmpty() ? new SimpleBlockFetcher() : new IndexedBlockFetcher();
            }
        }
        catch (IOException e)
        {
            sstable.markSuspect();
            throw new IOError(e);
        }
    }

    /**
     * Sets the seek position to the start of the row for column scanning.
     */
    private void setToRowStart(SSTableReader reader, RowIndexEntry indexEntry, FileDataInput input) throws IOException
    {
        if (input == null)
        {
            this.file = sstable.getFileDataInput(indexEntry.position);
        }
        else
        {
            this.file = input;
            input.seek(indexEntry.position);
        }
        sstable.decodeKey(ByteBufferUtil.readWithShortLength(file));
        SSTableReader.readRowSize(file, sstable.descriptor);
    }

    public ColumnFamily getColumnFamily()
    {
        return emptyColumnFamily;
    }

    public DecoratedKey getKey()
    {
        throw new UnsupportedOperationException();
    }

    protected OnDiskAtom computeNext()
    {
        try
        {
            if (fetcher.hasNext())
            {
                return fetcher.next();
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        return endOfData();
    }

    public void close() throws IOException
    {
        if (originalInput == null && file != null)
            file.close();
    }

    private abstract class BlockFetcher
    {
        protected int currentSlice;
        protected final Deque<OnDiskAtom> prefetched = new ArrayDeque<OnDiskAtom>();
        protected final Deque<OnDiskAtom> blockColumns = new ArrayDeque<OnDiskAtom>();
        protected Pair<ByteBuffer, ByteBuffer> current;
        protected ByteBuffer start;
        protected ByteBuffer finish;
        protected boolean noMoreBlocks;
        protected boolean readBackwards;

        public BlockFetcher()
        {
            this(false);
        }

        public BlockFetcher(boolean readBackwards)
        {
            this.readBackwards = readBackwards;
            currentSlice = 0;
            if (readBackwards)
                currentSlice = ranges.length - 1;

            this.prepareSlice();
        }

        /**
         * Checks that the next slice exist and updates the start/finish pointers.
         */
        protected boolean nextSlice()
        {

            if (reversed && readBackwards)
                currentSlice--;
            else
                currentSlice++;

            // no more slices? we're done
            if (currentSlice >= ranges.length || currentSlice < 0)
            {
                noMoreBlocks = true;
                return false;
            }

            prepareSlice();
            return true;
        }

        protected void prepareSlice()
        {
            current = ranges[currentSlice];
            start = !reversed ? current.left : current.right;
            finish = !reversed ? current.right : current.left;
        }

        protected void addCol(OnDiskAtom col)
        {
            if (reversed)
                blockColumns.addFirst(col);
            else
                blockColumns.addLast(col);
        }

        public abstract boolean hasNext() throws IOException;

        public abstract OnDiskAtom next();

        protected boolean isColumnBeforeSliceStart(OnDiskAtom column)
        {
            return start.remaining() != 0 && comparator.compare(column.name(), start) < 0;
        }

        protected boolean isColumnBeforeSliceFinish(OnDiskAtom column)
        {
            return finish.remaining() == 0 || comparator.compare(column.name(), finish) <= 0;
        }

    }

    private class IndexedBlockFetcher extends BlockFetcher
    {

        // where this row starts
        private final long basePosition;
        // the current index position
        private int curRangeIndex;
        // current index
        private IndexInfo currentIndex;
        private OnDiskAtom currentColumn;

        public IndexedBlockFetcher() throws IOException
        {
            super();
            file.readInt();
            basePosition = file.getFilePointer();
            findIndexForFirstSlice();
        }

        public IndexedBlockFetcher(RowIndexEntry entry)
        {
            super();
            basePosition = entry.position;
            findIndexForFirstSlice();
        }

        private void findIndexForFirstSlice()
        {
            do
            {
                curRangeIndex = indexForSlice(currentSlice);
                if (curRangeIndex < 0 || curRangeIndex >= indexes.size())
                    continue;
                currentIndex = indexes.get(curRangeIndex);
                break;
            } while (nextSlice());
            if (curRangeIndex < 0 || curRangeIndex >= indexes.size())
                noMoreBlocks = true;
        }

        @Override
        public boolean hasNext() throws IOException
        {
            while (true)
            {
                // poll from the ordered blocks
                currentColumn = blockColumns.poll();
                if (currentColumn != null)
                    return true;

                OnDiskAtom perfetchedCol;
                if (reversed && (perfetchedCol = prefetched.peek()) != null)
                {

                    // col is before slice, we update the slice
                    if (isColumnBeforeSliceStart(perfetchedCol))
                    {

                        if (!nextSlice())
                            return false;

                        continue;
                    }

                    // col is within slice
                    if (isColumnBeforeSliceFinish(perfetchedCol))
                    {
                        currentColumn = prefetched.poll();
                        return true;
                    }

                    // col is after slice, discard
                    prefetched.poll();
                    continue;
                }

                if (noMoreBlocks)
                    return false;

                // try and get more from disk
                getMoreBlocks();
            }
        }

        @Override
        public OnDiskAtom next()
        {
            return currentColumn;
        }

        public void getMoreBlocks() throws IOException
        {

            /* seek to the correct offset to the data, and calculate the data size */
            long positionToSeek = basePosition + currentIndex.offset;

            // With new promoted indexes, our first seek in the data file will happen at that point.
            if (file == null)
                file = originalInput == null ? sstable.getFileDataInput(positionToSeek) : originalInput;

            OnDiskAtom.Serializer atomSerializer = emptyColumnFamily.getOnDiskSerializer();
            file.seek(positionToSeek);
            FileMark mark = file.mark();

            // scan from index start
            while (file.bytesPastMark(mark) < currentIndex.width)
            {

                OnDiskAtom column = atomSerializer.deserializeFromSSTable(file, sstable.descriptor.version);

                // col is before slice
                if (isColumnBeforeSliceStart(column))
                {

                    if (reversed)
                        prefetched.addFirst(column);

                    continue;
                }

                // col is within slice
                if (isColumnBeforeSliceFinish(column))
                {
                    addCol(column);
                    continue;
                }

                // col is after slice.
                if (reversed)
                    break;

                if (nextSlice() && indexForSlice(currentSlice) == curRangeIndex)
                    continue;
                break;

            }
            if (!nextIndex())
                noMoreBlocks = true;
        }

        private boolean nextIndex()
        {
            if (reversed)
                curRangeIndex--;
            else
                curRangeIndex++;

            if (curRangeIndex < 0 || curRangeIndex >= indexes.size())
                return false;

            currentIndex = indexes.get(curRangeIndex);
            return true;
        }

        private int indexForSlice(int slice)
        {
            return IndexHelper.indexFor(reversed ? ranges[slice].right : ranges[slice].left,
                    indexes,
                    comparator, reversed);
        }
    }

    private class SimpleBlockFetcher extends BlockFetcher
    {

        private Iterator<OnDiskAtom> iterator;

        private SimpleBlockFetcher() throws IOException
        {
            // since we have to deserialize in order and will read all ranges might as well reverse the ranges and
            // behave as if it was not reversed
            super(reversed);

            OnDiskAtom.Serializer atomSerializer = emptyColumnFamily.getOnDiskSerializer();
            int columns = file.readInt();

            for (int i = 0; i < columns; i++)
            {
                OnDiskAtom column = atomSerializer.deserializeFromSSTable(file, sstable.descriptor.version);

                // col is before slice
                if (isColumnBeforeSliceStart(column))
                    continue;

                // col is within slice
                if (isColumnBeforeSliceFinish(column))
                    addCol(column);

                // col is after slice. more slices?
                else if (!nextSlice())
                    break;
            }

            this.iterator = blockColumns.iterator();
        }

        @Override
        public boolean hasNext()
        {
            return iterator.hasNext();
        }

        @Override
        public OnDiskAtom next()
        {
            return iterator.next();
        }

    }
}
