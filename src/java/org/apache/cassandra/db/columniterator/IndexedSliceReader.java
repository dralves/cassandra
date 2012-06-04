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
import java.util.ArrayList;
import java.util.Deque;
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
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.lang.ArrayUtils;

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
    private final SliceRange[] ranges;

    private final BlockFetcher fetcher;
    private final Deque<OnDiskAtom> blockColumns = new ArrayDeque<OnDiskAtom>();
    private final AbstractType<?> comparator;

    private boolean isDone;

    /**
     * This slice reader assumes that ranges are sorted correctly, e.g. that for forward lookup ranges are in
     * lexicographic order of start elements and that for reverse lookup they are in reverse lexicographic order of
     * finish (reverse start) elements. i.e. forward: [a,b],[d,e],[g,h] reverse: [h,g],[e,d],[b,a]. This reader also
     * assumes that validation has been performed in terms of intervals (no overlapping intervals).
     */
    public IndexedSliceReader(SSTableReader sstable, RowIndexEntry indexEntry, FileDataInput input,
            SliceRange[] ranges, boolean reversed)
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
        while (true)
        {
            // previously fetched blocks
            OnDiskAtom column = blockColumns.poll();

            if (column != null)
                return column;

            // if there are no previous blocks we might be done
            if (isDone)
                return endOfData();
            // we're not, fetch more
            else
                try
                {
                    fetcher.getMoreBlocks();
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
        }
    }

    public void close() throws IOException
    {
        if (originalInput == null && file != null)
            file.close();
    }

    private abstract class BlockFetcher
    {
        protected int sliceIndex = 0;
        protected SliceRange current;
        protected ByteBuffer start;
        protected ByteBuffer finish;

        public BlockFetcher()
        {
            this(false);
        }

        public BlockFetcher(boolean reverseRanges)
        {
            if (reverseRanges)
                ArrayUtils.reverse(ranges);

            nextSlice();
        }

        public abstract void getMoreBlocks() throws IOException;

        /**
         * Prepare the next slice if there is one.
         */
        protected boolean nextSlice()
        {
            // no more slices we're done
            if (sliceIndex >= ranges.length)
            {
                isDone = true;
                return false;
            }

            // update current and reverse order if needed
            current = ranges[sliceIndex];
            start = !reversed ? current.start : current.finish;
            finish = !reversed ? current.finish : current.start;
            sliceIndex++;
            return true;
        }

        protected void addCol(OnDiskAtom col)
        {
            if (reversed)
                blockColumns.addFirst(col);
            else
                blockColumns.addLast(col);
        }

    }

    private class IndexedBlockFetcher extends BlockFetcher
    {

        /** holds read cols that might be needed later. for reverse reads only */
        private final List<OnDiskAtom> cache = new ArrayList<OnDiskAtom>();
        /** the index segment that is cached **/
        private int cachedIndex;

        // where this row starts
        private final long basePosition;
        // the current index position
        private int curRangeIndex;
        // current index
        private IndexInfo curColPosition;

        public IndexedBlockFetcher() throws IOException
        {
            file.readInt();
            basePosition = file.getFilePointer();
        }

        public IndexedBlockFetcher(RowIndexEntry entry)
        {
            basePosition = entry.position;
        }

        @Override
        public void getMoreBlocks() throws IOException
        {

            // if we're running reversed we might have previously deserialized this range
            if (reversed && cachedIndex == curRangeIndex && !cache.isEmpty())
            {
                for (OnDiskAtom cached : cache)
                {
                    // col is before slice
                    if (start.remaining() != 0 && comparator.compare(cached.name(), start) < 0)
                        continue;

                    // col is within slice
                    if (finish.remaining() == 0 || comparator.compare(cached.name(), finish) <= 0)
                        addCol(cached);
                }
            }
            else
            {

                /* seek to the correct offset to the data, and calculate the data size */
                long positionToSeek = basePosition + curColPosition.offset;

                // With new promoted indexes, our first seek in the data file will happen at that point.
                if (file == null)
                    file = originalInput == null ? sstable.getFileDataInput(positionToSeek) : originalInput;

                int prevRangeIndex = curRangeIndex;

                OnDiskAtom.Serializer atomSerializer = emptyColumnFamily.getOnDiskSerializer();
                file.seek(positionToSeek);
                FileMark mark = file.mark();

                // scan from index start
                while (file.bytesPastMark(mark) < curColPosition.width)
                {
                    OnDiskAtom column = atomSerializer.deserializeFromSSTable(file, sstable.descriptor.version);
                    // col is before slice
                    if (start.remaining() != 0 && comparator.compare(column.name(), start) < 0)
                    {
                        // if we're reading reversed cache the values, we might need them because 'next' ranges are
                        // actually before this one
                        if (reversed)
                        {
                            cache.add(column);
                            cachedIndex = curRangeIndex;
                        }
                        continue;
                    }

                    // col is within slice
                    if (finish.remaining() == 0 || comparator.compare(column.name(), finish) <= 0)
                        addCol(column);

                    // col is after slice.
                    else
                    {
                        // if we're reading reversed we're sure that no col after finish will be needed.
                        if (reversed)
                            break;
                        // when reading forward we check for the next slice and whether it's 'start' is still within
                        // this index's range, if so we continue, if not we return
                        else if (nextSlice() && prevRangeIndex == curRangeIndex)
                            continue;
                        else
                            return;
                    }

                }
            }

            // if we reach this point we're at the end of an index range
            if (reversed)
            {
                // if we're reading reversed this range might continue to the previous index segment (and we're sure we
                // won't need the cache)
                if (comparator.compare(start, curColPosition.firstName) < 0)
                {
                    curRangeIndex--;
                    cache.clear();
                    cachedIndex = -1;
                    updateIndexPosition();
                }
                // if not try the next slice
                else
                    nextSlice();

            }
            else
            {
                curRangeIndex++;
                updateIndexPosition();
            }
        }

        /**
         * Next slice in indexed fetcher might make the index jump.
         */
        @Override
        protected boolean nextSlice()
        {
            if (super.nextSlice())
            {
                // if there are more slices search for the next index (start by the indexed segment that hold the
                // 'start' for a forward range and the one that holds 'finish' for a reversed range so that we don't
                // read more that we need)
                curRangeIndex = IndexHelper.indexFor(reversed ? finish : start, indexes, comparator, reversed);
                // slice range falls out of the indexes
                return updateIndexPosition();
            }
            return false;
        }

        private boolean updateIndexPosition()
        {
            // slice range falls out of the indexes
            if (curRangeIndex < 0 || curRangeIndex >= indexes.size())
            {
                // there are more slices but they fall out of this row's col range
                isDone = true;
                return false;
            }
            curColPosition = indexes.get(curRangeIndex);
            return true;
        }
    }

    private class SimpleBlockFetcher extends BlockFetcher
    {

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
                if (start.remaining() != 0 && comparator.compare(column.name(), start) < 0)
                    continue;

                // col is within slice
                if (finish.remaining() == 0 || comparator.compare(column.name(), finish) <= 0)
                    addCol(column);

                // col is after slice. more slices?
                else if (!nextSlice())
                    break;
            }

            isDone = true;
        }

        public void getMoreBlocks() throws IOException
        {
        }
    }

}
