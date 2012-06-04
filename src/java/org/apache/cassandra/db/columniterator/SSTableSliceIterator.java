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

import java.io.IOException;
import java.util.List;
import java.util.SortedSet;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.thrift.SliceRange;

/**
 *  A Column Iterator over SSTable
 */
public class SSTableSliceIterator implements IColumnIterator
{
    private final IColumnIterator reader;
    private final DecoratedKey key;

    public SSTableSliceIterator(SSTableReader sstable, DecoratedKey key, List<SliceRange> ranges, boolean reversed)
    {
        this.key = key;
        RowIndexEntry indexEntry = sstable.getPosition(key, SSTableReader.Operator.EQ);
        this.reader = indexEntry == null ? null : createReader(sstable, indexEntry, null, ranges, reversed);
    }

    /**
     * An iterator for a slice within an SSTable
     * @param sstable Table for the CFS we are reading from
     * @param file Optional parameter that input is read from.  If null is passed, this class creates an appropriate one automatically.
     * If this class creates, it will close the underlying file when #close() is called.
     * If a caller passes a non-null argument, this class will NOT close the underlying file when the iterator is closed (i.e. the caller is responsible for closing the file)
     * In all cases the caller should explicitly #close() this iterator.
     * @param key The key the requested slice resides under
     * @param startColumn The start of the slice
     * @param finishColumn The end of the slice
     * @param reversed Results are returned in reverse order iff reversed is true.
     */
    public SSTableSliceIterator(SSTableReader sstable, FileDataInput file, DecoratedKey key, List<SliceRange> ranges, boolean reversed, RowIndexEntry indexEntry)
    {
        this.key = key;
        reader = createReader(sstable, indexEntry, file, ranges, reversed);
    }

    private static IColumnIterator createReader(SSTableReader sstable, RowIndexEntry indexEntry, FileDataInput file, List<SliceRange> ranges, boolean reversed)
    {
        return ranges.size() == 1 && ranges.get(0).start.remaining() == 0 && !reversed
                 ? new SimpleSliceReader(sstable, indexEntry, file, ranges.get(ranges.size()-1).finish)
                 : new IndexedSliceReader(sstable, indexEntry, file, ranges, reversed);
    }

    public DecoratedKey getKey()
    {
        return key;
    }

    public ColumnFamily getColumnFamily()
    {
        return reader == null ? null : reader.getColumnFamily();
    }

    public boolean hasNext()
    {
        return reader != null && reader.hasNext();
    }

    public IColumn next()
    {
        return reader.next();
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    public void close() throws IOException
    {
        if (reader != null)
            reader.close();
    }

}
