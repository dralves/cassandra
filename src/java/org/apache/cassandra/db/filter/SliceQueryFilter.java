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
package org.apache.cassandra.db.filter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.IColumnContainer;
import org.apache.cassandra.db.Memtable;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.SuperColumn;
import org.apache.cassandra.db.columniterator.IColumnIterator;
import org.apache.cassandra.db.columniterator.SSTableSliceIterator;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.thrift.SliceRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

public class SliceQueryFilter implements IFilter
{
    private static final Logger logger = LoggerFactory.getLogger(SliceQueryFilter.class);

    private final List<SliceRange> ranges;
    public final boolean reversed;
    public volatile int count;


    public SliceQueryFilter(ByteBuffer start, ByteBuffer finish, boolean reversed, int count)
    {
        this(ImmutableList.of(new SliceRange(start, finish, reversed, count)), reversed, count);
    }

    /**
     * Constructor that accepts multiple ranges. All ranges are assumed to be in the same direction (forward or
     * reversed).
     */
    public SliceQueryFilter(List<SliceRange> ranges, boolean reversed, int count)
    {
        this.ranges = ranges;
        this.reversed = reversed;
        this.count = count;
    }

    public IColumnIterator getMemtableColumnIterator(ColumnFamily cf, DecoratedKey key)
    {
        return Memtable.getSliceIterator(key, cf, this);
    }

    public IColumnIterator getSSTableColumnIterator(SSTableReader sstable, DecoratedKey key)
    {
        return new SSTableSliceIterator(sstable, key, ranges, reversed);
    }

    public IColumnIterator getSSTableColumnIterator(SSTableReader sstable, FileDataInput file, DecoratedKey key, RowIndexEntry indexEntry)
    {
        return new SSTableSliceIterator(sstable, file, key, ranges, reversed, indexEntry);
    }

    public SuperColumn filterSuperColumn(SuperColumn superColumn, int gcBefore)
    {
        // we clone shallow, then add, under the theory that generally we're interested in a relatively small number of subcolumns.
        // this may be a poor assumption.
        SuperColumn scFiltered = superColumn.cloneMeShallow();
        Iterator<IColumn> subcolumns;
        if (reversed)
        {
            List<IColumn> columnsAsList = new ArrayList<IColumn>(superColumn.getSubColumns());
            subcolumns = Lists.reverse(columnsAsList).iterator();
        }
        else
        {
            subcolumns = superColumn.getSubColumns().iterator();
        }

        // iterate until we get to the "real" start column
        Comparator<ByteBuffer> comparator = reversed ? superColumn.getComparator().reverseComparator : superColumn.getComparator();
        while (subcolumns.hasNext())
        {
            IColumn column = subcolumns.next();
            if (comparator.compare(column.name(), ranges.get(0).start) >= 0)
            {
                subcolumns = Iterators.concat(Iterators.singletonIterator(column), subcolumns);
                break;
            }
        }
        // subcolumns is either empty now, or has been redefined in the loop above.  either is ok.
        collectReducedColumns(scFiltered, subcolumns, gcBefore);
        return scFiltered;
    }

    public Comparator<IColumn> getColumnComparator(AbstractType<?> comparator)
    {
        return reversed ? comparator.columnReverseComparator : comparator.columnComparator;
    }

    public void collectReducedColumns(IColumnContainer container, Iterator<IColumn> reducedColumns, int gcBefore)
    {
        int liveColumns = 0;
        AbstractType<?> comparator = container.getComparator();

        while (reducedColumns.hasNext())
        {
            if (liveColumns >= count)
                break;

            IColumn column = reducedColumns.next();
            if (logger.isDebugEnabled())
                logger.debug(String.format("collecting %s of %s: %s",
                                           liveColumns, count, column.getString(comparator)));

            if (ranges.get(ranges.size()-1).finish.remaining() > 0
                && ((!reversed && comparator.compare(column.name(), ranges.get(ranges.size()-1).finish) > 0))
                    || (reversed && comparator.compare(column.name(), ranges.get(ranges.size()-1).finish) < 0))
                break;

            // only count live columns towards the `count` criteria
            if (column.isLive()
                && (!container.isMarkedForDelete()
                    || column.mostRecentLiveChangeAt() > container.getMarkedForDeleteAt()))
            {
                liveColumns++;
            }

            // but we need to add all non-gc-able columns to the result for read repair:
            if (QueryFilter.isRelevant(column, container, gcBefore))
                container.addColumn(column);
        }
    }

    public ByteBuffer start() {
        return this.ranges.get(0).start;
    }
    
    public ByteBuffer finish() {
        return this.ranges.get(0).finish;
    }
    
    public void setStart(ByteBuffer start) {
        this.ranges.get(0).start = start;
    }

    @Override
    public String toString()
    {
        return "SliceQueryFilter [reversed=" + reversed + ", ranges=" + ranges + ", count=" + count + "]";
    }

    public boolean isReversed()
    {
        return reversed;
    }

    public void updateColumnsLimit(int newLimit)
    {
        count = newLimit;
    }
}
