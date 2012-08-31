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
package org.apache.cassandra.db.index;

import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.IFilter;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;

public abstract class SecondaryIndexSearcher
{
    protected final SecondaryIndexManager    indexManager;
    protected final Set<ByteBuffer> columns;
    protected final ColumnFamilyStore baseCfs;

    public SecondaryIndexSearcher(SecondaryIndexManager indexManager, Set<ByteBuffer> columns)
    {
        this.indexManager = indexManager;
        this.columns = columns;
        this.baseCfs = indexManager.baseCfs;
    }
    
    public SecondaryIndex highestSelectivityIndex(List<IndexExpression> clause)
    {
        IndexExpression expr = highestSelectivityPredicate(clause);

        if (expr != null)
            return indexManager.getIndexForColumn(expr.column_name);

        return null;
    }

    public IndexExpression highestSelectivityPredicate(List<IndexExpression> clause)
    {
        IndexExpression best = null;
        int bestMeanCount = Integer.MAX_VALUE;
        for (IndexExpression expression : clause)
        {
            // skip columns belonging to a different index type
            if (!columns.contains(expression.column_name))
                continue;

            SecondaryIndex index = indexManager.getIndexForColumn(expression.column_name);
            if (index == null || (expression.op != IndexOperator.EQ))
                continue;
            int columns = index.getIndexCfs().getMeanColumns();
            if (columns < bestMeanCount)
            {
                best = expression;
                bestMeanCount = columns;
            }
        }
        return best;
    }

    public abstract List<Row> search(List<IndexExpression> clause, AbstractBounds<RowPosition> range, int maxResults, IFilter dataFilter, boolean maxIsColumns);

    /**
     * @return true this index is able to handle given clauses.
     */
    public abstract boolean isIndexing(List<IndexExpression> clause);
    
    protected boolean isIndexValueStale(ColumnFamily liveData, ByteBuffer indexedColumnName, ByteBuffer indexedValue)
    {
        IColumn liveColumn = liveData.getColumn(indexedColumnName);
        if (null == liveColumn || liveColumn.isMarkedForDelete())
            return true;
        
        ByteBuffer liveValue = liveColumn.value();
        return 0 != liveData.metadata().getValueValidator(indexedColumnName).compare(indexedValue, liveValue);
    }
}
