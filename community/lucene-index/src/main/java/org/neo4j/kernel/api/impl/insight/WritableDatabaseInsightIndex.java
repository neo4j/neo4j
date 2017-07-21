/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.api.impl.insight;

import java.io.IOException;
import java.util.List;

import org.neo4j.kernel.api.impl.index.WritableAbstractDatabaseIndex;
import org.neo4j.kernel.api.impl.index.partition.AbstractIndexPartition;

class WritableDatabaseInsightIndex extends WritableAbstractDatabaseIndex<InsightLuceneIndex>
{
    private InsightLuceneIndex insightLuceneIndex;

    WritableDatabaseInsightIndex( InsightLuceneIndex insightLuceneIndex )
    {
        super( insightLuceneIndex );
        this.insightLuceneIndex = insightLuceneIndex;
    }

    public PartitionedInsightIndexWriter getIndexWriter() throws IOException
    {
        return insightLuceneIndex.getIndexWriter( this );
    }

    public InsightIndexReader getIndexReader() throws IOException
    {
        return luceneIndex.getIndexReader();
    }

    public boolean isOnline() throws IOException
    {
        return luceneIndex.isOnline();
    }

    public void markAsOnline() throws IOException
    {
        commitCloseLock.lock();
        try
        {
            luceneIndex.markAsOnline();
        }
        finally
        {
            commitCloseLock.unlock();
        }
    }

    public void markAsFailed( String failure ) throws IOException
    {
        luceneIndex.markAsFailed( failure );
    }

    public boolean hasSinglePartition( List<AbstractIndexPartition> partitions )
    {
        return luceneIndex.hasSinglePartition( partitions );
    }

    public AbstractIndexPartition getFirstPartition( List<AbstractIndexPartition> partitions )
    {
        return luceneIndex.getFirstPartition( partitions );
    }
}
