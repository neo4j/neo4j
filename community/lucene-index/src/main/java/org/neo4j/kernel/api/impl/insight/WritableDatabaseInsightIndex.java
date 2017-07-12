package org.neo4j.kernel.api.impl.insight;

import java.io.IOException;
import java.util.List;

import org.neo4j.kernel.api.impl.index.WritableAbstractDatabaseIndex;
import org.neo4j.kernel.api.impl.index.partition.AbstractIndexPartition;

class WritableDatabaseInsightIndex extends WritableAbstractDatabaseIndex<InsightLuceneIndex>
{
    private InsightLuceneIndex insightLuceneIndex;

    public WritableDatabaseInsightIndex( InsightLuceneIndex insightLuceneIndex )
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

      /**
     * {@inheritDoc}
     */
    public boolean isOnline() throws IOException
    {
        return luceneIndex.isOnline();
    }

    /**
     * {@inheritDoc}
     */
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

    /**
     * {@inheritDoc}
     */
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
