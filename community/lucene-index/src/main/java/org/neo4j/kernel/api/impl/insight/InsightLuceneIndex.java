package org.neo4j.kernel.api.impl.insight;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.helpers.TaskCoordinator;
import org.neo4j.kernel.api.impl.index.AbstractLuceneIndex;
import org.neo4j.kernel.api.impl.index.partition.AbstractIndexPartition;
import org.neo4j.kernel.api.impl.index.partition.IndexPartitionFactory;
import org.neo4j.kernel.api.impl.index.partition.PartitionSearcher;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;

import static java.util.Collections.singletonMap;

class InsightLuceneIndex extends AbstractLuceneIndex
{

    private String[] properties;

    public InsightLuceneIndex( PartitionedIndexStorage indexStorage, IndexPartitionFactory partitionFactory,
            String[] properties )
    {
        super( indexStorage, partitionFactory );
        this.properties = properties;
    }

    private static final String KEY_STATUS = "status";
    private static final String ONLINE = "online";
    private static final Map<String,String> ONLINE_COMMIT_USER_DATA = singletonMap( KEY_STATUS, ONLINE );

    private final TaskCoordinator taskCoordinator = new TaskCoordinator( 10, TimeUnit.MILLISECONDS );

    public PartitionedInsightIndexWriter getIndexWriter( WritableDatabaseInsightIndex writableIndex ) throws IOException
    {
        ensureOpen();
        return new PartitionedInsightIndexWriter( writableIndex );
    }

    public InsightIndexReader getIndexReader() throws IOException
    {
        ensureOpen();
        List<AbstractIndexPartition> partitions = getPartitions();
        return hasSinglePartition( partitions ) ? createSimpleReader( partitions )
                                                : createPartitionedReader( partitions );
    }

    public void drop() throws IOException
    {
        taskCoordinator.cancel();
        try
        {
            taskCoordinator.awaitCompletion();
        }
        catch ( InterruptedException e )
        {
            throw new IOException( "Interrupted while waiting for concurrent tasks to complete.", e );
        }
        super.drop();
    }

    /**
     * Check if this index is marked as online.
     *
     * @return <code>true</code> if index is online, <code>false</code> otherwise
     * @throws IOException
     */
    public boolean isOnline() throws IOException
    {
        ensureOpen();
        AbstractIndexPartition partition = getFirstPartition( getPartitions() );
        Directory directory = partition.getDirectory();
        try ( DirectoryReader reader = DirectoryReader.open( directory ) )
        {
            Map<String,String> userData = reader.getIndexCommit().getUserData();
            return ONLINE.equals( userData.get( KEY_STATUS ) );
        }
    }

    /**
     * Marks index as online by including "status" -> "online" map into commit metadata of the first partition.
     *
     * @throws IOException
     */
    public void markAsOnline() throws IOException
    {
        ensureOpen();
        AbstractIndexPartition partition = getFirstPartition( getPartitions() );
        IndexWriter indexWriter = partition.getIndexWriter();
        indexWriter.setCommitData( ONLINE_COMMIT_USER_DATA );
        flush( false );
    }

    /**
     * Writes the given failure message to the failure storage.
     *
     * @param failure the failure message.
     * @throws IOException
     */
    public void markAsFailed( String failure ) throws IOException
    {
        indexStorage.storeIndexFailure( failure );
    }


    private SimpleInsightIndexReader createSimpleReader( List<AbstractIndexPartition> partitions ) throws IOException
    {
        AbstractIndexPartition singlePartition = getFirstPartition( partitions );
        return new SimpleInsightIndexReader( singlePartition.acquireSearcher(), properties );
    }


    private PartitionedInsightIndexReader createPartitionedReader( List<AbstractIndexPartition> partitions )
            throws IOException
    {
        List<PartitionSearcher> searchers = acquireSearchers( partitions );
        return new PartitionedInsightIndexReader( searchers, properties );
    }

}
