/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.schema;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.function.Factory;
import org.neo4j.helpers.TaskCoordinator;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.index.AbstractLuceneIndex;
import org.neo4j.kernel.api.impl.index.partition.IndexPartition;
import org.neo4j.kernel.api.impl.index.partition.PartitionSearcher;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.kernel.api.impl.schema.reader.PartitionedIndexReader;
import org.neo4j.kernel.api.impl.schema.reader.SimpleIndexReader;
import org.neo4j.kernel.api.impl.schema.verification.PartitionedUniquenessVerifier;
import org.neo4j.kernel.api.impl.schema.verification.SimpleUniquenessVerifier;
import org.neo4j.kernel.api.impl.schema.verification.UniquenessVerifier;
import org.neo4j.kernel.api.impl.schema.writer.LuceneIndexWriter;
import org.neo4j.kernel.api.impl.schema.writer.PartitionedIndexWriter;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.storageengine.api.schema.IndexReader;

import static java.util.Collections.singletonMap;

/**
 * Implementation of Lucene schema index that support multiple partitions.
 */
public class LuceneSchemaIndex extends AbstractLuceneIndex
{
    private static final String KEY_STATUS = "status";
    private static final String ONLINE = "online";
    private static final Map<String,String> ONLINE_COMMIT_USER_DATA = singletonMap( KEY_STATUS, ONLINE );

    private final IndexConfiguration config;
    private final IndexSamplingConfig samplingConfig;

    private final TaskCoordinator taskCoordinator = new TaskCoordinator( 10, TimeUnit.MILLISECONDS );

    public LuceneSchemaIndex( PartitionedIndexStorage indexStorage, IndexConfiguration config,
            IndexSamplingConfig samplingConfig, Factory<IndexWriterConfig> writerConfigFactory )
    {
        super( indexStorage, writerConfigFactory );
        this.config = config;
        this.samplingConfig = samplingConfig;
    }

    public LuceneIndexWriter getIndexWriter() throws IOException
    {
        ensureOpen();
        return new PartitionedIndexWriter( this );
    }

    public IndexReader getIndexReader() throws IOException
    {
        ensureOpen();
        partitionsLock.lock();
        try
        {
            List<IndexPartition> partitions = getPartitions();
            return hasSinglePartition( partitions ) ? createSimpleReader( partitions )
                                                    : createPartitionedReader( partitions );
        }
        finally
        {
            partitionsLock.unlock();
        }
    }

    /**
     * Verifies uniqueness of property values present in this index.
     *
     * @param accessor the accessor to retrieve actual property values from the store.
     * @param propertyKeyId the id of the property to verify.
     * @throws IndexEntryConflictException if there are duplicates.
     * @throws IOException
     * @see UniquenessVerifier#verify(PropertyAccessor, int)
     */
    public void verifyUniqueness( PropertyAccessor accessor, int propertyKeyId )
            throws IOException, IndexEntryConflictException
    {
        try ( UniquenessVerifier verifier = createUniquenessVerifier() )
        {
            verifier.verify( accessor, propertyKeyId );
        }
    }

    /**
     * Verifies uniqueness of updated property values.
     *
     * @param accessor the accessor to retrieve actual property values from the store.
     * @param propertyKeyId the id of the property to verify.
     * @param updatedPropertyValues the values to check uniqueness for.
     * @throws IndexEntryConflictException if there are duplicates.
     * @throws IOException
     * @see UniquenessVerifier#verify(PropertyAccessor, int, List)
     */
    public void verifyUniqueness( PropertyAccessor accessor, int propertyKeyId, List<Object> updatedPropertyValues )
            throws IOException, IndexEntryConflictException
    {
        try ( UniquenessVerifier verifier = createUniquenessVerifier() )
        {
            verifier.verify( accessor, propertyKeyId, updatedPropertyValues );
        }
    }

    @Override
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
        IndexPartition partition = getFirstPartition( getPartitions() );
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
        commitCloseLock.lock();
        try
        {
            IndexPartition partition = getFirstPartition( getPartitions() );
            IndexWriter indexWriter = partition.getIndexWriter();
            indexWriter.setCommitData( ONLINE_COMMIT_USER_DATA );
            flush();
        }
        finally
        {
            commitCloseLock.unlock();
        }
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

    private UniquenessVerifier createUniquenessVerifier() throws IOException
    {
        ensureOpen();
        maybeRefreshBlocking();
        List<IndexPartition> partitions = getPartitions();
        return hasSinglePartition( partitions ) ? createSimpleUniquenessVerifier( partitions )
                                                : createPartitionedUniquenessVerifier( partitions );
    }

    private SimpleIndexReader createSimpleReader( List<IndexPartition> partitions ) throws IOException
    {
        IndexPartition singlePartition = getFirstPartition( partitions );
        return new SimpleIndexReader( singlePartition.acquireSearcher(), config, samplingConfig, taskCoordinator );
    }

    private UniquenessVerifier createSimpleUniquenessVerifier( List<IndexPartition> partitions ) throws IOException
    {
        IndexPartition singlePartition = getFirstPartition( partitions );
        PartitionSearcher partitionSearcher = singlePartition.acquireSearcher();
        return new SimpleUniquenessVerifier( partitionSearcher );
    }

    private PartitionedIndexReader createPartitionedReader( List<IndexPartition> partitions ) throws IOException
    {
        List<PartitionSearcher> searchers = acquireSearchers( partitions );
        return new PartitionedIndexReader( searchers, config, samplingConfig, taskCoordinator );
    }

    private UniquenessVerifier createPartitionedUniquenessVerifier( List<IndexPartition> partitions ) throws IOException
    {
        List<PartitionSearcher> searchers = acquireSearchers( partitions );
        return new PartitionedUniquenessVerifier( searchers );
    }

}
