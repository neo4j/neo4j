/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
import org.apache.lucene.store.Directory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.helpers.TaskCoordinator;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.index.AbstractLuceneIndex;
import org.neo4j.kernel.api.impl.index.partition.AbstractIndexPartition;
import org.neo4j.kernel.api.impl.index.partition.IndexPartitionFactory;
import org.neo4j.kernel.api.impl.index.partition.PartitionSearcher;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.kernel.api.impl.schema.reader.PartitionedIndexReader;
import org.neo4j.kernel.api.impl.schema.reader.SimpleIndexReader;
import org.neo4j.kernel.api.impl.schema.verification.PartitionedUniquenessVerifier;
import org.neo4j.kernel.api.impl.schema.verification.SimpleUniquenessVerifier;
import org.neo4j.kernel.api.impl.schema.verification.UniquenessVerifier;
import org.neo4j.kernel.api.impl.schema.writer.LuceneIndexWriter;
import org.neo4j.kernel.api.impl.schema.writer.PartitionedIndexWriter;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.values.storable.Value;

import static java.util.Collections.singletonMap;

/**
 * Implementation of Lucene schema index that support multiple partitions.
 */
class LuceneSchemaIndex extends AbstractLuceneIndex
{
    private static final String KEY_STATUS = "status";
    private static final String ONLINE = "online";
    private static final Map<String,String> ONLINE_COMMIT_USER_DATA = singletonMap( KEY_STATUS, ONLINE );

    private final SchemaIndexDescriptor descriptor;
    private final IndexSamplingConfig samplingConfig;

    private final TaskCoordinator taskCoordinator = new TaskCoordinator( 10, TimeUnit.MILLISECONDS );

    LuceneSchemaIndex( PartitionedIndexStorage indexStorage, SchemaIndexDescriptor descriptor,
            IndexSamplingConfig samplingConfig, IndexPartitionFactory partitionFactory )
    {
        super( indexStorage, partitionFactory );
        this.descriptor = descriptor;
        this.samplingConfig = samplingConfig;
    }

    public LuceneIndexWriter getIndexWriter( WritableDatabaseSchemaIndex writableLuceneSchemaIndex )
    {
        ensureOpen();
        return new PartitionedIndexWriter( writableLuceneSchemaIndex );
    }

    public IndexReader getIndexReader() throws IOException
    {
        ensureOpen();
        List<AbstractIndexPartition> partitions = getPartitions();
        return hasSinglePartition( partitions ) ? createSimpleReader( partitions )
                                                : createPartitionedReader( partitions );
    }

    public SchemaIndexDescriptor getDescriptor()
    {
        return descriptor;
    }

    /**
     * Verifies uniqueness of property values present in this index.
     *
     * @param accessor the accessor to retrieve actual property values from the store.
     * @param propertyKeyIds the ids of the properties to verify.
     * @throws IndexEntryConflictException if there are duplicates.
     * @throws IOException
     * @see UniquenessVerifier#verify(PropertyAccessor, int[])
     */
    public void verifyUniqueness( PropertyAccessor accessor, int[] propertyKeyIds )
            throws IOException, IndexEntryConflictException
    {
        flush( true );
        try ( UniquenessVerifier verifier = createUniquenessVerifier() )
        {
            verifier.verify( accessor, propertyKeyIds );
        }
    }

    /**
     * Verifies uniqueness of updated property values.
     *
     * @param accessor the accessor to retrieve actual property values from the store.
     * @param propertyKeyIds the ids of the properties to verify.
     * @param updatedValueTuples the values to check uniqueness for.
     * @throws IndexEntryConflictException if there are duplicates.
     * @throws IOException
     * @see UniquenessVerifier#verify(PropertyAccessor, int[], List)
     */
    public void verifyUniqueness( PropertyAccessor accessor, int[] propertyKeyIds, List<Value[]> updatedValueTuples )
            throws IOException, IndexEntryConflictException
    {
        try ( UniquenessVerifier verifier = createUniquenessVerifier() )
        {
            verifier.verify( accessor, propertyKeyIds, updatedValueTuples );
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

    private UniquenessVerifier createUniquenessVerifier() throws IOException
    {
        ensureOpen();
        maybeRefreshBlocking();
        List<AbstractIndexPartition> partitions = getPartitions();
        return hasSinglePartition( partitions ) ? createSimpleUniquenessVerifier( partitions )
                                                : createPartitionedUniquenessVerifier( partitions );
    }

    private SimpleIndexReader createSimpleReader( List<AbstractIndexPartition> partitions ) throws IOException
    {
        AbstractIndexPartition singlePartition = getFirstPartition( partitions );
        return new SimpleIndexReader( singlePartition.acquireSearcher(), descriptor, samplingConfig, taskCoordinator );
    }

    private UniquenessVerifier createSimpleUniquenessVerifier( List<AbstractIndexPartition> partitions ) throws IOException
    {
        AbstractIndexPartition singlePartition = getFirstPartition( partitions );
        PartitionSearcher partitionSearcher = singlePartition.acquireSearcher();
        return new SimpleUniquenessVerifier( partitionSearcher );
    }

    private PartitionedIndexReader createPartitionedReader( List<AbstractIndexPartition> partitions ) throws IOException
    {
        List<PartitionSearcher> searchers = acquireSearchers( partitions );
        return new PartitionedIndexReader( searchers, descriptor, samplingConfig, taskCoordinator );
    }

    private UniquenessVerifier createPartitionedUniquenessVerifier( List<AbstractIndexPartition> partitions ) throws IOException
    {
        List<PartitionSearcher> searchers = acquireSearchers( partitions );
        return new PartitionedUniquenessVerifier( searchers );
    }

}
