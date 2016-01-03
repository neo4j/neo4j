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
package org.neo4j.kernel.api.impl.index;

import org.apache.lucene.index.IndexWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.helpers.TaskCoordinator;
import org.neo4j.kernel.api.impl.index.partition.IndexPartition;
import org.neo4j.kernel.api.impl.index.partition.PartitionSearcher;
import org.neo4j.kernel.api.impl.index.reader.PartitionedIndexReader;
import org.neo4j.kernel.api.impl.index.reader.SimpleIndexReader;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.storageengine.api.schema.IndexReader;

import static java.util.Collections.singletonMap;

public class LuceneSchemaIndex extends AbstractLuceneIndex
{
    private static final String KEY_STATUS = "status";
    private static final String ONLINE = "online";
    private static final Map<String,String> ONLINE_COMMIT_USER_DATA = singletonMap( KEY_STATUS, ONLINE );

    private final IndexConfiguration config;
    private final IndexSamplingConfig samplingConfig;

    private final TaskCoordinator taskCoordinator = new TaskCoordinator( 10, TimeUnit.MILLISECONDS );

    public LuceneSchemaIndex( PartitionedIndexStorage indexStorage, IndexConfiguration config,
            IndexSamplingConfig samplingConfig )
    {
        super( indexStorage );
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
        readWriteLock.lock();
        try
        {
            List<IndexPartition> partitions = getPartitions();
            return hasSinglePartition( partitions ) ? createSimpleReader( partitions )
                                                    : createPartitionedReader( partitions );
        }
        finally
        {
            readWriteLock.unlock();
        }
    }

    private boolean hasSinglePartition( List<IndexPartition> partitions )
    {
        return partitions.size() == 1;
    }

    private SimpleIndexReader createSimpleReader( List<IndexPartition> partitions ) throws IOException
    {
        return new SimpleIndexReader( partitions.get( 0 ).acquireSearcher(), config,
                samplingConfig, taskCoordinator );
    }

    private PartitionedIndexReader createPartitionedReader( List<IndexPartition> partitions ) throws IOException
    {
        List<PartitionSearcher> searchers = new ArrayList<>();
        for ( IndexPartition partition : partitions )
        {
            searchers.add( partition.acquireSearcher() );
        }
        return new PartitionedIndexReader( searchers );
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

    public void markAsOnline() throws IOException
    {
        ensureOpen();
        commitCloseLock.lock();
        try
        {
            IndexPartition indexPartition = getPartitions().get( 0 );
            IndexWriter indexWriter = indexPartition.getIndexWriter();
            indexWriter.setCommitData( ONLINE_COMMIT_USER_DATA );
            indexWriter.commit();
        }
        finally
        {
            commitCloseLock.unlock();
        }
    }

    public void markAsFailed( String failure ) throws IOException
    {
        indexStorage.storeIndexFailure( failure );
    }
}
