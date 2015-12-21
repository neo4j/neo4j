/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import org.apache.lucene.store.Directory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;

import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.impl.index.partition.IndexPartition;
import org.neo4j.kernel.api.impl.index.partition.PartitionSearcher;
import org.neo4j.kernel.api.impl.index.reader.PartitionedIndexReader;
import org.neo4j.kernel.api.impl.index.reader.SimpleIndexReader;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.storageengine.api.schema.IndexReader;

import static java.util.Collections.singletonMap;

public class LuceneIndex implements Closeable
{
    private final ReentrantLock commitCloseLock = new ReentrantLock();
    private final ReentrantLock readWriteLock = new ReentrantLock();

    private List<IndexPartition> partitions = new CopyOnWriteArrayList<>();
    private boolean open = false;
    private PartitionedIndexStorage indexStorage;

    private static final String KEY_STATUS = "status";
    private static final String ONLINE = "online";
    private static final Map<String,String> ONLINE_COMMIT_USER_DATA = singletonMap( KEY_STATUS, ONLINE );

    public LuceneIndex( PartitionedIndexStorage indexStorage )
    {
        this.indexStorage = indexStorage;
    }

    public void prepare() throws IOException
    {
        indexStorage.prepareFolder( indexStorage.getIndexFolder() );
        indexStorage.reserveIndexFailureStorage();
        createNewPartitionFolder();
    }

    public void open() throws IOException
    {
        List<Directory> directories = indexStorage.openIndexDirectories();
        for ( Directory directory : directories )
        {
            partitions.add( new IndexPartition( directory ) );
        }
        open = true;
    }

    public LuceneIndexWriter getIndexWriter() throws IOException
    {
        ensureOpen();
        return new PartitionedIndexWriter( this );
    }

    public void maybeRefresh() throws IOException
    {
        readWriteLock.lock();
        try
        {
            for ( IndexPartition partition : getPartitions() )
            {
                partition.maybeRefresh();
            }
        }
        finally
        {
            readWriteLock.unlock();
        }
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

    IndexPartition addNewPartition() throws IOException
    {
        ensureOpen();
        readWriteLock.lock();
        try
        {
            File partitionFolder = createNewPartitionFolder();
            IndexPartition indexPartition = new IndexPartition( indexStorage.openDirectory( partitionFolder ) );
            partitions.add( indexPartition );
            return indexPartition;
        }
        finally
        {
            readWriteLock.unlock();
        }
    }


    List<IndexPartition> getPartitions()
    {
        ensureOpen();
        return partitions;
    }

    private void ensureOpen()
    {
        if ( !open )
        {
            throw new IllegalStateException( "Please open lucene index before working with it." );
        }
    }

    @Override
    public void close() throws IOException
    {
        commitCloseLock.lock();
        try
        {
            IOUtils.closeAll( partitions );
        }
        finally
        {
            commitCloseLock.unlock();
        }
    }

    public void drop() throws IOException
    {
        close();
        indexStorage.cleanupFolder( indexStorage.getIndexFolder() );
    }

    public void markAsOnline() throws IOException
    {
        ensureOpen();
        commitCloseLock.lock();
        try
        {
            IndexPartition indexPartition = partitions.get( 0 );
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
        ensureOpen();
        indexStorage.storeIndexFailure( failure );
    }

    private IndexReader createSimpleReader( List<IndexPartition> partitions ) throws IOException
    {
        return new SimpleIndexReader( partitions.get( 0 ).acquireSearcher() );
    }

    private IndexReader createPartitionedReader( List<IndexPartition> partitions ) throws IOException
    {
        List<PartitionSearcher> searchers = new ArrayList<>();
        for ( IndexPartition partition : partitions )
        {
            searchers.add( partition.acquireSearcher() );
        }
        return new PartitionedIndexReader( searchers );
    }

    private boolean hasSinglePartition( List<IndexPartition> partitions )
    {
        return partitions.size() == 1;
    }

    private File createNewPartitionFolder() throws IOException
    {
        File partitionFolder = indexStorage.getPartitionFolder( partitions.size() + 1 );
        indexStorage.prepareFolder( partitionFolder );
        return partitionFolder;
    }

}
