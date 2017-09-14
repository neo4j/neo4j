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
package org.neo4j.kernel.api.impl.index;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.kernel.api.impl.index.partition.AbstractIndexPartition;

/**
 * Writable lucene index representation that wraps provided index implementation and
 * allow read only operations only on top of it.
 * @param <T> - particular index implementation
 */
public class WritableAbstractDatabaseIndex<T extends AbstractLuceneIndex> implements DatabaseIndex
{
    // lock used to guard commits and close of lucene indexes from separate threads
    protected final ReentrantLock commitCloseLock = new ReentrantLock();

    protected T luceneIndex;

    public WritableAbstractDatabaseIndex( T luceneIndex )
    {
        this.luceneIndex = luceneIndex;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void create() throws IOException
    {
        luceneIndex.create();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void open() throws IOException
    {
        luceneIndex.open();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isOpen()
    {
        return luceneIndex.isOpen();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isReadOnly()
    {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean exists() throws IOException
    {
        return luceneIndex.exists();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isValid()
    {
        return luceneIndex.isValid();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void drop() throws IOException
    {
        commitCloseLock.lock();
        try
        {
            luceneIndex.drop();
        }
        finally
        {
            commitCloseLock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void flush() throws IOException
    {
        commitCloseLock.lock();
        try
        {
            luceneIndex.flush( false );
        }
        finally
        {
            commitCloseLock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException
    {
        commitCloseLock.lock();
        try
        {
            luceneIndex.close();
        }
        finally
        {
            commitCloseLock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LuceneAllDocumentsReader allDocumentsReader()
    {
        return luceneIndex.allDocumentsReader();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResourceIterator<File> snapshot() throws IOException
    {
        commitCloseLock.lock();
        try
        {
            return luceneIndex.snapshot();
        }
        finally
        {
            commitCloseLock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void maybeRefreshBlocking() throws IOException
    {
        luceneIndex.maybeRefreshBlocking();
    }

    /**
     * Add new partition to the index. Must only be called by a single thread at a time.
     *
     * @return newly created partition
     * @throws IOException
     */
    public AbstractIndexPartition addNewPartition() throws IOException
    {
        return luceneIndex.addNewPartition();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AbstractIndexPartition> getPartitions()
    {
        return luceneIndex.getPartitions();
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
