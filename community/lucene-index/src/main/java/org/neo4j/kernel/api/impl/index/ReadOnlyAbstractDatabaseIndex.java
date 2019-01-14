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
package org.neo4j.kernel.api.impl.index;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.kernel.api.impl.index.partition.AbstractIndexPartition;

/**
 * Read only lucene index representation that wraps provided index implementation and
 * allow read only operations only on top of it.
 * @param <T> - particular index implementation
 */
public abstract class ReadOnlyAbstractDatabaseIndex<T extends AbstractLuceneIndex> implements DatabaseIndex
{
    protected T luceneIndex;

    public ReadOnlyAbstractDatabaseIndex( T luceneIndex )
    {
        this.luceneIndex = luceneIndex;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void create()
    {
        throw new UnsupportedOperationException( "Index creation in read only mode is not supported." );
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
        return true;
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
    public void drop()
    {
        throw new UnsupportedOperationException( "Index drop is not supported in read only mode." );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void flush()
    {
        // nothing to flush in read only mode
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException
    {
        luceneIndex.close();
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
        return luceneIndex.snapshot();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void maybeRefreshBlocking()
    {
        //nothing to refresh in read only mode
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AbstractIndexPartition> getPartitions()
    {
        return luceneIndex.getPartitions();
    }

}
