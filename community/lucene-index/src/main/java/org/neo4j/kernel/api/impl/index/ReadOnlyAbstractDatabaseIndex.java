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

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.kernel.api.impl.schema.writer.LuceneIndexWriter;
import org.neo4j.storageengine.api.schema.IndexReader;

/**
 * Read only lucene index representation that wraps provided index implementation and
 * allow read only operations only on top of it.
 * @param <INDEX> - particular index implementation
 */
public abstract class ReadOnlyAbstractDatabaseIndex<INDEX extends AbstractLuceneIndex<READER>, READER extends IndexReader>
        extends AbstractDatabaseIndex<INDEX, READER>
{
    public ReadOnlyAbstractDatabaseIndex( INDEX luceneIndex )
    {
        super( luceneIndex );
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
    public boolean isReadOnly()
    {
        return true;
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

    @Override
    public LuceneIndexWriter getIndexWriter()
    {
        throw new UnsupportedOperationException( "Can't get index writer for read only lucene index." );
    }

    /**
     * Unsupported operation in read only index.
     */
    @Override
    public void markAsOnline()
    {
        throw new UnsupportedOperationException( "Can't mark read only index." );
    }
}
