/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.index.impl.lucene;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

class WritableIndexReference extends IndexReference
{
    private final IndexWriter writer;
    private boolean writerIsClosed;
    private final AtomicBoolean stale = new AtomicBoolean();

    WritableIndexReference( IndexIdentifier identifier, IndexSearcher searcher, IndexWriter writer )
    {
        super(identifier, searcher );
        this.writer = writer;
    }

    @Override
    public IndexWriter getWriter()
    {
        return writer;
    }

    @Override
    public synchronized void dispose() throws IOException
    {
        disposeSearcher();
        disposeWriter();
    }

    @Override
    public boolean checkAndClearStale()
    {
        return stale.compareAndSet( true, false );
    }

    @Override
    public void setStale()
    {
        stale.set( true );
    }

    private void disposeWriter() throws IOException
    {
        if ( !writerIsClosed )
        {
            writer.close();
            writerIsClosed = true;
        }
    }

    boolean isWriterClosed()
    {
        return writerIsClosed;
    }
}
