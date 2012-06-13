/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.search.IndexSearcher;

class IndexSearcherRef
{
    private final IndexIdentifier identifier;
    private final IndexSearcher searcher;
    private final AtomicInteger refCount = new AtomicInteger( 0 );
    private volatile boolean isClosed;

    /**
     * We need this because we only want to close the reader/searcher if
     * it has been detached... i.e. the {@link LuceneDataSource} no longer
     * has any reference to it, only an iterator out in the client has a ref.
     * And when that client calls close() it should be closed.
     */
    private volatile boolean detached;

    public IndexSearcherRef( IndexIdentifier identifier, IndexSearcher searcher )
    {
        this.identifier = identifier;
        this.searcher = searcher;
    }

    public IndexSearcher getSearcher()
    {
        return this.searcher;
    }

    public IndexIdentifier getIdentifier()
    {
        return identifier;
    }

    void incRef()
    {
        this.refCount.incrementAndGet();
    }

    public void dispose() throws IOException
    {
        if ( !this.isClosed )
        {
            this.searcher.close();
            this.searcher.getIndexReader().close();
            this.isClosed = true;
        }
    }

    public void detachOrClose() throws IOException
    {
        if ( this.refCount.get() == 0 )
        {
            dispose();
        }
        else
        {
            this.detached = true;
        }
    }

    public boolean close() throws IOException
    {
        if ( this.isClosed || this.refCount.get() == 0 )
        {
            return true;
        }

        boolean reallyClosed = false;
        if ( this.refCount.decrementAndGet() <= 0 && this.detached )
        {
            dispose();
            reallyClosed = true;
        }
        return reallyClosed;
    }

    boolean closeStrict()
    {
        try
        {
            return close();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    boolean isClosed()
    {
        return isClosed;
    }
}
