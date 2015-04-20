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
package org.neo4j.index.impl.lucene;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;

class IndexReference
{
    private final IndexIdentifier identifier;
    private final IndexWriter writer;
    private final IndexSearcher searcher;
    private final AtomicInteger refCount = new AtomicInteger( 0 );
    private boolean searcherIsClosed;
    private boolean writerIsClosed;

    /**
     * We need this because we only want to close the reader/searcher if
     * it has been detached... i.e. the {@link LuceneDataSource} no longer
     * has any reference to it, only an iterator out in the client has a ref.
     * And when that client calls close() it should be closed.
     */
    private volatile boolean detached;

    private final AtomicBoolean stale = new AtomicBoolean();

    public IndexReference( IndexIdentifier identifier, IndexSearcher searcher, IndexWriter writer )
    {
        this.identifier = identifier;
        this.searcher = searcher;
        this.writer = writer;
    }

    public IndexSearcher getSearcher()
    {
        return this.searcher;
    }
    
    public IndexWriter getWriter()
    {
        return writer;
    }

    public IndexIdentifier getIdentifier()
    {
        return identifier;
    }

    void incRef()
    {
        this.refCount.incrementAndGet();
    }
    
    public synchronized void dispose( boolean writerAlso ) throws IOException
    {
        if ( !searcherIsClosed )
        {
            searcher.close();
            searcher.getIndexReader().close();
            searcherIsClosed = true;
        }
        
        if ( writerAlso && !writerIsClosed )
        {
            writer.close();
            writerIsClosed = true;
        }
    }

    public /*synchronized externally*/ void detachOrClose() throws IOException
    {
        if ( this.refCount.get() == 0 )
        {
            dispose( false );
        }
        else
        {
            this.detached = true;
        }
    }

    synchronized boolean close()
    {
        try
        {
            if ( this.searcherIsClosed || this.refCount.get() == 0 )
            {
                return true;
            }

            boolean reallyClosed = false;
            if ( this.refCount.decrementAndGet() <= 0 && this.detached )
            {
                dispose( false );
                reallyClosed = true;
            }
            return reallyClosed;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    /*synchronized externally*/ boolean isClosed()
    {
        return searcherIsClosed;
    }

    /*synchronized externally*/ boolean checkAndClearStale()
    {
        return stale.compareAndSet( true, false );
    }

    public synchronized void setStale()
    {
        stale.set( true );
    }
}
