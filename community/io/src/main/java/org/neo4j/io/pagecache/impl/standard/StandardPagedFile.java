/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.io.pagecache.impl.standard;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;

import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageLock;
import org.neo4j.io.pagecache.PagedFile;

import static org.neo4j.io.pagecache.impl.standard.PageTable.PinnablePage;

public class StandardPagedFile implements PagedFile
{
    static final int BEING_LOADED = -1;
    static final int NOT_LOADED = -2;
    static final Object NULL = new Object();

    private final PageTable table;
    private final PageTable.PageIO pageIO;
    private ConcurrentMap<Long, Object> addressTranslationTable = new ConcurrentHashMap<>();

    public StandardPagedFile( PageTable table, StoreChannel channel )
    {
        this.table = table;
        this.pageIO = new StandardPageIO(channel);
    }

    @Override
    public void pin( PageCursor cursor, PageLock lock, long pageId ) throws IOException
    {
        for (;;)
        {
            Object pageRef = addressTranslationTable.get( pageId );
            if ( pageRef == null )
            {
                addressTranslationTable.putIfAbsent( pageId, NULL );
            }
            else if ( pageRef == NULL )
            {
                CountDownLatch latch = new CountDownLatch( 1 );
                if ( addressTranslationTable.replace( pageId, pageRef, latch ) )
                {
                    PinnablePage page = table.load( pageIO, pageId, lock );
                    addressTranslationTable.put( pageId, page );
                    latch.countDown();
                    return; // yay!
                }
            }
            else if ( pageRef instanceof CountDownLatch )
            {
                try
                {
                    ((CountDownLatch) pageRef).await();
                }
                catch ( InterruptedException e )
                {
                    Thread.interrupted();
                    throw new IOException( "Interrupted while waiting for page load.", e );
                }
            }
            else
            {
                // happy case where we have a page id
                PinnablePage page = (PinnablePage) pageRef;
                if ( page.pin( pageIO, pageId, lock ) )
                {
                    return; // yay!
                }
                addressTranslationTable.replace( pageId, page, NULL );
            }
        }
    }

    @Override
    public void unpin( PageCursor cursor )
    {

    }

    @Override
    public int pageSize()
    {
        return 0;
    }
}
