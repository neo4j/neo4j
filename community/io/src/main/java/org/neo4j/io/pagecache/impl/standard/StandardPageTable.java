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

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.io.pagecache.PageLock;

public class StandardPageTable implements PageTable
{
    private final AtomicReference<StandardPinnablePage> freeList;

    public StandardPageTable( long memoryPoolSizeInBytes )
    {
        freeList = new AtomicReference<>();
        int pageSize = 1024;

        for ( int i = 0; i < memoryPoolSizeInBytes / pageSize; i++ )
        {
            StandardPinnablePage page = new StandardPinnablePage(
                    ByteBuffer.allocateDirect( pageSize ) );
            page.next = freeList.get();
            freeList.compareAndSet( page.next, page );
        }
    }

    @Override
    public PinnablePage load( PageIO io, long pageId, PageLock lock )
    {
        StandardPinnablePage page = nextFreePage();
        page.reset( io, pageId );
        page.pin( io, pageId, lock );
        io.read( pageId, page.buffer() );
        return page;
    }

    private StandardPinnablePage nextFreePage()
    {
        StandardPinnablePage page;
        do {
            page = freeList.get();
            if ( page == null )
            {
                // TODO: Wait for a latch from the sweep thread here.
                continue;
            }
        } while ( page == null || !freeList.compareAndSet( page, page.next ));
        return page;
    }
}
