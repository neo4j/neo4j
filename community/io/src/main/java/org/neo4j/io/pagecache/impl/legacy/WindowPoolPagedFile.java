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
package org.neo4j.io.pagecache.impl.legacy;

import java.io.IOException;

import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageLock;
import org.neo4j.io.pagecache.PageLockException;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.impl.common.OffsetTrackingCursor;
import org.neo4j.kernel.impl.nioneo.store.OperationType;
import org.neo4j.kernel.impl.nioneo.store.PersistenceWindow;
import org.neo4j.kernel.impl.nioneo.store.WindowPool;

public class WindowPoolPagedFile implements PagedFile
{
    private final WindowPool pool;
    private final int pageSize;
    private final StoreChannel channel;
    private final int recordSize;

    public WindowPoolPagedFile( WindowPool pool, int pageSize, StoreChannel channel, int transitionalRecordSizeUntilWeHaveNewPageCache )
    {
        this.pool = pool;
        this.pageSize = pageSize;
        this.channel = channel;
        recordSize = transitionalRecordSizeUntilWeHaveNewPageCache;
    }

    @Override
    public void pin( PageCursor cursor, PageLock lock, long pageId ) throws IOException
    {
        assert cursor instanceof OffsetTrackingCursor : "Cursor must come from this page cache";
        OffsetTrackingCursor trackingCursor = (OffsetTrackingCursor)cursor;

        PersistenceWindow window = pool.acquire(
                pageSize / recordSize * pageId,
                lock == PageLock.READ ? OperationType.READ : OperationType.WRITE );

        trackingCursor.reset( new WindowPoolPage( window ) );
    }

    @Override
    public void unpin( PageCursor cursor )
    {
        OffsetTrackingCursor trackingCursor = (OffsetTrackingCursor) cursor;
        WindowPoolPage page = (WindowPoolPage) trackingCursor.getPage();
        try
        {
            page.release( pool );
        }
        catch ( IOException e )
        {
            throw new PageLockException( e );
        }
    }

    @Override
    public int pageSize()
    {
        return pageSize;
    }
}
