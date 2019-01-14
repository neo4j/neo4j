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
package org.neo4j.index.internal.gbptree;

import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.function.Supplier;

import org.neo4j.io.pagecache.PageCursor;

class SimpleIdProvider implements IdProvider
{
    private final Queue<Pair<Long,Long>> releasedIds = new LinkedList<>();
    private final Supplier<PageCursor> cursorSupplier;
    private long lastId;

    SimpleIdProvider( Supplier<PageCursor> cursorSupplier )
    {
        this.cursorSupplier = cursorSupplier;
        reset();
    }

    @Override
    public long acquireNewId( long stableGeneration, long unstableGeneration )
    {
        if ( !releasedIds.isEmpty() )
        {
            Pair<Long,Long> free = releasedIds.peek();
            if ( free.getLeft() <= stableGeneration )
            {
                releasedIds.poll();
                Long pageId = free.getRight();
                zapPage( pageId );
                return pageId;
            }
        }
        lastId++;
        return lastId;
    }

    @Override
    public void releaseId( long stableGeneration, long unstableGeneration, long id )
    {
        releasedIds.add( Pair.of( unstableGeneration, id ) );
    }

    long lastId()
    {
        return lastId;
    }

    void reset()
    {
        releasedIds.clear();
        lastId = IdSpace.MIN_TREE_NODE_ID - 1;
    }

    private void zapPage( Long pageId )
    {
        try ( PageCursor cursor = cursorSupplier.get() )
        {
            cursor.next( pageId );
            cursor.zapPage();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Could not go to page " + pageId );
        }
    }
}
