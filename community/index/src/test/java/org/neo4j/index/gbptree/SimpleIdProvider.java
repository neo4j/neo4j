/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.index.gbptree;

import org.apache.commons.lang3.tuple.Pair;

import java.util.LinkedList;
import java.util.Queue;

class SimpleIdProvider implements IdProvider
{
    private final Queue<Pair<Long,Long>> releasedIds = new LinkedList<>();
    private long lastId;

    SimpleIdProvider()
    {
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
                return free.getRight();
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
}
