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
package org.neo4j.io.pagecache;

import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.concurrent.BinaryLatch;

/**
 * A tiny dumb lock manager built specifically for the page cache stress test, because it needs something to represent
 * the entity locks since page write locks are not exclusive. Also, for the stress test, a simple array of
 * ReentrantLocks would take up too much memory.
 */
public class TinyLockManager
{
    private final ConcurrentHashMap<Integer,BinaryLatch> map = new ConcurrentHashMap<>( 64, 0.75f, 64 );

    public void lock( int recordId )
    {
        Integer record = recordId;
        BinaryLatch myLatch = new BinaryLatch();
        for (;;)
        {
            BinaryLatch existingLatch = map.putIfAbsent( record, myLatch );
            if ( existingLatch == null )
            {
                break;
            }
            else
            {
                existingLatch.await();
            }
        }
    }

    public boolean tryLock( int recordId )
    {
        Integer record = recordId;
        BinaryLatch myLatch = new BinaryLatch();
        BinaryLatch existingLatch = map.putIfAbsent( record, myLatch );
        return existingLatch == null;
    }

    public void unlock( int recordId )
    {
        map.remove( recordId ).release();
    }
}
