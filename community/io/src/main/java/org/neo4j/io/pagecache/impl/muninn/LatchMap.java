/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.io.pagecache.impl.muninn;

import org.neo4j.concurrent.BinaryLatch;
import org.neo4j.unsafe.impl.internal.dragons.FeatureToggles;
import org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil;

final class LatchMap
{
    static final class Latch extends BinaryLatch
    {
        private LatchMap latchMap;
        private int index;

        @Override
        public void release()
        {
            latchMap.setLatch( index, null );
            super.release();
        }
    }

    private static final int faultLockStriping = FeatureToggles.getInteger( LatchMap.class, "faultLockStriping", 128 );
    private static final long faultLockMask = faultLockStriping - 1;
    private static final int latchesArrayBase = UnsafeUtil.arrayBaseOffset( Latch[].class );
    private static final int latchesArrayScale = UnsafeUtil.arrayIndexScale( Latch[].class );

    private final Latch[] latches;

    LatchMap()
    {
        latches = new Latch[faultLockStriping];
    }

    private long offset( int index )
    {
        return UnsafeUtil.arrayOffset( index, latchesArrayBase, latchesArrayScale );
    }

    private void setLatch( int index, BinaryLatch newValue )
    {
        UnsafeUtil.putObjectVolatile( latches, offset( index ), newValue );
    }

    private boolean compareAndSetLatch( int index, Latch expected, Latch update )
    {
        return UnsafeUtil.compareAndSwapObject( latches, offset( index ), expected, update );
    }

    private Latch getLatch( int index )
    {
        return (Latch) UnsafeUtil.getObjectVolatile( latches, offset( index ) );
    }

    Latch takeOrAwaitLatch( long identifier )
    {
        int index = index( identifier );
        Latch latch = getLatch( index );
        while ( latch == null )
        {
            latch = new Latch();
            if ( compareAndSetLatch( index, null, latch ) )
            {
                latch.latchMap = this;
                latch.index = index;
                return latch;
            }
            latch = getLatch( index );
        }
        latch.await();
        return null;
    }

    private int index( long identifier )
    {
        return (int) (mix( identifier ) & faultLockMask);
    }

    private long mix( long identifier )
    {
        identifier ^= identifier << 21;
        identifier ^= identifier >>> 35;
        identifier ^= identifier << 4;
        return identifier;
    }
}
