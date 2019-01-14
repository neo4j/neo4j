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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil;

class GBPTreeLock
{
    private static final long stateOffset = UnsafeUtil.getFieldOffset( GBPTreeLock.class, "state" );
    private static final long writerLockBit = 0x00000000_00000001L;
    private static final long cleanerLockBit = 0x00000000_00000002L;
    private volatile long state;

    // Used for testing
    GBPTreeLock copy()
    {
        GBPTreeLock copy = new GBPTreeLock();
        copy.state = state;
        return copy;
    }

    void writerLock()
    {
        doLock( writerLockBit );
    }

    void writerUnlock()
    {
        doUnlock( writerLockBit );
    }

    void cleanerLock()
    {
        doLock( cleanerLockBit );
    }

    void cleanerUnlock()
    {
        doUnlock( cleanerLockBit );
    }

    void writerAndCleanerLock()
    {
        doLock( writerLockBit | cleanerLockBit );
    }

    void writerAndCleanerUnlock()
    {
        doUnlock( writerLockBit | cleanerLockBit );
    }

    private void doLock( long targetLockBit )
    {
        long currentState;
        long newState;
        do
        {
            currentState = state;
            while ( !canLock( currentState, targetLockBit ) )
            {
                // sleep
                sleep();
                currentState = state;
            }
            newState = currentState | targetLockBit;
        } while ( !UnsafeUtil.compareAndSwapLong( this, stateOffset, currentState, newState ) );
    }

    private void doUnlock( long targetLockBit )
    {
        long currentState;
        long newState;
        do
        {
            currentState = state;
            if ( !canUnlock( currentState, targetLockBit) )
            {
                throw new IllegalStateException( "Can not unlock lock that is already locked" );
            }
            newState = currentState & ~targetLockBit;
        }
        while ( !UnsafeUtil.compareAndSwapLong( this, stateOffset, currentState, newState ) );
    }

    private boolean canLock( long state, long targetLockBit )
    {
        return (state & targetLockBit) == 0;
    }

    private boolean canUnlock( long state, long targetLockBit )
    {
        return (state & targetLockBit) == targetLockBit;
    }

    private void sleep()
    {
        LockSupport.parkNanos( TimeUnit.MILLISECONDS.toNanos( 10 ) );
    }
}
