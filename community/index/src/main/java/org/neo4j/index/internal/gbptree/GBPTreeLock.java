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
package org.neo4j.index.internal.gbptree;

import org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil;

class GBPTreeLock
{
    private static final long stateOffset = UnsafeUtil.getFieldOffset( GBPTreeLock.class, "state" );
    private static final long writerLockBit = 0x00000000_00000001L;
    private static final long cleanerLockBit = 0x00000000_00000002L;
    private volatile long state;

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

    private void doLock( long targetLockBit )
    {
        long currentState;
        long newState;
        do
        {
            currentState = state;
            while ( isLocked( currentState, targetLockBit ) )
            {
                // sleep
                sleep();
                currentState = state;
            }
            newState = currentState | targetLockBit;
        } while ( !UnsafeUtil.compareAndSwapLong( this, stateOffset, currentState, newState ) );
    }

    private boolean isLocked( long state, long targetLockBit )
    {
        return (state & targetLockBit) == targetLockBit;
    }

    private void doUnlock( long targetLockBit )
    {
        long currentState;
        long newState;
        do
        {
            currentState = state;
            if ( !isLocked( currentState, targetLockBit) )
            {
                throw new IllegalStateException( "Can not unlock lock that is already locked" );
            }
            newState = currentState & ~targetLockBit;
        }
        while ( !UnsafeUtil.compareAndSwapLong( this, stateOffset, currentState, newState ) );
    }

    private void sleep()
    {
        try
        {
            Thread.sleep( 10 );
        }
        catch ( InterruptedException e )
        {
            // todo what to do in this case
            throw new RuntimeException( e );
        }
    }
}
