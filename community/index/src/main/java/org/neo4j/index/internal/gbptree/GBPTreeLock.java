/*
 * Copyright (c) "Neo4j"
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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.neo4j.util.VisibleForTesting;

class GBPTreeLock
{
    private static final long writerLockBit = 0x00000000_00000001L;
    private static final long cleanerLockBit = 0x00000000_00000002L;
    @SuppressWarnings( "unused" ) // accessed via VarHandle
    private long state;
    private static final VarHandle STATE;

    static
    {
        try
        {
            MethodHandles.Lookup l = MethodHandles.lookup();
            STATE = l.findVarHandle( GBPTreeLock.class, "state", long.class );
        }
        catch ( ReflectiveOperationException e )
        {
            throw new ExceptionInInitializerError( e );
        }
    }

    // Used for testing
    GBPTreeLock copy()
    {
        GBPTreeLock copy = new GBPTreeLock();
        STATE.setVolatile( copy, (long) STATE.getVolatile( this ) );
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
            currentState = (long) STATE.getVolatile( this );
            while ( !canLock( currentState, targetLockBit ) )
            {
                // sleep
                sleep();
                currentState = (long) STATE.getVolatile( this );
            }
            newState = currentState | targetLockBit;
        } while ( !STATE.weakCompareAndSet( this, currentState, newState ) );
    }

    private void doUnlock( long targetLockBit )
    {
        long currentState;
        long newState;
        do
        {
            currentState = (long) STATE.getVolatile( this );
            if ( !canUnlock( currentState, targetLockBit) )
            {
                throw new IllegalStateException( "Can not unlock lock that is already locked" );
            }
            newState = currentState & ~targetLockBit;
        }
        while ( !STATE.weakCompareAndSet( this, currentState, newState ) );
    }

    private static boolean canLock( long state, long targetLockBit )
    {
        return (state & targetLockBit) == 0;
    }

    private static boolean canUnlock( long state, long targetLockBit )
    {
        return (state & targetLockBit) == targetLockBit;
    }

    private static void sleep()
    {
        LockSupport.parkNanos( TimeUnit.MILLISECONDS.toNanos( 10 ) );
    }

    /**
     * Force reset the lock state, to avoid threads getting stuck in tests.
     */
    @VisibleForTesting
    void forceUnlock()
    {
        STATE.setVolatile( this, 0 );
    }
}
