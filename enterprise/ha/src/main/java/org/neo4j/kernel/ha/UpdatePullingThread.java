/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.ha;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

class UpdatePullingThread extends Thread
{
    interface Operation
    {
        void perform();
    }

    private volatile boolean halted;
    private volatile boolean paused = true;
    private final AtomicInteger targetTicket = new AtomicInteger(), currentTicket = new AtomicInteger();
    private final Operation operation;

    UpdatePullingThread( Operation operation )
    {
        super( "UpdatePuller" );
        this.operation = operation;
    }

    @Override
    public void run()
    {
        while ( !halted )
        {
            if ( !paused )
            {
                int round = targetTicket.get();
                if ( currentTicket.get() < round )
                {
                    operation.perform();
                    currentTicket.set( round );
                    continue;
                }
            }

            LockSupport.parkNanos( 100_000_000 );
        }
    }

    void halt()
    {
        this.halted = true;
    }

    void pause( boolean paused )
    {
        boolean wasPaused = this.paused;
        this.paused = paused;
        if ( wasPaused )
        {
            LockSupport.unpark( this );
        }
    }

    int poke()
    {
        int result = this.targetTicket.incrementAndGet();
        LockSupport.unpark( this );
        return result;
    }

    int current()
    {
        return currentTicket.get();
    }

    public boolean isActive()
    {
        // Should return true if this is active
        return !halted && !paused;
    }

    @Override
    public String toString()
    {
        return "UpdatePuller[halted:" + halted + ", paused:" + paused +
                ", current:" + currentTicket + ", target:" + targetTicket + "]";
    }
}
