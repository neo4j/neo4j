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

import org.neo4j.graphdb.event.ErrorState;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.kernel.AvailabilityGuard;

public class HaKernelPanicHandler implements KernelEventHandler
{
    private final AvailabilityGuard availabilityGuard;
    private final HaRecovery recovery;

    public HaKernelPanicHandler( AvailabilityGuard availabilityGuard, HaRecovery recovery )
    {
        this.availabilityGuard = availabilityGuard;
        this.recovery = recovery;
    }

    @Override
    public void beforeShutdown()
    {
    }

    @Override
    public void kernelPanic( ErrorState error )
    {
        if ( error == ErrorState.TX_MANAGER_NOT_OK )
        {
            recovery.recover();
        }
        else if ( error == ErrorState.STORAGE_MEDIA_FULL )
        {
            // Fatal error - Permanently unavailable
            availabilityGuard.shutdown();
        }
    }

    @Override
    public Object getResource()
    {
        return null;
    }

    @Override
    public ExecutionOrder orderComparedTo( KernelEventHandler other )
    {
        return ExecutionOrder.DOESNT_MATTER;
    }

}
