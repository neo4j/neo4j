/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.coreedge.raft.locks;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.helpers.Clock;

public class CoreServiceRegistry
{
    private final Clock clock;

    public enum ServiceType
    {
        LOCK_MANAGER, ;

    }

    private HashMap<ServiceType,CoreServiceAssignment> serviceMap = new HashMap<>();

    public CoreServiceRegistry( Clock clock )
    {
        this.clock = clock;
    }

    public synchronized void registerProvider( ServiceType serviceType, CoreServiceAssignment assignment )
    {
        serviceMap.put( serviceType, assignment );
        notifyAll();
    }

    synchronized public CoreServiceAssignment lookupService( ServiceType serviceType, long time, TimeUnit unit ) throws TimeoutException
    {
        CoreServiceAssignment assignment;
        long endTime = clock.currentTimeMillis() + unit.toMillis( time );

        while ( (assignment = serviceMap.get( serviceType )) == null )
        {
            if ( clock.currentTimeMillis() >= endTime )
            {
                throw new TimeoutException();
            }

            try
            {
                wait( 1000 );
            }
            catch ( InterruptedException e )
            {
                throw new TimeoutException( e.getMessage() );
            }
        }

        return assignment;
    }
}
