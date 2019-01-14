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
package org.neo4j.server.rest.paging;

import java.time.Clock;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LeaseManager
{
    private final Clock clock;
    private Map<String, Lease> leases = new ConcurrentHashMap<>();

    public LeaseManager( Clock clock )
    {
        this.clock = clock;
    }

    public Lease createLease( long seconds, PagedTraverser leasedTraverser ) throws LeaseAlreadyExpiredException
    {
        if ( seconds < 1 )
        {
            return null;
        }

        Lease lease = new Lease( leasedTraverser, seconds, clock );
        leases.put( lease.getId(), lease );

        return lease;
    }

    public Lease getLeaseById( String id )
    {
        pruneOldLeasesByNaivelyIteratingThroughAllOfThem();
        Lease lease = leases.get( id );

        if ( lease != null )
        {
            lease.renew();
        }

        return lease;
    }

    private void pruneOldLeasesByNaivelyIteratingThroughAllOfThem()
    {
        for ( String key : leases.keySet() )
        {
            try
            {
                Lease lease = leases.get( key );
                if ( lease.getStartTime() + lease.getPeriod() < clock.millis() )
                {
                    remove( key );
                }
            }
            catch ( Exception e )
            {
                // do nothing - if something goes wrong, it just means another
                // thread already nuked the lease
            }
        }
    }

    public Clock getClock()
    {
        return clock;
    }

    public void remove( String key )
    {
        if ( leases.containsKey( key ) )
        {
            leases.remove( key );
        }
    }
}
