/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.server.rest.web.paging;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LeaseManager<T extends Leasable>
{
    private final Clock clock;
    private Map<String, Lease<T>> leases = new ConcurrentHashMap<String, Lease<T>>();

    public LeaseManager( Clock clock )
    {
        this.clock = clock;
    }

    public Lease<T> createLease( long seconds, T leasedObject ) throws LeaseAlreadyExpiredException
    {
        if ( seconds < 1 )
        {
            return null;
        }

        Lease<T> lease = new Lease<T>( leasedObject, seconds, clock );
        leases.put( lease.getId(), lease );
        return lease;
    }

    public Lease<T> getLeaseById( String id )
    {
        pruneOldLeasesByNaivelyIteratingThroughAllOfThem();
        Lease<T> lease = leases.get( id );
        
        if(lease!= null) {
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
                Lease<T> lease = leases.get( key );
                if ( lease.getStartTime() + lease.getPeriod() < clock.currentTimeInMilliseconds() )
                {
                    leases.remove( key );
                }
            }
            catch ( Exception e )
            {
                // do nothing - if something goes wrong, it just means another
                // thread already nuked the lease
            }
        }
    }
}
