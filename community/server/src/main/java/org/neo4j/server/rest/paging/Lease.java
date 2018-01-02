/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.server.rest.paging;

import java.util.UUID;

import org.neo4j.helpers.Clock;

public class Lease
{
    private long startTime;
    public final PagedTraverser leasedTraverser;
    private final String id;
    private long leasePeriod;
    private final Clock clock;

    Lease( PagedTraverser leasedTraverser, long leasePeriodInSeconds, Clock clock ) throws LeaseAlreadyExpiredException
    {
        if ( leasePeriodInSeconds < 0 )
        {
            throw new LeaseAlreadyExpiredException( String.format( "Negative lease periods [%d] are not permitted",
                    leasePeriodInSeconds ) );
        }

        this.clock = clock;
        this.leasedTraverser = leasedTraverser;
        this.startTime = clock.currentTimeMillis();
        this.leasePeriod = leasePeriodInSeconds * 1000;
        this.id = toHexOnly( UUID.randomUUID() );
    }

    public String getId()
    {
        return id;
    }

    private String toHexOnly( UUID uuid )
    {
        return uuid.toString()
                .replaceAll( "-", "" );
    }

    public PagedTraverser getLeasedItemAndRenewLease()
    {
        renew();
        return leasedTraverser;
    }

    public void renew()
    {
        if ( !expired() )
        {
            startTime = clock.currentTimeMillis();
        }
    }

    public boolean expired()
    {
        return startTime + leasePeriod < clock.currentTimeMillis();
    }

    public long getStartTime()
    {
        return startTime;
    }

    public long getPeriod()
    {
        return leasePeriod;
    }
}
