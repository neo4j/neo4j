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

import java.util.UUID;

public class Lease<T extends Leasable>
{
    public final long expirationTime;
    public final T leasedItem;
    private final String id;

    Lease( T leasedItem, long absoluteExpirationTimeInMilliseconds ) throws LeaseAlreadyExpiredException
    {
        if ( absoluteExpirationTimeInMilliseconds - System.currentTimeMillis() < 0 )
        {
            throw new LeaseAlreadyExpiredException( String.format(
                    "Trying to create a lease [%d] milliseconds in the past is not permitted",
                    absoluteExpirationTimeInMilliseconds - System.currentTimeMillis() ) );
        }

        this.leasedItem = leasedItem;
        this.expirationTime = absoluteExpirationTimeInMilliseconds;
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

    public T getLeasedItem()
    {
        return leasedItem;
    }
}
