/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.causalclustering.routing;

import java.util.Objects;
import java.util.Optional;

import org.neo4j.helpers.AdvertisedSocketAddress;

/**
 * This class binds a certain role with an address and
 * thus defines a reachable endpoint with defined capabilities.
 */
public class Endpoint
{
    private final AdvertisedSocketAddress address;
    private final Role role;

    public Endpoint( AdvertisedSocketAddress address, Role role )
    {
        this.address = address;
        this.role = role;
    }

    public Endpoint( AdvertisedSocketAddress address, Role role, String dbName )
    {
        this.address = address;
        this.role = role;
    }

    public AdvertisedSocketAddress address()
    {
        return address;
    }

    public static Endpoint write( AdvertisedSocketAddress address )
    {
        return new Endpoint( address, Role.WRITE );
    }

    public static Endpoint read( AdvertisedSocketAddress address )
    {
        return new Endpoint( address, Role.READ );
    }

    public static Endpoint route( AdvertisedSocketAddress address )
    {
        return new Endpoint( address, Role.ROUTE );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        Endpoint endPoint = (Endpoint) o;
        return Objects.equals( address, endPoint.address ) && role == endPoint.role;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( address, role );
    }

    @Override
    public String toString()
    {
        return "EndPoint{" + "address=" + address + ", role=" + role + '}';
    }
}
