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
package org.neo4j.causalclustering.routing.procedure;

import java.util.List;
import java.util.stream.Stream;

import org.neo4j.causalclustering.routing.Endpoint;
import org.neo4j.causalclustering.routing.Role;
import org.neo4j.helpers.AdvertisedSocketAddress;

import static java.util.stream.Collectors.toList;

public final class RoutingResultFormatHelper
{

    public static List<Endpoint> parseEndpoints( Object[] addresses, Role role )
    {
        return Stream.of( addresses )
                .map( rawAddress -> parseAddress( (String) rawAddress ) )
                .map( address -> new Endpoint( address, role ) )
                .collect( toList() );
    }

    private static AdvertisedSocketAddress parseAddress( String address )
    {
        String[] split = address.split( ":" );
        return new AdvertisedSocketAddress( split[0], Integer.valueOf( split[1] ) );
    }
}
