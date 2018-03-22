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
package org.neo4j.causalclustering.routing.multi_cluster.procedure;

import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.causalclustering.routing.Endpoint;
import org.neo4j.causalclustering.routing.multi_cluster.MultiClusterRoutingResult;
import org.neo4j.helpers.AdvertisedSocketAddress;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class MultiClusterRoutingResultFormatTest
{

    @Test
    public void shouldSerializeToAndFromRecordFormat()
    {
        List<Endpoint> fooRouters = asList(
                Endpoint.route( new AdvertisedSocketAddress( "host1", 1 ) ),
                Endpoint.route( new AdvertisedSocketAddress( "host2", 1 ) ),
                Endpoint.route( new AdvertisedSocketAddress( "host3", 1 ) )
        );

        List<Endpoint> barRouters = asList(
                Endpoint.route( new AdvertisedSocketAddress( "host4", 1 ) ),
                Endpoint.route( new AdvertisedSocketAddress( "host5", 1 ) ),
                Endpoint.route( new AdvertisedSocketAddress( "host6", 1 ) )
        );

        Map<String,List<Endpoint>> routers = new HashMap<>();
        routers.put( "foo", fooRouters );
        routers.put( "bar", barRouters );

        long ttlSeconds = 5;
        MultiClusterRoutingResult original = new MultiClusterRoutingResult( routers, ttlSeconds * 1000 );

        Object[] record = MultiClusterRoutingResultFormat.build( original );

        MultiClusterRoutingResult parsed = MultiClusterRoutingResultFormat.parse( record );

        assertEquals( original, parsed );
    }
}
