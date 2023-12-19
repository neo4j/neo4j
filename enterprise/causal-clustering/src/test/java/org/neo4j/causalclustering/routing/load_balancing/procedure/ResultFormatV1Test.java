/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.routing.load_balancing.procedure;

import org.junit.Test;

import java.util.List;

import org.neo4j.causalclustering.routing.Endpoint;
import org.neo4j.causalclustering.routing.load_balancing.LoadBalancingResult;
import org.neo4j.helpers.AdvertisedSocketAddress;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

public class ResultFormatV1Test
{
    @Test
    public void shouldSerializeToAndFromRecordFormat()
    {
        // given
        List<Endpoint> writers = asList(
                Endpoint.write( new AdvertisedSocketAddress( "write", 1 ) ),
                Endpoint.write( new AdvertisedSocketAddress( "write", 2 ) ),
                Endpoint.write( new AdvertisedSocketAddress( "write", 3 ) ) );
        List<Endpoint> readers = asList(
                Endpoint.read( new AdvertisedSocketAddress( "read", 4 ) ),
                Endpoint.read( new AdvertisedSocketAddress( "read", 5 ) ),
                Endpoint.read( new AdvertisedSocketAddress( "read", 6 ) ),
                Endpoint.read( new AdvertisedSocketAddress( "read", 7 ) ) );
        List<Endpoint> routers = singletonList(
                Endpoint.route( new AdvertisedSocketAddress( "route", 8 ) )
        );

        long ttlSeconds = 5;
        LoadBalancingResult original = new LoadBalancingResult( routers, writers, readers, ttlSeconds * 1000 );

        // when
        Object[] record = ResultFormatV1.build( original );

        // then
        LoadBalancingResult parsed = ResultFormatV1.parse( record );

        assertEquals( original, parsed );
    }

    @Test
    public void shouldSerializeToAndFromRecordFormatWithNoEntries()
    {
        // given
        List<Endpoint> writers = emptyList();
        List<Endpoint> readers = emptyList();
        List<Endpoint> routers = emptyList();

        long ttlSeconds = 0;
        LoadBalancingResult original = new LoadBalancingResult( routers, writers, readers, ttlSeconds * 1000 );

        // when
        Object[] record = ResultFormatV1.build( original );

        // then
        LoadBalancingResult parsed = ResultFormatV1.parse( record );

        assertEquals( original, parsed );
    }
}
