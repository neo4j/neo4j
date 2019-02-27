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
package org.neo4j.procedure.builtin.routing;

import org.junit.jupiter.api.Test;

import java.util.List;

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.values.AnyValue;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;

class RoutingResultFormatTest
{
    @Test
    void shouldSerializeToAndFromRecordFormat()
    {
        // given
        List<AdvertisedSocketAddress> writers = asList(
                new AdvertisedSocketAddress( "write", 1 ),
                new AdvertisedSocketAddress( "write", 2 ),
                new AdvertisedSocketAddress( "write", 3 ) );
        List<AdvertisedSocketAddress> readers = asList(
                new AdvertisedSocketAddress( "read", 4 ),
                new AdvertisedSocketAddress( "read", 5 ),
                new AdvertisedSocketAddress( "read", 6 ),
                new AdvertisedSocketAddress( "read", 7 ) );
        List<AdvertisedSocketAddress> routers = singletonList(
                new AdvertisedSocketAddress( "route", 8 ) );

        long ttlSeconds = 5;
        RoutingResult original = new RoutingResult( routers, writers, readers, ttlSeconds * 1000 );

        // when
        AnyValue[] record = RoutingResultFormat.build( original );

        // then
        RoutingResult parsed = RoutingResultFormat.parse( record );

        assertEquals( original, parsed );
    }

    @Test
    void shouldSerializeToAndFromRecordFormatWithNoEntries()
    {
        // given
        List<AdvertisedSocketAddress> writers = emptyList();
        List<AdvertisedSocketAddress> readers = emptyList();
        List<AdvertisedSocketAddress> routers = emptyList();

        long ttlSeconds = 0;
        RoutingResult original = new RoutingResult( routers, writers, readers, ttlSeconds * 1000 );

        // when
        AnyValue[] record = RoutingResultFormat.build( original );

        // then
        RoutingResult parsed = RoutingResultFormat.parse( record );

        assertEquals( original, parsed );
    }
}
