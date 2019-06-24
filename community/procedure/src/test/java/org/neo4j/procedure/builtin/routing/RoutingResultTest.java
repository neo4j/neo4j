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

import org.neo4j.configuration.helpers.SocketAddress;

import static java.util.Collections.emptyList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RoutingResultTest
{
    @Test
    void shouldExposeEndpointsWhenEmpty()
    {
        var result = new RoutingResult( emptyList(), emptyList(), emptyList(), 42 );

        assertThat( result.readEndpoints(), is( empty() ) );
        assertThat( result.writeEndpoints(), is( empty() ) );
        assertThat( result.routeEndpoints(), is( empty() ) );
    }

    @Test
    void shouldExposeEndpoints()
    {
        var address1 = new SocketAddress( "localhost", 1 );
        var address2 = new SocketAddress( "localhost", 2 );
        var address3 = new SocketAddress( "localhost", 3 );

        var readers = List.of( address1, address3 );
        var writers = List.of( address3, address2 );
        var routers = List.of( address1, address2, address3 );

        var result = new RoutingResult( routers, writers, readers, 42 );

        assertEquals( result.readEndpoints(), readers );
        assertEquals( result.writeEndpoints(), writers );
        assertEquals( result.routeEndpoints(), routers );
    }

    @Test
    void shouldExposeTtl()
    {
        var result = new RoutingResult( emptyList(), emptyList(), emptyList(), 424242 );

        assertEquals( 424242, result.ttlMillis() );
    }

    @Test
    void shouldCheckIfContainsEndpoints()
    {
        var address = new SocketAddress( "localhost", 1 );
        var emptyResult = new RoutingResult( emptyList(), emptyList(), emptyList(), 42 );
        var nonEmptyResult = new RoutingResult( List.of( address ), List.of( address ), List.of( address ), 42 );

        assertTrue( emptyResult.containsNoEndpoints() );
        assertFalse( nonEmptyResult.containsNoEndpoints() );
    }
}
