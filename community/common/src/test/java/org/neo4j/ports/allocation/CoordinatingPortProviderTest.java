/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.ports.allocation;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CoordinatingPortProviderTest
{
    @Test
    void shouldProvideUniquePorts()
    {
        PortRepository portRepository = mock( PortRepository.class );
        PortProvider portProvider = new CoordinatingPortProvider( portRepository, port -> false );

        when( portRepository.reserveNextPort( "foo" ) ).thenReturn( 40, 41 );
        int port1 = portProvider.getNextFreePort( "foo" );
        int port2 = portProvider.getNextFreePort( "foo" );

        assertThat( port1, is( not( equalTo( port2 ) ) ) );
    }

    @Test
    void shouldSkipReservedPorts()
    {
        PortRepository portRepository = mock( PortRepository.class );
        PortProvider portProvider = new CoordinatingPortProvider( portRepository, port -> false );

        when( portRepository.reserveNextPort( "foo" ) ).thenReturn( 40, 41, 43 );
        assertThat( portProvider.getNextFreePort( "foo" ), is( 40 ) );
        assertThat( portProvider.getNextFreePort( "foo" ), is( 41 ) );
        assertThat( portProvider.getNextFreePort( "foo" ), is( 43 ) );
    }

    @Test
    void shouldSkipOccupiedPorts()
    {
        PortRepository portRepository = mock( PortRepository.class );
        PortProbe portProbe = mock( PortProbe.class );
        PortProvider portProvider = new CoordinatingPortProvider( portRepository, portProbe );

        when( portRepository.reserveNextPort( "foo" ) ).thenReturn( 40, 41, 42, 43 );
        when( portProbe.isOccupied( 40 ) ).thenReturn( false );
        when( portProbe.isOccupied( 41 ) ).thenReturn( false );
        when( portProbe.isOccupied( 42 ) ).thenReturn( true );
        when( portProbe.isOccupied( 43 ) ).thenReturn( false );
        assertThat( portProvider.getNextFreePort( "foo" ), is( 40 ) );
        assertThat( portProvider.getNextFreePort( "foo" ), is( 41 ) );
        assertThat( portProvider.getNextFreePort( "foo" ), is( 43 ) );
    }
}
