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
package org.neo4j.ports.allocation;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;

public class SimplePortProviderTest
{
    @Test
    public void shouldProvideUniquePorts()
    {
        PortProvider portProvider = new SimplePortProvider( port -> false, 42 );

        int port1 = portProvider.getNextFreePort( "foo" );
        int port2 = portProvider.getNextFreePort( "foo" );

        assertThat( port1, is( not( equalTo( port2 ) ) ) );
    }

    @Test
    public void shouldSkipOccupiedPorts()
    {
        PortProbe portProbe = mock( PortProbe.class );
        PortProvider portProvider = new SimplePortProvider( portProbe, 40 );

        when( portProbe.isOccupied( 40 ) ).thenReturn( false );
        when( portProbe.isOccupied( 41 ) ).thenReturn( false );
        when( portProbe.isOccupied( 42 ) ).thenReturn( true );
        when( portProbe.isOccupied( 43 ) ).thenReturn( false );
        assertThat( portProvider.getNextFreePort( "foo" ), is( 40 ) );
        assertThat( portProvider.getNextFreePort( "foo" ), is( 41 ) );
        assertThat( portProvider.getNextFreePort( "foo" ), is( 43 ) );
    }

    @Test
    public void shouldNotOverRun()
    {
        PortProvider portProvider = new SimplePortProvider( port -> false, 65534 );

        portProvider.getNextFreePort( "foo" );
        portProvider.getNextFreePort( "foo" );

        try
        {
            portProvider.getNextFreePort( "foo" );

            fail();
        }
        catch ( IllegalStateException e )
        {
            assertThat( e.getMessage(), is( "There are no more ports available" ) );
        }
    }
}
