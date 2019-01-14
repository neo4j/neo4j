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

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.ports.allocation.PortConstants.EphemeralPortMinimum;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashSet;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class PortRepositoryIT
{
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void shouldReservePorts() throws Exception
    {
        PortRepository portRepository1 = new PortRepository( temporaryDirectory(), EphemeralPortMinimum );

        int port1 = portRepository1.reserveNextPort( "foo" );
        int port2 = portRepository1.reserveNextPort( "foo" );
        int port3 = portRepository1.reserveNextPort( "foo" );

        assertThat( new HashSet<>( asList( port1, port2, port3) ).size(), is( 3 ) );
    }

    @Test
    public void shouldCoordinateUsingFileSystem() throws Exception
    {
        Path temporaryDirectory = temporaryDirectory();
        PortRepository portRepository1 = new PortRepository( temporaryDirectory, EphemeralPortMinimum );
        PortRepository portRepository2 = new PortRepository( temporaryDirectory, EphemeralPortMinimum );

        int port1 = portRepository1.reserveNextPort( "foo" );
        int port2 = portRepository1.reserveNextPort( "foo" );
        int port3 = portRepository1.reserveNextPort( "foo" );
        int port4 = portRepository2.reserveNextPort( "foo" );
        int port5 = portRepository2.reserveNextPort( "foo" );
        int port6 = portRepository1.reserveNextPort( "foo" );

        assertThat( new HashSet<>( asList( port1, port2, port3, port4, port5, port6 ) ).size(), is( 6 ) );
    }

    @Test
    @Ignore
    public void shouldNotInterfereWithOtherRepositories() throws Exception
    {
        PortRepository portRepository1 = new PortRepository( temporaryDirectory(), EphemeralPortMinimum );
        PortRepository portRepository2 = new PortRepository( temporaryDirectory(), EphemeralPortMinimum );

        int port1 = portRepository1.reserveNextPort( "foo" );
        int port2 = portRepository1.reserveNextPort( "foo" );
        int port3 = portRepository1.reserveNextPort( "foo" );
        int port4 = portRepository2.reserveNextPort( "foo" );
        int port5 = portRepository2.reserveNextPort( "foo" );
        int port6 = portRepository1.reserveNextPort( "foo" );

        assertThat( new HashSet<>( asList( port1, port2, port3, port4, port5, port6 ) ).size(), is( 4 ) );
    }

    @Test
    public void shouldNotOverrun() throws Exception
    {
        PortRepository portRepository1 = new PortRepository( temporaryDirectory(), 65534 );

        portRepository1.reserveNextPort( "foo" );
        portRepository1.reserveNextPort( "foo" );

        try
        {
            portRepository1.reserveNextPort( "foo" );

            fail();
        }
        catch ( IllegalStateException e )
        {
            assertThat( e.getMessage(), is( "There are no more ports available" ) );
        }
    }

    private Path temporaryDirectory() throws IOException
    {
        return temporaryFolder.newFolder("port-repository").toPath();
    }
}
