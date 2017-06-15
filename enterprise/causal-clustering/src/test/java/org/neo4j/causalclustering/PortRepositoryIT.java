/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.causalclustering.PortConstants.EphemeralPortMinimum;
import static org.neo4j.helpers.collection.Iterators.asSet;

import java.nio.file.Path;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.test.rule.TestDirectory;

public class PortRepositoryIT
{
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();

    @Test
    public void shouldReservePorts() throws Exception
    {
        Path directory = testDirectory.cleanDirectory( "port-repository" ).toPath();
        PortRepository portRepository1 = new PortRepository( directory, EphemeralPortMinimum );

        int port1 = portRepository1.reserveNextPort( "foo" );
        int port2 = portRepository1.reserveNextPort( "foo" );
        int port3 = portRepository1.reserveNextPort( "foo" );

        assertThat( asSet( port1, port2, port3 ).size(), is( 3 ) );
    }

    @Test
    public void shouldCoordinateUsingFileSystem() throws Exception
    {
        Path directory = testDirectory.cleanDirectory( "port-repository" ).toPath();
        PortRepository portRepository1 = new PortRepository( directory, EphemeralPortMinimum );
        PortRepository portRepository2 = new PortRepository( directory, EphemeralPortMinimum );

        int port1 = portRepository1.reserveNextPort( "foo" );
        int port2 = portRepository1.reserveNextPort( "foo" );
        int port3 = portRepository1.reserveNextPort( "foo" );
        int port4 = portRepository2.reserveNextPort( "foo" );
        int port5 = portRepository2.reserveNextPort( "foo" );
        int port6 = portRepository1.reserveNextPort( "foo" );

        assertThat( asSet( port1, port2, port3, port4, port5, port6 ).size(), is( 6 ) );
    }

    @Test
    @Ignore
    public void shouldNotInterfereWithOtherRepositories() throws Exception
    {
        Path directory1 = testDirectory.cleanDirectory( "port-repository-1" ).toPath();
        Path directory2 = testDirectory.cleanDirectory( "port-repository-2" ).toPath();

        PortRepository portRepository1 = new PortRepository( directory1, EphemeralPortMinimum );
        PortRepository portRepository2 = new PortRepository( directory2, EphemeralPortMinimum );

        int port1 = portRepository1.reserveNextPort( "foo" );
        int port2 = portRepository1.reserveNextPort( "foo" );
        int port3 = portRepository1.reserveNextPort( "foo" );
        int port4 = portRepository2.reserveNextPort( "foo" );
        int port5 = portRepository2.reserveNextPort( "foo" );
        int port6 = portRepository1.reserveNextPort( "foo" );

        assertThat( asSet( port1, port2, port3, port4, port5, port6 ).size(), is( 4 ) );
    }

    @Test
    public void shouldNotOverrun() throws Exception
    {
        Path directory = testDirectory.cleanDirectory( "port-repository" ).toPath();
        PortRepository portRepository1 = new PortRepository( directory, 65534 );

        portRepository1.reserveNextPort( "foo" );
        portRepository1.reserveNextPort( "foo" );

        try
        {
            portRepository1.reserveNextPort( "foo" );

            fail();
        }
        catch ( IllegalStateException e )
        {
            assertThat( e.getMessage(), is( "There are no more ephemeral/ dynamic ports available" ) );
        }
    }
}
