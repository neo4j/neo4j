/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.coreedge.scenarios;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.coreedge.discovery.Cluster;
import org.neo4j.coreedge.discovery.EdgeServerConnectionException;
import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.test.TargetDirectory;

import static java.util.Arrays.asList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ClusterFormationIT
{
    @Rule
    public final TargetDirectory.TestDirectory dir = TargetDirectory.testDirForTest( getClass() );

    private Cluster cluster;

    @After
    public void shutdown()
    {
        if ( cluster != null )
        {
            cluster.shutdown();
        }
    }

    @Test
    public void shouldBeAbleToAddAndRemoveEdgeServers() throws Exception
    {
        // given
        cluster = Cluster.start( dir.directory(), 3, 3);

        // when
        cluster.removeEdgeServerWithServerId( 0 );
        cluster.addEdgeServerWithFileLocation( 0 );

        // then
        assertEquals( 3, cluster.numberOfEdgeServers() );

        // when
        cluster.removeEdgeServerWithServerId( 0 );
        cluster.addEdgeServerWithFileLocation( 3 );

        // then
        assertEquals( 3, cluster.numberOfEdgeServers() );
    }

    @Test
    public void shouldBeAbleToAddAndRemoveCoreServers() throws Exception
    {
        // given
        cluster = Cluster.start( dir.directory(), 3, 0);

        // when
        cluster.removeCoreServerWithServerId( 0 );
        cluster.addCoreServerWithServerId( 0, 3 );

        // then
        assertEquals( 3, cluster.numberOfCoreServers() );

        // when
        cluster.removeCoreServerWithServerId( 1 );

        // then
        assertEquals( 2, cluster.numberOfCoreServers() );

        // when
        cluster.addCoreServerWithServerId( 4, 3 );

        // then
        assertEquals( 3, cluster.numberOfCoreServers() );
    }

    @Test
    public void shouldBeAbleToRestartTheCluster() throws Exception
    {
        // when
        cluster = Cluster.start( dir.directory(), 3, 0);

        // then
        assertEquals( 3, cluster.numberOfCoreServers() );

        // when
        cluster.shutdown();
        cluster = Cluster.start( dir.directory(), 3, 0 );

        // then
        assertEquals( 3, cluster.numberOfCoreServers() );

        // when
        cluster.removeCoreServerWithServerId( 1 );
        cluster.addCoreServerWithServerId( 3, 3 );
        cluster.shutdown();
        cluster = Cluster.start( dir.directory(), 3, 0 );

        assertEquals( 3, cluster.numberOfCoreServers() );
    }

    @Test
    public void shouldThrowFriendlyExceptionIfEdgeServerCannotConnectToACoreCluster() throws Exception
    {
        // given
        cluster = Cluster.start( dir.directory(), 0, 0 ); // deliberately using Hazelcast for simplicity

        // when
        try
        {
            cluster.startEdgeServer( 99, asList( AdvertisedSocketAddress.address( "localhost:5001" ) ) );
        }
        catch ( RuntimeException e )
        {
            assertTrue( e.getCause().getCause() instanceof EdgeServerConnectionException );
        }
    }
}
