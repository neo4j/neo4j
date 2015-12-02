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

import java.net.InetSocketAddress;
import java.util.Map;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.coreedge.discovery.Cluster;
import org.neo4j.coreedge.discovery.EdgeServerConnectionException;
import org.neo4j.coreedge.discovery.HazelcastClientLifecycle;
import org.neo4j.coreedge.server.CoreEdgeClusterSettings;
import org.neo4j.coreedge.server.ListenSocketAddress;
import org.neo4j.coreedge.server.core.CoreGraphDatabase;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertEquals;

import static org.neo4j.coreedge.discovery.Cluster.start;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class HazelcastClientLifeCycleIT
{
    public final
    @Rule
    TargetDirectory.TestDirectory dir = TargetDirectory.testDirForTest( getClass() );

    @Rule
    public ExpectedException exceptionMatcher = ExpectedException.none();

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
    public void shouldConnectToCoreClusterAsLongAsOneInitialHostIsAvailable() throws Throwable
    {
        // given
        cluster = start( dir.directory(), 2, 0 );

        // when

        ListenSocketAddress goodHostnamePort = hostnamePort( cluster.coreServers().iterator().next() );
        InetSocketAddress address = goodHostnamePort.socketAddress();
        String badHost = "localhost:9999";
        String goodHost = address.getHostString() + ":" + address.getPort();

        HazelcastClientLifecycle client = new HazelcastClientLifecycle( getConfig( badHost + "," + goodHost )
        );

        client.start();

        // then
        assertEquals( 2, client.currentTopology().getNumberOfCoreServers() );

        client.stop();
    }

    private ListenSocketAddress hostnamePort( CoreGraphDatabase aCoreServer )
    {
        return aCoreServer.getDependencyResolver()
                .resolveDependency( Config.class ).get( CoreEdgeClusterSettings.cluster_listen_address );
    }

    @Test
    public void shouldThrowAnExceptionIfUnableToConnectToCoreCluster() throws Throwable
    {
        // when
        String badHost = "localhost:9999";
        HazelcastClientLifecycle client = new HazelcastClientLifecycle( getConfig( badHost ) );

        // then
        exceptionMatcher.expect( EdgeServerConnectionException.class );

        client.start();
        client.stop();
    }


    private Config getConfig( String initialHosts )
    {
        Map<String, String> params = stringMap();
        params.put( "org.neo4j.server.database.mode", "CORE_EDGE" );
        params.put( ClusterSettings.cluster_name.name(), Cluster.CLUSTER_NAME );
        params.put( ClusterSettings.server_id.name(), String.valueOf( 99 ) );
        params.put( CoreEdgeClusterSettings.initial_core_cluster_members.name(), initialHosts );
        return new Config( params );
    }
}
