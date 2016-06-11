/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.neo4j.coreedge.discovery.Cluster;
import org.neo4j.coreedge.discovery.DiscoveryServiceFactory;
import org.neo4j.coreedge.discovery.SharedDiscoveryService;
import org.neo4j.coreedge.discovery.SharedDiscoveryService;
import org.neo4j.coreedge.raft.roles.Role;
import org.neo4j.coreedge.server.core.CoreGraphDatabase;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.rule.TargetDirectory;

import static org.junit.Assert.assertEquals;

public class ClusterFormationIT
{
    @Rule
    public final TargetDirectory.TestDirectory dir = TargetDirectory.testDirForTest( getClass() );

    private Cluster cluster;

    @After
    public void shutdown() throws ExecutionException, InterruptedException
    {
        if ( cluster != null )
        {
            cluster.shutdown();
        }
    }

    @Test
    public void shouldBeAbleToAddAndRemoveCoreServers() throws Exception
    {
        // given
        cluster = Cluster.start( dir.directory(), 3, 0, new SharedDiscoveryService() );

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
    public void shouldBeAbleToAddAndRemoveCoreServersUnderModestLoad() throws Exception
    {
        // given
        cluster = Cluster.start( dir.directory(), 3, 0, new SharedDiscoveryService() );

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit( (Runnable) () -> {
            CoreGraphDatabase leader = cluster.getDbWithRole( Role.LEADER );
            try ( Transaction tx = leader.beginTx() )
            {
                leader.createNode();
                tx.success();
            }
        } );

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

        executorService.shutdown();
    }

    @Test
    public void shouldBeAbleToRestartTheCluster() throws Exception
    {
        // when
        final DiscoveryServiceFactory discoveryServiceFactory = new SharedDiscoveryService();
        cluster = Cluster.start( dir.directory(), 3, 0, discoveryServiceFactory );

        // then
        assertEquals( 3, cluster.numberOfCoreServers() );

        // when
        cluster.shutdown();
        cluster = Cluster.start( dir.directory(), 3, 0, discoveryServiceFactory );

        // then
        assertEquals( 3, cluster.numberOfCoreServers() );

        // when
        cluster.removeCoreServerWithServerId( 1 );
        cluster.addCoreServerWithServerId( 3, 3 );
        cluster.shutdown();
        cluster = Cluster.start( dir.directory(), 3, 0, discoveryServiceFactory );

        assertEquals( 3, cluster.numberOfCoreServers() );
    }
}
