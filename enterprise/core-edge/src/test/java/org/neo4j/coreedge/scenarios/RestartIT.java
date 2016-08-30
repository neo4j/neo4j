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

import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.coreedge.discovery.Cluster;
import org.neo4j.coreedge.discovery.CoreClusterMember;
import org.neo4j.coreedge.discovery.EdgeClusterMember;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.storageengine.api.lock.AcquireLockTimeoutException;
import org.neo4j.test.coreedge.ClusterRule;

import static org.junit.Assert.assertTrue;
import static org.neo4j.coreedge.discovery.Cluster.dataMatchesEventually;
import static org.neo4j.graphdb.Label.label;

public class RestartIT
{
    @Rule
    public final ClusterRule clusterRule = new ClusterRule( getClass() )
            .withNumberOfCoreMembers( 3 )
            .withNumberOfEdgeMembers( 0 );

    @Test
    public void restartFirstServer() throws Exception
    {
        // given
        Cluster cluster = clusterRule.startCluster();

        // when
        cluster.removeCoreMemberWithMemberId( 0 );
        cluster.addCoreMemberWithId( 0 );

        // then
        cluster.shutdown();
    }

    @Test
    public void restartSecondServer() throws Exception
    {
        // given
        Cluster cluster = clusterRule.startCluster();

        // when
        cluster.removeCoreMemberWithMemberId( 1 );
        cluster.addCoreMemberWithId( 1 );

        // then
        cluster.shutdown();
    }

    @Test
    public void restartWhileDoingTransactions() throws Exception
    {
        // given
        Cluster cluster = clusterRule.startCluster();

        // when
        final GraphDatabaseService coreDB = cluster.getCoreMemberById( 0 ).database();

        ExecutorService executor = Executors.newCachedThreadPool();

        final AtomicBoolean done = new AtomicBoolean( false );
        executor.execute( () -> {
            while ( !done.get() )
            {
                try ( Transaction tx = coreDB.beginTx() )
                {
                    Node node = coreDB.createNode( label( "boo" ) );
                    node.setProperty( "foobar", "baz_bat" );
                    tx.success();
                }
                catch ( AcquireLockTimeoutException e )
                {
                    // expected sometimes
                }
            }
        } );
        Thread.sleep( 500 );

        cluster.removeCoreMemberWithMemberId( 1 );
        cluster.addCoreMemberWithId( 1 );
        Thread.sleep( 500 );

        // then
        done.set( true );
        executor.shutdown();
    }

    @Test
    public void shouldHaveWritableClusterAfterCompleteRestart() throws Exception
    {
        // given
        Cluster cluster = clusterRule.startCluster();
        cluster.shutdown();

        // when
        cluster.start();

        CoreClusterMember last = cluster.coreTx( ( db, tx ) ->
        {
            Node node = db.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        } );

        // then
        dataMatchesEventually( last, cluster.coreMembers() );
        cluster.shutdown();
    }

    @Test
    public void edgeTest() throws Exception
    {
        // given
        Cluster cluster = clusterRule.withNumberOfCoreMembers( 2 ).withNumberOfEdgeMembers( 1 ).startCluster();

        // when
        final GraphDatabaseService coreDB = cluster.awaitLeader( 5000 ).database();

        try ( Transaction tx = coreDB.beginTx() )
        {
            Node node = coreDB.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        }

        cluster.addCoreMemberWithId( 2 ).start();
        cluster.shutdown();

        for ( CoreClusterMember core : cluster.coreMembers() )
        {
            ConsistencyCheckService.Result result = new ConsistencyCheckService().runFullConsistencyCheck(
                    core.storeDir(), Config.defaults(), ProgressMonitorFactory.NONE,
                    FormattedLogProvider.toOutputStream( System.out ), new DefaultFileSystemAbstraction(), false );
            assertTrue( "Inconsistent: " + core, result.isSuccessful() );
        }

        for ( EdgeClusterMember edge : cluster.edgeMembers() )
        {
            ConsistencyCheckService.Result result = new ConsistencyCheckService().runFullConsistencyCheck(
                    edge.storeDir(), Config.defaults(), ProgressMonitorFactory.NONE,
                    FormattedLogProvider.toOutputStream( System.out ), new DefaultFileSystemAbstraction(), false );
            assertTrue( "Inconsistent: " + edge, result.isSuccessful() );
        }
    }
}
