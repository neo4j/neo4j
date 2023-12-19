/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.scenarios;

import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.causalclustering.discovery.ReadReplica;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.security.WriteOperationsNotAllowedException;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.lock.AcquireLockTimeoutException;
import org.neo4j.test.causalclustering.ClusterRule;

import static org.junit.Assert.assertTrue;
import static org.neo4j.causalclustering.discovery.Cluster.dataMatchesEventually;
import static org.neo4j.graphdb.Label.label;

public class RestartIT
{
    @Rule
    public final ClusterRule clusterRule = new ClusterRule()
            .withNumberOfCoreMembers( 3 )
            .withNumberOfReadReplicas( 0 );

    @Test
    public void restartFirstServer() throws Exception
    {
        // given
        Cluster cluster = clusterRule.startCluster();

        // when
        cluster.removeCoreMemberWithServerId( 0 );
        cluster.addCoreMemberWithId( 0 ).start();

        // then
        cluster.shutdown();
    }

    @Test
    public void restartSecondServer() throws Exception
    {
        // given
        Cluster cluster = clusterRule.startCluster();

        // when
        cluster.removeCoreMemberWithServerId( 1 );
        cluster.addCoreMemberWithId( 1 ).start();

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
        executor.execute( () ->
        {
            while ( !done.get() )
            {
                try ( Transaction tx = coreDB.beginTx() )
                {
                    Node node = coreDB.createNode( label( "boo" ) );
                    node.setProperty( "foobar", "baz_bat" );
                    tx.success();
                }
                catch ( AcquireLockTimeoutException | WriteOperationsNotAllowedException e )
                {
                    // expected sometimes
                }
            }
        } );
        Thread.sleep( 500 );

        cluster.removeCoreMemberWithServerId( 1 );
        cluster.addCoreMemberWithId( 1 ).start();
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
    public void readReplicaTest() throws Exception
    {
        // given
        Cluster cluster = clusterRule.withNumberOfCoreMembers( 2 ).withNumberOfReadReplicas( 1 ).startCluster();

        // when
        CoreClusterMember last = cluster.coreTx( ( db, tx ) ->
        {
            Node node = db.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        } );

        cluster.addCoreMemberWithId( 2 ).start();
        dataMatchesEventually( last, cluster.coreMembers() );
        dataMatchesEventually( last, cluster.readReplicas() );

        cluster.shutdown();

        try ( FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction() )
        {
            for ( CoreClusterMember core : cluster.coreMembers() )
            {
                ConsistencyCheckService.Result result = new ConsistencyCheckService()
                        .runFullConsistencyCheck( core.storeDir(), Config.defaults(), ProgressMonitorFactory.NONE,
                                NullLogProvider.getInstance(), fileSystem, false,
                                new ConsistencyFlags( true, true, true, false ) );
                assertTrue( "Inconsistent: " + core, result.isSuccessful() );
            }

            for ( ReadReplica readReplica : cluster.readReplicas() )
            {
                ConsistencyCheckService.Result result = new ConsistencyCheckService()
                        .runFullConsistencyCheck( readReplica.storeDir(), Config.defaults(), ProgressMonitorFactory.NONE,
                                NullLogProvider.getInstance(), fileSystem, false,
                                new ConsistencyFlags( true, true, true, false ) );
                assertTrue( "Inconsistent: " + readReplica, result.isSuccessful() );
            }
        }
    }
}
