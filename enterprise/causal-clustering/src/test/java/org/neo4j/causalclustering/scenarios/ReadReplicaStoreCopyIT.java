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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.neo4j.causalclustering.catchup.tx.FileCopyMonitor;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.causalclustering.discovery.ReadReplica;
import org.neo4j.causalclustering.readreplica.ReadReplicaGraphDatabase;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.causalclustering.ClusterRule;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.configuration.Settings.FALSE;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class ReadReplicaStoreCopyIT
{
    @Rule
    public final ClusterRule clusterRule = new ClusterRule()
            .withSharedCoreParam( GraphDatabaseSettings.keep_logical_logs, FALSE )
            .withNumberOfCoreMembers( 3 )
            .withNumberOfReadReplicas( 1 );

    @Test( timeout = 240_000 )
    public void shouldNotBePossibleToStartTransactionsWhenReadReplicaCopiesStore() throws Throwable
    {
        Cluster cluster = clusterRule.startCluster();

        ReadReplica readReplica = cluster.findAnyReadReplica();

        readReplica.txPollingClient().stop();

        writeSomeDataAndForceLogRotations( cluster );
        Semaphore storeCopyBlockingSemaphore = addStoreCopyBlockingMonitor( readReplica );
        try
        {
            readReplica.txPollingClient().start();
            waitForStoreCopyToStartAndBlock( storeCopyBlockingSemaphore );

            ReadReplicaGraphDatabase replicaGraphDatabase = readReplica.database();
            try
            {
                replicaGraphDatabase.beginTx();
                fail( "Exception expected" );
            }
            catch ( Exception e )
            {
                assertThat( e, instanceOf( TransactionFailureException.class ) );
                assertThat( e.getMessage(), containsString( "Database is stopped to copy store" ) );
            }
        }
        finally
        {
            // release all waiters of the semaphore
            storeCopyBlockingSemaphore.release( Integer.MAX_VALUE );
        }
    }

    private static void writeSomeDataAndForceLogRotations( Cluster cluster ) throws Exception
    {
        for ( int i = 0; i < 20; i++ )
        {
            cluster.coreTx( ( db, tx ) ->
            {
                db.execute( "CREATE ()" );
                tx.success();
            } );

            forceLogRotationOnAllCores( cluster );
        }
    }

    private static void forceLogRotationOnAllCores( Cluster cluster )
    {
        for ( CoreClusterMember core : cluster.coreMembers() )
        {
            forceLogRotationAndPruning( core );
        }
    }

    private static void forceLogRotationAndPruning( CoreClusterMember core )
    {
        try
        {
            DependencyResolver dependencyResolver = core.database().getDependencyResolver();
            dependencyResolver.resolveDependency( LogRotation.class ).rotateLogFile();
            SimpleTriggerInfo info = new SimpleTriggerInfo( "test" );
            dependencyResolver.resolveDependency( CheckPointer.class ).forceCheckPoint( info );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private static Semaphore addStoreCopyBlockingMonitor( ReadReplica readReplica )
    {
        DependencyResolver dependencyResolver = readReplica.database().getDependencyResolver();
        Monitors monitors = dependencyResolver.resolveDependency( Monitors.class );

        Semaphore semaphore = new Semaphore( 0 );

        monitors.addMonitorListener( (FileCopyMonitor) file ->
        {
            try
            {
                semaphore.acquire();
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
                throw new RuntimeException( e );
            }
        } );

        return semaphore;
    }

    private static void waitForStoreCopyToStartAndBlock( Semaphore storeCopyBlockingSemaphore ) throws Exception
    {
        assertEventually( "Read replica did not copy files", storeCopyBlockingSemaphore::hasQueuedThreads,
                is( true ), 60, TimeUnit.SECONDS );
    }
}
