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
package org.neo4j.coreedge.stresstests;

import org.junit.Test;

import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import java.util.function.IntFunction;

import org.neo4j.backup.OnlineBackup;
import org.neo4j.backup.OnlineBackupSettings;
import org.neo4j.coreedge.discovery.Cluster;
import org.neo4j.coreedge.discovery.ClusterMember;
import org.neo4j.coreedge.discovery.HazelcastDiscoveryServiceFactory;
import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.SocketAddress;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_0;

import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.lang.System.getProperty;
import static org.neo4j.StressTestingHelper.ensureExistsAndEmpty;
import static org.neo4j.StressTestingHelper.fromEnv;
import static org.neo4j.function.Suppliers.untilTimeExpired;
import static org.neo4j.kernel.configuration.Settings.TRUE;

public class BackupStoreCopyInteractionStressTesting
{
    private static final String DEFAULT_NUMBER_OF_CORES = "3";
    private static final String DEFAULT_NUMBER_OF_EDGES = "1";
    private static final String DEFAULT_DURATION_IN_MINUTES = "30";
    private static final String DEFAULT_WORKING_DIR = new File( getProperty( "java.io.tmpdir" ) ).getPath();
    private static final String DEFAULT_BASE_CORE_BACKUP_PORT = "8000";
    private static final String DEFAULT_BASE_EDGE_BACKUP_PORT = "9000";

    @Test
    public void shouldBehaveCorrectlyUnderStress() throws Exception
    {
        int numberOfCores =
                parseInt( fromEnv( "BACKUP_STORE_COPY_INTERACTION_STRESS_NUMBER_OF_CORES", DEFAULT_NUMBER_OF_CORES ) );
        int numberOfEdges =
                parseInt( fromEnv( "BACKUP_STORE_COPY_INTERACTION_STRESS_NUMBER_OF_EDGES", DEFAULT_NUMBER_OF_EDGES ) );
        long durationInMinutes =
                parseLong( fromEnv( "BACKUP_STORE_COPY_INTERACTION_STRESS_DURATION", DEFAULT_DURATION_IN_MINUTES ) );
        String workingDirectory =
                fromEnv( "BACKUP_STORE_COPY_INTERACTION_STRESS_WORKING_DIRECTORY", DEFAULT_WORKING_DIR );
        int baseCoreBackupPort = parseInt( fromEnv( "BACKUP_STORE_COPY_INTERACTION_STRESS_BASE_CORE_BACKUP_PORT",
                DEFAULT_BASE_CORE_BACKUP_PORT ) );
        int baseEdgeBackupPort = parseInt( fromEnv( "BACKUP_STORE_COPY_INTERACTION_STRESS_BASE_EDGE_BACKUP_PORT",
                DEFAULT_BASE_EDGE_BACKUP_PORT ) );

        File clusterDirectory = ensureExistsAndEmpty( new File( workingDirectory, "cluster" ) );
        File backupDirectory = ensureExistsAndEmpty( new File( workingDirectory, "backups" ) );

        Map<String,String> params = Collections.emptyMap();
        Map<String,IntFunction<String>> paramsPerCoreInstance =
                configureBackup( baseCoreBackupPort, baseEdgeBackupPort, true );
        Map<String,IntFunction<String>> paramsPerEdgeInstance =
                configureBackup( baseCoreBackupPort, baseEdgeBackupPort, false );

        HazelcastDiscoveryServiceFactory discoveryServiceFactory = new HazelcastDiscoveryServiceFactory();
        Cluster cluster = new Cluster( clusterDirectory, numberOfCores, numberOfEdges, discoveryServiceFactory, params,
                paramsPerCoreInstance, params, paramsPerEdgeInstance, StandardV3_0.NAME );

        ExecutorService service = Executors.newFixedThreadPool( 3 );
        BooleanSupplier keepGoing = untilTimeExpired( durationInMinutes, TimeUnit.MINUTES );

        try
        {
            cluster.start();
            Future<?> workload = service.submit( workLoad( cluster, keepGoing ) );
            Future<?> startStopWorker = service.submit( startStopLoad( cluster, keepGoing ) );
            Future<?> backupWorker = service.submit(
                    backupLoad( backupDirectory, baseCoreBackupPort, baseEdgeBackupPort, cluster, keepGoing ) );

            workload.get();
            startStopWorker.get();
            backupWorker.get();
        }
        finally
        {
            cluster.shutdown();
        }

        // let's cleanup disk space when everything went well
        FileUtils.deleteRecursively( clusterDirectory );
        FileUtils.deleteRecursively( backupDirectory );
    }

    private Runnable workLoad( Cluster cluster, BooleanSupplier keepGoing )
    {
        return new RepeatUntilRunnable( keepGoing )
        {
            @Override
            protected void doWork()
            {
                try
                {
                    cluster.coreTx( ( db, tx ) ->
                    {
                        db.createNode();
                        tx.success();
                    } );
                }
                catch ( InterruptedException e )
                {
                    // whatever let's go on with the workload
                    Thread.interrupted();
                }
                catch ( TimeoutException | DatabaseShutdownException e )
                {
                    // whatever let's go on with the workload
                }
            }
        };
    }

    private Runnable startStopLoad( Cluster cluster, BooleanSupplier keepGoing )
    {
        return new RepeatUntilOnSelectedMemberRunnable( keepGoing, cluster )
        {
            @Override
            protected void doWorkOnMember( boolean isCore, int id )
            {
                ClusterMember member = pickSingleMember( cluster, id, isCore );
                member.shutdown();
                LockSupport.parkNanos( 500_000_000 );
                member.start();
            }
        };
    }

    private Runnable backupLoad( File baseDirectory, int baseCoreBackupPort, int baseEdgeBackupPort, Cluster cluster,
            BooleanSupplier keepGoing )
    {
        return new RepeatUntilOnSelectedMemberRunnable( keepGoing, cluster )
        {
            @Override
            protected void doWorkOnMember( boolean isCore, int id )
            {
                SocketAddress address = backupAddress( baseCoreBackupPort, baseEdgeBackupPort, isCore, id );
                File backupDirectory = new File( baseDirectory, Integer.toString( address.getPort() ) );
                OnlineBackup backup =
                        OnlineBackup.from( address.getHostname(), address.getPort() ).backup( backupDirectory );

                if ( !backup.isConsistent() )
                {
                    System.err.println( "Not consistent backup from " + address );
                }
            }
        };
    }

    private static abstract class RepeatUntilOnSelectedMemberRunnable extends RepeatUntilRunnable
    {
        private final Random random = new Random();
        private final Cluster cluster;

        RepeatUntilOnSelectedMemberRunnable( BooleanSupplier keepGoing, Cluster cluster )
        {
            super( keepGoing );
            this.cluster = cluster;
        }

        @Override
        protected final void doWork()
        {
            boolean isCore = random.nextBoolean();
            Collection<? extends ClusterMember> members = pickMembers( cluster, isCore );
            if ( members.isEmpty() )
            {
                return;
            }
            int id = random.nextInt( members.size() );
            doWorkOnMember( isCore, id );
        }

        protected abstract void doWorkOnMember( boolean isCore, int id );
    }

    private static abstract class RepeatUntilRunnable implements Runnable
    {
        private BooleanSupplier keepGoing;

        RepeatUntilRunnable( BooleanSupplier keepGoing )
        {
            this.keepGoing = keepGoing;
        }

        @Override
        public final void run()
        {
            while ( keepGoing.getAsBoolean() )
            {
                doWork();
            }
        }

        protected abstract void doWork();
    }

    private static Collection<? extends ClusterMember> pickMembers( Cluster cluster, boolean isCore )
    {
        return isCore ? cluster.coreMembers() : cluster.edgeMembers();
    }

    private static ClusterMember pickSingleMember( Cluster cluster, int id, boolean isCore )
    {
        return isCore ? cluster.getCoreMemberById( id ) : cluster.getEdgeMemberById( id );
    }

    private static Map<String,IntFunction<String>> configureBackup( int baseCoreBackupPort, int baseEdgeBackupPort,
            boolean isCore )
    {
        Map<String,IntFunction<String>> settings = new HashMap<>();
        settings.put( OnlineBackupSettings.online_backup_enabled.name(), id -> TRUE );
        settings.put( OnlineBackupSettings.online_backup_server.name(),
                id -> backupAddress( baseCoreBackupPort, baseEdgeBackupPort, isCore, id ).toString() );
        return settings;
    }

    private static SocketAddress backupAddress( int baseCoreBackupPort, int baseEdgeBackupPort, boolean isCore, int id )
    {
        return new AdvertisedSocketAddress( "localhost", (isCore ? baseCoreBackupPort : baseEdgeBackupPort) + id );
    }
}
