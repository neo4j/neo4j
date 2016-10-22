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
package org.neo4j.causalclustering.stresstests;

import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.IntFunction;

import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.HazelcastDiscoveryServiceFactory;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.SocketAddress;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_0;

import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.lang.System.getProperty;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertNull;
import static org.neo4j.StressTestingHelper.ensureExistsAndEmpty;
import static org.neo4j.StressTestingHelper.fromEnv;
import static org.neo4j.StressTestingHelper.prettyPrintStackTrace;
import static org.neo4j.causalclustering.stresstests.ClusterConfiguration.configureBackup;
import static org.neo4j.causalclustering.stresstests.ClusterConfiguration.configureRaftLogRotationAndPruning;
import static org.neo4j.causalclustering.stresstests.ClusterConfiguration.configureTxLogRotationAndPruning;
import static org.neo4j.causalclustering.stresstests.ClusterConfiguration.enableRaftMessageLogging;
import static org.neo4j.function.Suppliers.untilTimeExpired;

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

        BiFunction<Boolean,Integer,SocketAddress> backupAddress = ( isCore, id ) ->
                new AdvertisedSocketAddress( "localhost", (isCore ? baseCoreBackupPort : baseEdgeBackupPort) + id );

        Map<String,String> coreParams = enableRaftMessageLogging(
                configureRaftLogRotationAndPruning( configureTxLogRotationAndPruning( new HashMap<>() ) ) );
        Map<String,String> readReplicaParams = configureTxLogRotationAndPruning( new HashMap<>() );

        Map<String,IntFunction<String>> instanceCoreParams =
                configureBackup( new HashMap<>(), id -> backupAddress.apply( true, id ) );
        Map<String,IntFunction<String>> instanceReadReplicaParams =
                configureBackup( new HashMap<>(), id -> backupAddress.apply( false, id ) );

        HazelcastDiscoveryServiceFactory discoveryServiceFactory = new HazelcastDiscoveryServiceFactory();
        Cluster cluster =
                new Cluster( clusterDirectory, numberOfCores, numberOfEdges, discoveryServiceFactory, coreParams,
                        instanceCoreParams, readReplicaParams, instanceReadReplicaParams, StandardV3_0.NAME );

        AtomicBoolean stopTheWorld = new AtomicBoolean();
        BooleanSupplier notExpired = untilTimeExpired( durationInMinutes, MINUTES );
        BooleanSupplier keepGoing = () ->!stopTheWorld.get() && notExpired.getAsBoolean();
        Runnable onFailure = () -> stopTheWorld.set( true );

        ExecutorService service = Executors.newFixedThreadPool( 3 );
        try
        {
            cluster.start();
            Future<Throwable> workload = service.submit( new Workload( keepGoing, onFailure, cluster ) );
            Future<Throwable> startStopWorker =
                    service.submit( new StartStopLoad( keepGoing, onFailure, cluster, numberOfCores, numberOfEdges ) );
            Future<Throwable> backupWorker = service.submit(
                    new BackupLoad( keepGoing, onFailure, cluster, numberOfCores, numberOfEdges, backupDirectory,
                            backupAddress ) );

            long timeout = durationInMinutes + 5;
            assertNull( prettyPrintStackTrace( workload.get() ), workload.get( timeout, MINUTES ) );
            assertNull( prettyPrintStackTrace( startStopWorker.get() ), startStopWorker.get( timeout, MINUTES  ) );
            assertNull( prettyPrintStackTrace( backupWorker.get() ), backupWorker.get( timeout, MINUTES ) );
        }
        finally
        {
            cluster.shutdown();
        }

        // let's cleanup disk space when everything went well
        FileUtils.deleteRecursively( clusterDirectory );
        FileUtils.deleteRecursively( backupDirectory );
    }
}
