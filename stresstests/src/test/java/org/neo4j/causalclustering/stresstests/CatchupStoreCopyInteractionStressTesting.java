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
package org.neo4j.causalclustering.stresstests;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.HazelcastDiscoveryServiceFactory;
import org.neo4j.concurrent.Futures;
import org.neo4j.causalclustering.discovery.IpFamily;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_0;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static java.lang.Boolean.parseBoolean;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.lang.System.getProperty;
import static java.util.Collections.emptyMap;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.neo4j.causalclustering.stresstests.ClusterConfiguration.configureRaftLogRotationAndPruning;
import static org.neo4j.causalclustering.stresstests.ClusterConfiguration.enableRaftMessageLogging;
import static org.neo4j.function.Suppliers.untilTimeExpired;
import static org.neo4j.helper.DatabaseConfiguration.configureTxLogRotationAndPruning;
import static org.neo4j.helper.StressTestingHelper.ensureExistsAndEmpty;
import static org.neo4j.helper.StressTestingHelper.fromEnv;

public class CatchupStoreCopyInteractionStressTesting
{
    private static final String DEFAULT_NUMBER_OF_CORES = "3";
    private static final String DEFAULT_NUMBER_OF_EDGES = "1";
    private static final String DEFAULT_DURATION_IN_MINUTES = "30";
    private static final String DEFAULT_ENABLE_INDEXES = "false";
    private static final String DEFAULT_TX_PRUNE = "50 files";
    private static final String DEFAULT_WORKING_DIR = new File( getProperty( "java.io.tmpdir" ) ).getPath();

    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();
    private final PageCacheRule pageCacheRule = new PageCacheRule();

    @Rule
    public RuleChain rules = RuleChain.outerRule( fileSystemRule ).around( pageCacheRule );

    private FileSystemAbstraction fs;
    private PageCache pageCache;

    @Before
    public void setUp()
    {
        fs = fileSystemRule.get();
        pageCache = pageCacheRule.getPageCache( fs );
    }

    @Test
    public void shouldBehaveCorrectlyUnderStress() throws Exception
    {
        int numberOfCores =
                parseInt( fromEnv( "CATCHUP_STORE_COPY_INTERACTION_STRESS_NUMBER_OF_CORES", DEFAULT_NUMBER_OF_CORES ) );
        int numberOfEdges =
                parseInt( fromEnv( "CATCHUP_STORE_COPY_INTERACTION_STRESS_NUMBER_OF_EDGES", DEFAULT_NUMBER_OF_EDGES ) );
        long durationInMinutes =
                parseLong( fromEnv( "CATCHUP_STORE_COPY_INTERACTION_STRESS_DURATION", DEFAULT_DURATION_IN_MINUTES ) );
        String workingDirectory =
                fromEnv( "CATCHUP_STORE_COPY_INTERACTION_STRESS_WORKING_DIRECTORY", DEFAULT_WORKING_DIR );
        boolean enableIndexes = parseBoolean(
                fromEnv( "CATCHUP_STORE_COPY_INTERACTION_STRESS_ENABLE_INDEXES", DEFAULT_ENABLE_INDEXES ) );
        String txPrune = fromEnv( "CATCHUP_STORE_COPY_INTERACTION_STRESS_TX_PRUNE", DEFAULT_TX_PRUNE );

        File clusterDirectory = ensureExistsAndEmpty( new File( workingDirectory, "cluster" ) );

        Map<String,String> coreParams = enableRaftMessageLogging(
                configureRaftLogRotationAndPruning( configureTxLogRotationAndPruning( new HashMap<>(), txPrune ) ) );
        Map<String,String> edgeParams = configureTxLogRotationAndPruning( new HashMap<>(), txPrune );

        HazelcastDiscoveryServiceFactory discoveryServiceFactory = new HazelcastDiscoveryServiceFactory();
        Cluster cluster =
                new Cluster( clusterDirectory, numberOfCores, numberOfEdges, discoveryServiceFactory, coreParams,
                        emptyMap(), edgeParams, emptyMap(), Standard.LATEST_NAME, IpFamily.IPV4, false );

        AtomicBoolean stopTheWorld = new AtomicBoolean();
        BooleanSupplier notExpired = untilTimeExpired( durationInMinutes, MINUTES );
        BooleanSupplier keepGoing = () -> !stopTheWorld.get() && notExpired.getAsBoolean();
        Runnable onFailure = () -> stopTheWorld.set( true );

        ExecutorService service = Executors.newCachedThreadPool();
        try
        {
            cluster.start();
            if ( enableIndexes )
            {
                Workload.setupIndexes( cluster );
            }

            Future<?> workload = service.submit( new Workload( keepGoing, onFailure, cluster ) );
            Future<?> startStopWorker = service.submit(
                    new StartStopLoad( fs, pageCache, keepGoing, onFailure, cluster, numberOfCores, numberOfEdges ) );
            Future<?> catchUpWorker = service.submit( new CatchUpLoad( keepGoing, onFailure, cluster ) );

            Futures.combine( workload, startStopWorker, catchUpWorker ).get( durationInMinutes + 5, MINUTES );
        }
        finally
        {
            cluster.shutdown();
            service.shutdown();
        }

        // let's cleanup disk space when everything went well
        FileUtils.deleteRecursively( clusterDirectory );
    }
}
