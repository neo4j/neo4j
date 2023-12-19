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
package org.neo4j.kernel.stresstests.transaction.checkpoint;

import org.junit.Test;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.stresstests.transaction.checkpoint.tracers.TimerTransactionTracer;
import org.neo4j.kernel.stresstests.transaction.checkpoint.workload.Workload;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.scheduler.JobSchedulerAdapter;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.unsafe.impl.batchimport.ParallelBatchImporter;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitors;

import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.lang.System.getProperty;
import static org.neo4j.helper.StressTestingHelper.ensureExistsAndEmpty;
import static org.neo4j.helper.StressTestingHelper.fromEnv;
import static org.neo4j.kernel.stresstests.transaction.checkpoint.mutation.RandomMutationFactory.defaultRandomMutation;
import static org.neo4j.unsafe.impl.batchimport.AdditionalInitialIds.EMPTY;
import static org.neo4j.unsafe.impl.batchimport.Configuration.DEFAULT;
import static org.neo4j.unsafe.impl.batchimport.ImportLogic.NO_MONITOR;

/**
 * Notice the class name: this is _not_ going to be run as part of the main build.
 */
public class CheckPointingLogRotationStressTesting
{
    private static final String DEFAULT_DURATION_IN_MINUTES = "5";
    private static final String DEFAULT_STORE_DIR = new File( getProperty( "java.io.tmpdir" ), "store" ).getPath();
    private static final String DEFAULT_NODE_COUNT = "100000";
    private static final String DEFAULT_WORKER_THREADS = "16";
    private static final String DEFAULT_PAGE_CACHE_MEMORY = "4g";

    private static final int CHECK_POINT_INTERVAL_MINUTES = 1;
    private JobScheduler jobScheduler = new JobSchedulerAdapter()
    {
        @Override
        public JobHandle schedule( Group group, Runnable job )
        {
            return new JobHandle()
            {
                @Override
                public void cancel( boolean mayInterruptIfRunning )
                {

                }

                @Override
                public void waitTermination()
                {

                }
            };
        }
    };

    @Test
    public void shouldBehaveCorrectlyUnderStress() throws Throwable
    {
        long durationInMinutes =
                parseLong( fromEnv( "CHECK_POINT_LOG_ROTATION_STRESS_DURATION", DEFAULT_DURATION_IN_MINUTES ) );
        File storeDir = new File( fromEnv( "CHECK_POINT_LOG_ROTATION_STORE_DIRECTORY", DEFAULT_STORE_DIR ) );
        long nodeCount = parseLong( fromEnv( "CHECK_POINT_LOG_ROTATION_NODE_COUNT", DEFAULT_NODE_COUNT ) );
        int threads = parseInt( fromEnv( "CHECK_POINT_LOG_ROTATION_WORKER_THREADS", DEFAULT_WORKER_THREADS ) );
        String pageCacheMemory = fromEnv( "CHECK_POINT_LOG_ROTATION_PAGE_CACHE_MEMORY", DEFAULT_PAGE_CACHE_MEMORY );

        System.out.println( "1/6\tBuilding initial store..." );
        try ( FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction() )
        {
            Config dbConfig = Config.defaults();
            new ParallelBatchImporter( ensureExistsAndEmpty( storeDir ), fileSystem, null, DEFAULT,
                    NullLogService.getInstance(), ExecutionMonitors.defaultVisible( jobScheduler ), EMPTY, dbConfig,
                    RecordFormatSelector.selectForConfig( dbConfig, NullLogProvider.getInstance() ), NO_MONITOR )
                    .doImport( new NodeCountInputs( nodeCount ) );
        }

        System.out.println( "2/6\tStarting database..." );
        GraphDatabaseBuilder builder = new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir );
        GraphDatabaseService db = builder
                .setConfig( GraphDatabaseSettings.pagecache_memory, pageCacheMemory )
                .setConfig( GraphDatabaseSettings.keep_logical_logs, Settings.FALSE )
                .setConfig( GraphDatabaseSettings.check_point_interval_time, CHECK_POINT_INTERVAL_MINUTES + "m" )
                .setConfig( GraphDatabaseFacadeFactory.Configuration.tracer, "timer" )
                .newGraphDatabase();

        System.out.println("3/6\tWarm up db...");
        try ( Workload workload = new Workload( db, defaultRandomMutation( nodeCount, db ), threads ) )
        {
            // make sure to run at least one checkpoint during warmup
            long warmUpTimeMillis = TimeUnit.SECONDS.toMillis( CHECK_POINT_INTERVAL_MINUTES * 2 );
            workload.run( warmUpTimeMillis, Workload.TransactionThroughput.NONE );
        }

        System.out.println( "4/6\tStarting workload..." );
        TransactionThroughputChecker throughput = new TransactionThroughputChecker();
        try ( Workload workload = new Workload( db, defaultRandomMutation( nodeCount, db ), threads ) )
        {
            workload.run( TimeUnit.MINUTES.toMillis( durationInMinutes ), throughput );
        }

        System.out.println( "5/6\tShutting down..." );
        db.shutdown();

        try
        {
            System.out.println( "6/6\tPrinting stats and recorded timings..." );
            TimerTransactionTracer.printStats( System.out );
            throughput.assertThroughput( System.out );
        }
        finally
        {
            System.out.println( "Done." );
        }

        // let's cleanup disk space when everything went well
        FileUtils.deleteRecursively( storeDir );
    }
}
