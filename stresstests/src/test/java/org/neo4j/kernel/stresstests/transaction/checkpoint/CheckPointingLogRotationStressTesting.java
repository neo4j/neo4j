/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.stresstests.transaction.checkpoint;

import org.junit.Test;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.stresstests.transaction.checkpoint.tracers.TimerTransactionTracer;
import org.neo4j.kernel.stresstests.transaction.checkpoint.workload.Workload;
import org.neo4j.unsafe.impl.batchimport.ParallelBatchImporter;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitors;

import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.lang.System.getProperty;
import static java.lang.System.getenv;
import static org.neo4j.kernel.stresstests.transaction.checkpoint.mutation.RandomMutationFactory.defaultRandomMutation;
import static org.neo4j.unsafe.impl.batchimport.Configuration.DEFAULT;

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
    private static final String DEFAULT_PAGE_SIZE = "8k";

    private static final int CHECK_POINT_INTERVAL_MINUTES = 1;

    @Test
    public void shouldBehaveCorrectlyUnderStress() throws Throwable
    {
        long durationInMinutes =
                parseLong( fromEnv( "CHECK_POINT_LOG_ROTATION_STRESS_DURATION", DEFAULT_DURATION_IN_MINUTES ) );
        File storeDir = ensureExists( fromEnv( "CHECK_POINT_LOG_ROTATION_STORE_DIRECTORY", DEFAULT_STORE_DIR ) );
        long nodeCount = parseLong( fromEnv( "CHECK_POINT_LOG_ROTATION_NODE_COUNT", DEFAULT_NODE_COUNT ) );
        int threads = parseInt( fromEnv( "CHECK_POINT_LOG_ROTATION_WORKER_THREADS", DEFAULT_WORKER_THREADS ) );
        String pageCacheMemory = fromEnv( "CHECK_POINT_LOG_ROTATION_PAGE_CACHE_MEMORY", DEFAULT_PAGE_CACHE_MEMORY );
        String pageSize = fromEnv( "CHECK_POINT_LOG_ROTATION_PAGE_SIZE", DEFAULT_PAGE_SIZE );

        if ( storeDir.exists() )
        {
            FileUtils.deleteRecursively( storeDir );
        }

        System.out.println( "1/6\tBuilding initial store..." );
        new ParallelBatchImporter( storeDir, DEFAULT, NullLogService.getInstance(), ExecutionMonitors.defaultVisible(),
                new Config() ).doImport( new NodeCountInputs( nodeCount ) );

        System.out.println( "2/6\tStarting database..." );
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir )
                .setConfig( GraphDatabaseSettings.pagecache_memory, pageCacheMemory )
                .setConfig( GraphDatabaseSettings.mapped_memory_page_size, pageSize )
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
    }

    private File ensureExists( String directory )
    {
        File dir = new File( directory );
        //noinspection ResultOfMethodCallIgnored
        dir.mkdirs();
        return dir;
    }

    private static String fromEnv( String environmentVariableName, String defaultValue )
    {
        String environmentVariableValue = getenv( environmentVariableName );
        return environmentVariableValue == null ? defaultValue : environmentVariableValue;
    }
}
