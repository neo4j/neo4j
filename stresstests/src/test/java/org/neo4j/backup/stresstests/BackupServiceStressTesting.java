/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.backup.stresstests;

import org.junit.Test;

import java.io.File;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.causalclustering.stresstests.Config;
import org.neo4j.causalclustering.stresstests.Control;
import org.neo4j.diagnostics.utils.DumpUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.util.concurrent.Futures;

import static java.lang.Boolean.parseBoolean;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.lang.String.format;
import static java.lang.System.getProperty;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.fail;
import static org.neo4j.helper.DatabaseConfiguration.configureBackup;
import static org.neo4j.helper.DatabaseConfiguration.configureTxLogRotationAndPruning;
import static org.neo4j.helper.StressTestingHelper.ensureExistsAndEmpty;
import static org.neo4j.helper.StressTestingHelper.fromEnv;

/**
 * Notice the class name: this is _not_ going to be run as part of the main build.
 */
public class BackupServiceStressTesting
{
    private static final String DEFAULT_DURATION_IN_MINUTES = "30";
    private static final String DEFAULT_WORKING_DIR = new File( getProperty( "java.io.tmpdir" ) ).getPath();
    private static final String DEFAULT_HOSTNAME = "localhost";
    private static final String DEFAULT_PORT = "8200";
    private static final String DEFAULT_ENABLE_INDEXES = "false";
    private static final String DEFAULT_TX_PRUNE = "50 files";

    @Test
    public void shouldBehaveCorrectlyUnderStress() throws Exception
    {
        long durationInMinutes = parseLong( fromEnv( "BACKUP_SERVICE_STRESS_DURATION", DEFAULT_DURATION_IN_MINUTES ) );
        String directory = fromEnv( "BACKUP_SERVICE_STRESS_WORKING_DIRECTORY", DEFAULT_WORKING_DIR );
        String backupHostname = fromEnv( "BACKUP_SERVICE_STRESS_BACKUP_HOSTNAME", DEFAULT_HOSTNAME );
        int backupPort = parseInt( fromEnv( "BACKUP_SERVICE_STRESS_BACKUP_PORT", DEFAULT_PORT ) );
        String txPrune = fromEnv( "BACKUP_SERVICE_STRESS_TX_PRUNE", DEFAULT_TX_PRUNE );
        boolean enableIndexes =
                parseBoolean( fromEnv( "BACKUP_SERVICE_STRESS_ENABLE_INDEXES", DEFAULT_ENABLE_INDEXES ) );

        File store = new File( directory, "store" );
        File work = new File( directory, "work" );
        FileUtils.deleteRecursively( store );
        FileUtils.deleteRecursively( work );
        File storeDirectory = ensureExistsAndEmpty( store );
        Path workDirectory = ensureExistsAndEmpty( work ).toPath();

        final Map<String,String> config =
                configureBackup( configureTxLogRotationAndPruning( new HashMap<>(), txPrune ), backupHostname,
                        backupPort );
        GraphDatabaseBuilder graphDatabaseBuilder =
                new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDirectory.getAbsoluteFile() )
                        .setConfig( config );

        Control control = new Control( new Config() );

        AtomicReference<GraphDatabaseService> dbRef = new AtomicReference<>();
        ExecutorService service = Executors.newFixedThreadPool( 3 );
        try
        {
            dbRef.set( graphDatabaseBuilder.newGraphDatabase() );
            if ( enableIndexes )
            {
                TransactionalWorkload.setupIndexes( dbRef.get() );
            }
            Future<?> workload = service.submit( new TransactionalWorkload( control, dbRef::get ) );
            Future<?> backupWorker = service.submit( new BackupLoad( control, backupHostname, backupPort, workDirectory ) );
            Future<?> startStopWorker = service.submit( new StartStop( control, graphDatabaseBuilder::newGraphDatabase, dbRef ) );

            Futures.combine( workload, backupWorker, startStopWorker ).get(durationInMinutes + 5, MINUTES );

            service.shutdown();
            if ( !service.awaitTermination( 30, SECONDS ) )
            {
                printThreadDump();
                fail( "Didn't manage to shut down the workers correctly, dumped threads for forensic purposes" );
            }
        }
        catch ( TimeoutException t )
        {
            System.err.println( format( "Timeout waiting task completion. Dumping all threads." ) );
            printThreadDump();
            throw t;
        }
        finally
        {
            dbRef.get().shutdown();
            service.shutdown();
        }

        // let's cleanup disk space when everything went well
        FileUtils.deleteRecursively( storeDirectory );
        FileUtils.deletePathRecursively( workDirectory );
    }

    private static void printThreadDump()
    {
        System.err.println( DumpUtils.threadDump() );
    }
}
