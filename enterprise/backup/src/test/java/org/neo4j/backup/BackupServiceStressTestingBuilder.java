/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.backup;

import java.io.File;
import java.io.IOException;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.core.KernelPanicEventGenerator;
import org.neo4j.kernel.impl.transaction.log.LogRotation;
import org.neo4j.kernel.impl.util.Condition;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.kernel.monitoring.Monitors;

import static java.lang.System.currentTimeMillis;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.graphdb.DynamicRelationshipType.withName;

public class BackupServiceStressTestingBuilder
{
    private Condition untilCondition;
    private File storeDir;
    private File workingDirectory;
    private String backupHostname = "localhost";
    private int backupPort = 8200;

    public static Condition untilTimeExpired( long duration, TimeUnit unit )
    {
        final long endTimeInMilliseconds = currentTimeMillis() + unit.toMillis( duration );
        return new Condition()
        {
            @Override
            public boolean evaluate()
            {
                return currentTimeMillis() <= endTimeInMilliseconds;
            }
        };
    }

    public BackupServiceStressTestingBuilder until( Condition untilCondition )
    {
        Objects.requireNonNull( untilCondition );
        this.untilCondition = untilCondition;
        return this;
    }

    public BackupServiceStressTestingBuilder withStore( File storeDir )
    {
        Objects.requireNonNull( storeDir );
        assert storeDir.exists() && storeDir.isDirectory();
        this.storeDir = storeDir;
        return this;
    }

    public BackupServiceStressTestingBuilder withWorkingDirectory( File workingDirectory )
    {
        Objects.requireNonNull( workingDirectory );
        assert workingDirectory.exists() && workingDirectory.isDirectory();
        this.workingDirectory = workingDirectory;
        return this;
    }

    public BackupServiceStressTestingBuilder withBackupAddress( String hostname, int port )
    {
        Objects.requireNonNull( hostname );
        this.backupHostname = hostname;
        this.backupPort = port;
        return this;
    }

    public Callable<Integer> build()
    {
        Objects.requireNonNull( untilCondition, "must specify a condition" );
        Objects.requireNonNull( storeDir, "must specify a directory containing the db to backup from" );
        Objects.requireNonNull( workingDirectory, "must specify a directory where to save backups/broken stores" );
        return new RunTest( untilCondition, storeDir, workingDirectory, backupHostname, backupPort );
    }

    private static class RunTest implements Callable<Integer>
    {
        private final FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();

        private final Condition until;
        private final File storeDir;
        private final String backupHostname;
        private final int backupPort;
        private final File backupDir;
        private final File brokenDir;

        private RunTest( Condition until, File storeDir, File workingDir, String backupHostname, int backupPort )
        {
            this.until = until;
            this.storeDir = storeDir;
            this.backupHostname = backupHostname;
            this.backupPort = backupPort;
            this.backupDir = new File( workingDir, "backup" );
            fileSystem.mkdir( backupDir );
            this.brokenDir = new File( workingDir, "broken_stores" );
            fileSystem.mkdir( brokenDir );
        }

        @Override
        public Integer call() throws Exception
        {
            final GraphDatabaseAPI db = (GraphDatabaseAPI) new GraphDatabaseFactory()
                    .newEmbeddedDatabaseBuilder( storeDir.getAbsolutePath() )
                    .setConfig( OnlineBackupSettings.online_backup_server, backupHostname + ":" + backupPort )
                    .setConfig( GraphDatabaseSettings.keep_logical_logs, "true" )
                    .newGraphDatabase();

            try
            {
                createIndex( db );
                createSomeData( db );
                rotateLog( db );

                final AtomicBoolean keepGoing = new AtomicBoolean( true );

                // when
                final OnlineBackupKernelExtension backup = new OnlineBackupKernelExtension(
                        new Config(),
                        db,
                        db.getDependencyResolver().resolveDependency( KernelPanicEventGenerator.class ),
                        new DevNullLoggingService(),
                        new Monitors() );
                try
                {
                    backup.init();
                    backup.start();
                }
                catch ( Throwable t )
                {
                    throw new RuntimeException( t );
                }

                ExecutorService executor = Executors.newFixedThreadPool( 2 );
                executor.execute( new Runnable()
                {
                    @Override
                    public void run()
                    {
                        while ( keepGoing.get() && until.evaluate() )
                        {
                            createSomeData( db );
                        }
                    }
                } );

                final AtomicInteger inconsistentDbs = new AtomicInteger( 0 );
                executor.execute( new Runnable()
                {
                    private final BackupService backupService = new BackupService(
                            fileSystem, StringLogger.DEV_NULL, new Monitors() );

                    @Override
                    public void run()
                    {
                        while ( keepGoing.get() && until.evaluate() )
                        {
                            cleanup( backupDir );
                            BackupService.BackupOutcome backupOutcome =
                                    backupService.doFullBackup( backupHostname, backupPort,
                                            backupDir.getAbsolutePath(), true, new Config(),
                                            BackupClient.BIG_READ_TIMEOUT,
                                            false );

                            if ( !backupOutcome.isConsistent() )
                            {
                                keepGoing.set( false );
                                int num = inconsistentDbs.incrementAndGet();
                                File dir = new File( brokenDir, "" + num );
                                fileSystem.mkdir( dir );
                                copyRecursively( backupDir, dir );
                            }
                        }
                    }

                    private void copyRecursively( File from, File to )
                    {
                        try
                        {
                            fileSystem.copyRecursively( from, to );
                        }
                        catch ( IOException e )
                        {
                            throw new RuntimeException( e );
                        }
                    }

                    private void cleanup( File dir )
                    {
                        try
                        {
                            fileSystem.deleteRecursively( dir );
                        }
                        catch ( IOException e )
                        {
                            throw new RuntimeException( e );
                        }
                    }

                } );

                while ( keepGoing.get() && until.evaluate() )
                {
                    Thread.sleep( 500 );
                }

                executor.shutdown();
                assertTrue( executor.awaitTermination( 30, TimeUnit.SECONDS ) );

                try
                {
                    backup.stop();
                    backup.shutdown();
                }
                catch ( Throwable t )
                {
                    throw new RuntimeException( t );
                }

                return inconsistentDbs.get();
            }
            finally
            {
                db.shutdown();
            }
        }


        private void createIndex( GraphDatabaseAPI db )
        {
            Random random = ThreadLocalRandom.current();
            try ( Transaction tx = db.beginTx() )
            {
                db.schema().indexFor( label( "" + random.nextInt( 3 ) ) ).on( "name" ).create();
                tx.success();
            }
        }

        private void createSomeData( GraphDatabaseAPI db )
        {
            Random random = ThreadLocalRandom.current();
            try ( Transaction tx = db.beginTx() )
            {
                Node start = db.createNode( label( "" + random.nextInt( 3 ) ) );
                start.setProperty( "name", "name " + random.nextInt() );
                Node end = db.createNode( label( "" + random.nextInt( 3 ) ) );
                end.setProperty( "name", "name " + random.nextInt() );
                Relationship rel = start.createRelationshipTo( end, withName( "" + random.nextInt( 5 ) ) );
                rel.setProperty( "something", "some " + random.nextInt() );
                tx.success();
            }
        }

        private void rotateLog( GraphDatabaseAPI db ) throws IOException
        {
            db.getDependencyResolver().resolveDependency( LogRotation.class ).rotateLogFile();
        }

    }
}
