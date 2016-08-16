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
package org.neo4j.backup;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.spi.SimpleKernelContext;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.kernel.impl.util.DebugUtil;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.impl.util.DependenciesProxy;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.NullLogProvider;

import static java.lang.System.currentTimeMillis;
import static org.junit.Assert.fail;

public class BackupServiceStressTestingBuilder
{
    private BooleanSupplier untilCondition;
    private File storeDirectory;
    private File backupDirectory;
    private String backupHostname = "localhost";
    private int backupPort = 8200;

    public static BooleanSupplier untilTimeExpired( long duration, TimeUnit unit )
    {
        final long endTimeInMilliseconds = currentTimeMillis() + unit.toMillis( duration );
        return () -> currentTimeMillis() <= endTimeInMilliseconds;
    }

    public BackupServiceStressTestingBuilder until( BooleanSupplier untilCondition )
    {
        Objects.requireNonNull( untilCondition );
        this.untilCondition = untilCondition;
        return this;
    }

    public BackupServiceStressTestingBuilder withStore( File storeDirectory )
    {
        Objects.requireNonNull( storeDirectory );
        assertDirectoryExistsAndIsEmpty( storeDirectory );
        this.storeDirectory = storeDirectory;
        return this;
    }

    public BackupServiceStressTestingBuilder withBackupDirectory( File backupDirectory )
    {
        Objects.requireNonNull( backupDirectory );
        assertDirectoryExistsAndIsEmpty( backupDirectory );
        this.backupDirectory = backupDirectory;
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
        Objects.requireNonNull( storeDirectory, "must specify a directory containing the db to backup from" );
        Objects.requireNonNull( backupDirectory, "must specify a directory where to save backups/broken stores" );
        return new RunTest( untilCondition, storeDirectory, backupDirectory, backupHostname, backupPort );
    }

    private static void assertDirectoryExistsAndIsEmpty( File directory )
    {
        String path = directory.getAbsolutePath();

        if ( !directory.exists() )
        {
            throw new IllegalArgumentException( "Directory does not exist: '" + path + "'" );
        }
        if ( !directory.isDirectory() )
        {
            throw new IllegalArgumentException( "Given File is not a directory: '" + path + "'" );
        }
        if ( directory.list().length > 0 )
        {
            throw new IllegalArgumentException( "Given directory is not empty: '" + path + "' " +
                    Arrays.toString( directory.list() ) );
        }
    }

    private static class RunTest implements Callable<Integer>
    {
        private static final int NUMBER_OF_LABELS = 3;
        private static final int NUMBER_OF_RELATIONSHIP_TYPES = 5;

        private final FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();

        private final BooleanSupplier until;
        private final File storeDir;
        private final String backupHostname;
        private final int backupPort;
        private final File backupDir;
        private final File brokenDir;

        private final Label indexLabel;

        private RunTest( BooleanSupplier until, File storeDir, File backupDir, String backupHostname, int backupPort )
        {
            this.until = until;
            this.storeDir = storeDir;
            this.backupHostname = backupHostname;
            this.backupPort = backupPort;
            this.backupDir = new File( backupDir, "backup" );
            fileSystem.mkdir( this.backupDir );
            this.brokenDir = new File( backupDir, "broken_stores" );
            fileSystem.mkdir( brokenDir );
            indexLabel = randomLabel();
        }

        @Override
        public Integer call() throws Exception
        {
            final GraphDatabaseAPI db = (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(
                    storeDir.getAbsoluteFile() ).setConfig( OnlineBackupSettings.online_backup_server,
                    backupHostname + ":" + backupPort ).setConfig( GraphDatabaseSettings.keep_logical_logs, "true" )
                    .newGraphDatabase();

            try
            {
                createIndex( db );
                createSomeData( db );
                rotateLogAndCheckPoint( db );

                final AtomicBoolean keepGoing = new AtomicBoolean( true );

                // when
                Dependencies dependencies = new Dependencies( db.getDependencyResolver() );
                dependencies.satisfyDependencies( Config.empty(), NullLogProvider.getInstance(), new Monitors() );

                LifeSupport life = new LifeSupport();
                try
                {
                    SimpleKernelContext context = new SimpleKernelContext( fileSystem, storeDir, DatabaseInfo.UNKNOWN,
                            dependencies );
                    OnlineBackupExtensionFactory.Dependencies extensionDeps = DependenciesProxy.dependencies(
                            dependencies, OnlineBackupExtensionFactory.Dependencies.class );
                    life.add( new OnlineBackupExtensionFactory().newInstance( context, extensionDeps ) );
                }
                catch ( Throwable e )
                {
                    throw new RuntimeException( e );
                }
                life.start();

                ExecutorService executor = Executors.newFixedThreadPool( 2 );
                executor.execute( () -> {
                    while ( keepGoing.get() && until.getAsBoolean() )
                    {
                        createSomeData( db );
                    }
                } );

                final AtomicInteger inconsistentDbs = new AtomicInteger( 0 );
                executor.submit( new Callable<Void>()
                {
                    private final BackupService backupService = new BackupService( fileSystem,
                            NullLogProvider.getInstance(), new Monitors() );

                    @Override
                    public Void call() throws IOException
                    {
                        while ( keepGoing.get() && until.getAsBoolean() )
                        {
                            fileSystem.deleteRecursively( backupDir );
                            BackupService.BackupOutcome backupOutcome = backupService.doFullBackup( backupHostname,
                                    backupPort, backupDir.getAbsoluteFile(), ConsistencyCheck.FULL, Config.empty(),
                                    BackupClient.BIG_READ_TIMEOUT, false );

                            if ( !backupOutcome.isConsistent() )
                            {
                                keepGoing.set( false );
                                int num = inconsistentDbs.incrementAndGet();
                                File dir = new File( brokenDir, "" + num );
                                fileSystem.mkdir( dir );
                                fileSystem.copyRecursively( backupDir, dir );
                            }
                        }
                        return null;
                    }
                } );

                while ( keepGoing.get() && until.getAsBoolean() )
                {
                    Thread.sleep( 500 );
                }

                executor.shutdown();
                if ( !executor.awaitTermination( 5, TimeUnit.MINUTES ) )
                {
                    DebugUtil.dumpThreads( System.err );
                    fail( "Didn't manage to shut down the workers correctly, dumped threads for forensic purposes" );
                }

                life.shutdown();

                return inconsistentDbs.get();
            }
            finally
            {
                db.shutdown();
            }
        }

        private void createIndex( GraphDatabaseAPI db )
        {
            try ( Transaction tx = db.beginTx() )
            {
                db.schema().indexFor( indexLabel ).on( "name" ).create();
                tx.success();
            }
        }

        private void createSomeData( GraphDatabaseAPI db )
        {
            Random random = ThreadLocalRandom.current();
            try ( Transaction tx = db.beginTx() )
            {
                Node start = createLabeledNode( db, random );
                Node end = createLabeledNode( db, random );
                Relationship rel = start.createRelationshipTo( end, randomRelationshipType() );
                rel.setProperty( "something", "some " + random.nextInt() );
                tx.success();
            }
        }

        private Node createLabeledNode( GraphDatabaseService db, Random random )
        {
            Node node = db.createNode( randomLabel() );
            node.setProperty( "name", "name'" + random.nextInt() );
            return node;
        }

        private void rotateLogAndCheckPoint( GraphDatabaseAPI db ) throws IOException
        {
            db.getDependencyResolver().resolveDependency( LogRotation.class ).rotateLogFile();
            db.getDependencyResolver().resolveDependency( CheckPointer.class ).forceCheckPoint(
                    new SimpleTriggerInfo( "test" )
            );
        }

        private static Label randomLabel()
        {
            return Label.label( "" + ThreadLocalRandom.current().nextInt( NUMBER_OF_LABELS ) );
        }

        private static RelationshipType randomRelationshipType()
        {
            String name = "" + ThreadLocalRandom.current().nextInt( NUMBER_OF_RELATIONSHIP_TYPES );
            return RelationshipType.withName( name );
        }
    }
}
