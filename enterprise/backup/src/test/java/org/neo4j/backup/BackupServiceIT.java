/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.Index;
import org.neo4j.helpers.Pair;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.core.KernelPanicEventGenerator;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.TransactionIdStore;
import org.neo4j.kernel.impl.nioneo.xa.DataSourceManager;
import org.neo4j.kernel.impl.storemigration.StoreFile;
import org.neo4j.kernel.impl.transaction.xaframework.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.xaframework.NoSuchTransactionException;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionMetadataCache.TransactionMetadata;
import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.kernel.monitoring.BackupMonitor;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.Mute;
import org.neo4j.test.TargetDirectory;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import static org.neo4j.test.DoubleLatch.awaitLatch;

public class BackupServiceIT
{
    private static final class StoreSnoopingMonitor extends BackupMonitor.Adapter
    {
        private final CountDownLatch firstStoreFinishedStreaming;
        private final CountDownLatch transactionCommitted;
        private final List<String> storesThatHaveBeenStreamed;

        private StoreSnoopingMonitor( CountDownLatch firstStoreFinishedStreaming, CountDownLatch transactionCommitted,
                List<String> storesThatHaveBeenStreamed )
        {
            this.firstStoreFinishedStreaming = firstStoreFinishedStreaming;
            this.transactionCommitted = transactionCommitted;
            this.storesThatHaveBeenStreamed = storesThatHaveBeenStreamed;
        }

        @Override
        public void streamedFile( File storefile )
        {
            if ( neitherStoreHasBeenStreamed() )
            {
                if ( storefile.getAbsolutePath().contains( NODE_STORE ) )
                {
                    storesThatHaveBeenStreamed.add( NODE_STORE );
                    firstStoreFinishedStreaming.countDown();
                }
                else if ( storefile.getAbsolutePath().contains( RELATIONSHIP_STORE ) )
                {
                    storesThatHaveBeenStreamed.add(RELATIONSHIP_STORE);
                    firstStoreFinishedStreaming.countDown();
                }
            }
        }

        private boolean neitherStoreHasBeenStreamed()
        {
            return storesThatHaveBeenStreamed.isEmpty();
        }

        @Override
        public void streamingFile( File storefile )
        {
            if ( storefile.getAbsolutePath().contains( RELATIONSHIP_STORE ) )
            {
                if ( streamedFirst( NODE_STORE ) )
                {
                    awaitLatch( transactionCommitted );
                }
            }
            else if ( storefile.getAbsolutePath().contains( NODE_STORE ) )
            {
                if ( streamedFirst( RELATIONSHIP_STORE ) )
                {
                    awaitLatch( transactionCommitted );
                }
            }
        }

        private boolean streamedFirst( String store )
        {
            return !storesThatHaveBeenStreamed.isEmpty() && storesThatHaveBeenStreamed.get( 0 ).equals( store );
        }
    }

    private static final TargetDirectory target = TargetDirectory.forTest( BackupServiceIT.class );
    private static final String NODE_STORE = "neostore.nodestore.db";
    private static final String RELATIONSHIP_STORE = "neostore.relationshipstore.db";
    private static final String BACKUP_HOST = "localhost";

    @Rule
    public TargetDirectory.TestDirectory testDirectory = target.testDirectory();

    @Rule
    public Mute mute = Mute.muteAll();

    private FileSystemAbstraction fileSystem;
    private File storeDir;
    private File backupDir;
    public int backupPort = 8200;

    @Before
    public void setup() throws IOException
    {
        fileSystem = new DefaultFileSystemAbstraction();

        storeDir = new File( testDirectory.directory(), "store_dir" );
        fileSystem.deleteRecursively( storeDir );
        fileSystem.mkdir( storeDir );

        backupDir = new File( testDirectory.directory(), "backup_dir" );
        fileSystem.deleteRecursively( backupDir );

        backupPort = backupPort + 1;
    }

    @Test
    public void shouldThrowExceptionWhenDoingFullBackupOnADirectoryContainingANeoStore() throws Exception
    {
        // given
        fileSystem.mkdir( backupDir );
        fileSystem.create( new File( backupDir, NeoStore.DEFAULT_NAME ) ).close();

        try
        {
            // when
            new BackupService( fileSystem ).doFullBackup( "", 0, backupDir.getAbsolutePath(), true, new Config() );
        }
        catch ( RuntimeException ex )
        {
            // then
            assertThat( ex.getMessage(), containsString( "already contains a database" ) );
        }
    }

    @Test
    public void shouldCopyStoreFiles() throws Throwable
    {
        // given
        GraphDatabaseService db = createDb( storeDir, defaultBackupPortHostParams() );
        createAndIndexNode( db, 1 );

        // when
        BackupService backupService = new BackupService( fileSystem );
        backupService.doFullBackup( BACKUP_HOST, backupPort, backupDir.getAbsolutePath(), false,
                new Config( defaultBackupPortHostParams() ) );
        db.shutdown();

        // then
        File[] files = fileSystem.listFiles( backupDir );

        for ( final StoreFile storeFile : StoreFile.values() )
        {
            assertThat( files, hasFile( storeFile.storeFileName() ) );
        }

        assertEquals( DbRepresentation.of( storeDir ), DbRepresentation.of( backupDir ) );
    }

    @Test
    public void shouldFindTransactionLogContainingLastNeoStoreAndLuceneTransactionInAnEmptyStore() throws IOException
    {
        // given
        GraphDatabaseService db = createDb( storeDir, defaultBackupPortHostParams() );

        // when
        BackupService backupService = new BackupService( fileSystem );
        backupService.doFullBackup( BACKUP_HOST, backupPort, backupDir.getAbsolutePath(), false,
                new Config( defaultBackupPortHostParams() ) );
        db.shutdown();

        // then
        assertEquals( DbRepresentation.of( storeDir ), DbRepresentation.of( backupDir ) );

        assertNotNull( getLastMasterForCommittedTx() );
    }

    @Test
    public void shouldFindTransactionLogContainingLastNeoStoreTransaction() throws Throwable
    {
        // given
        GraphDatabaseService db = createDb( storeDir, defaultBackupPortHostParams() );
        createAndIndexNode( db, 1 );

        // when
        BackupService backupService = new BackupService( fileSystem );
        backupService.doFullBackup( BACKUP_HOST, backupPort, backupDir.getAbsolutePath(), false,
                new Config( defaultBackupPortHostParams() ) );
        db.shutdown();

        // then
        assertEquals( DbRepresentation.of( storeDir ), DbRepresentation.of( backupDir ) );
        assertNotNull( getLastMasterForCommittedTx() );
    }

    @Test
    public void shouldFindTransactionLogContainingLastLuceneTransaction() throws Throwable
    {
        // given
        GraphDatabaseService db = createDb( storeDir, defaultBackupPortHostParams() );
        createAndIndexNode( db, 1 );

        // when
        BackupService backupService = new BackupService( fileSystem );
        backupService.doFullBackup( BACKUP_HOST, backupPort, backupDir.getAbsolutePath(), false,
                new Config( defaultBackupPortHostParams() ) );
        db.shutdown();

        // then
        assertEquals( DbRepresentation.of( storeDir ), DbRepresentation.of( backupDir ) );
        assertNotNull( getLastMasterForCommittedTx() );
    }

    @Test
    public void shouldGiveHelpfulErrorMessageIfLogsPrunedPastThePointOfNoReturn() throws Exception
    {
        // Given
        Map<String, String> config = defaultBackupPortHostParams();
        config.put( GraphDatabaseSettings.keep_logical_logs.name(), "false" );
        // have logs rotated on every transaction
        config.put( GraphDatabaseSettings.logical_log_rotation_threshold.name(), "20" );
        GraphDatabaseAPI db = createDb( storeDir, config );
        BackupService backupService = new BackupService( fileSystem );

        createAndIndexNode( db, 1 );

        // A full backup
        backupService.doFullBackup( BACKUP_HOST, backupPort, backupDir.getAbsolutePath(), false,
                new Config( defaultBackupPortHostParams() ) );

        // And the log the backup uses is rotated out
        createAndIndexNode( db, 2 );
        createAndIndexNode( db, 3 );
        createAndIndexNode( db, 3 );

        // when
        try
        {
            backupService.doIncrementalBackup( BACKUP_HOST, backupPort, backupDir.getAbsolutePath(), false );
            fail("Should have thrown exception.");
        }

        // Then
        catch(IncrementalBackupNotPossibleException e)
        {
            assertThat( e.getMessage(), equalTo("It's been too long since this backup was last updated, and it has " +
                    "fallen too far behind the database transaction stream for incremental backup to be possible. " +
                    "You need to perform a full backup at this point. You can modify this time interval by setting " +
                    "the '" + GraphDatabaseSettings.keep_logical_logs.name() + "' configuration on the database to a " +
                    "higher value.") );
        }
        db.shutdown();
    }

    @Test
    public void shouldFallbackToFullBackupIfIncrementalFailsAndExplicitlyAskedToDoThis() throws Exception
    {
        // Given
        Map<String, String> config = defaultBackupPortHostParams();
        config.put( GraphDatabaseSettings.keep_logical_logs.name(), "false" );
        // have logs rotated on every transaction
        config.put( GraphDatabaseSettings.logical_log_rotation_threshold.name(), "20" );
        GraphDatabaseAPI db = createDb( storeDir, config );
        BackupService backupService = new BackupService( fileSystem );

        createAndIndexNode( db, 1 );

        // A full backup
        backupService.doFullBackup( BACKUP_HOST, backupPort, backupDir.getAbsolutePath(), false,
                new Config( defaultBackupPortHostParams() ) );

        // And the log the backup uses is rotated out
        createAndIndexNode( db, 2 );
        createAndIndexNode( db, 3 );
        createAndIndexNode( db, 3 );

        // when
        backupService.doIncrementalBackupOrFallbackToFull( BACKUP_HOST, backupPort, backupDir.getAbsolutePath(), false, new Config(defaultBackupPortHostParams()));

        // Then
        db.shutdown();
        assertEquals( DbRepresentation.of( storeDir ), DbRepresentation.of( backupDir ) );
    }

    @Test
    public void shouldHandleBackupWhenLogFilesHaveBeenDeleted() throws Exception
    {
        // Given
        Map<String, String> config = defaultBackupPortHostParams();
        config.put( GraphDatabaseSettings.keep_logical_logs.name(), "false" );
        GraphDatabaseAPI db = createDb( storeDir, config );
        BackupService backupService = new BackupService( fileSystem );

        createAndIndexNode( db, 1 );

        // A full backup
        backupService.doIncrementalBackupOrFallbackToFull( BACKUP_HOST, backupPort, backupDir.getAbsolutePath(), false,
                new Config( defaultBackupPortHostParams() ) );

        // And the log the backup uses is rotated out
        createAndIndexNode( db, 2 );
        db = deleteLogFilesAndRestart( config, db );

        createAndIndexNode( db, 3 );
        db = deleteLogFilesAndRestart( config, db );

        // when
        backupService.doIncrementalBackupOrFallbackToFull( BACKUP_HOST, backupPort, backupDir.getAbsolutePath(),
                false, new Config( defaultBackupPortHostParams() ) );

        // Then
        db.shutdown();
        assertEquals( DbRepresentation.of( storeDir ), DbRepresentation.of( backupDir ) );
    }

    private GraphDatabaseAPI deleteLogFilesAndRestart( Map<String, String> config, GraphDatabaseAPI db )
    {
        db.shutdown();
        for ( File logFile : storeDir.listFiles( new FileFilter()
        {
            @Override
            public boolean accept( File pathname )
            {
                return pathname.getName().contains( "logical" );
            }
        } ) )
        {
            logFile.delete();
        }
        db = createDb( storeDir, config );
        return db;
    }

    @Test
    public void shouldDoFullBackupOnIncrementalFallbackToFullIfNoBackupFolderExists() throws Exception
    {
        // Given
        Map<String, String> config = defaultBackupPortHostParams();
        config.put( GraphDatabaseSettings.keep_logical_logs.name(), "false" );
        GraphDatabaseAPI db = createDb( storeDir, config );
        BackupService backupService = new BackupService( fileSystem );

        createAndIndexNode( db, 1 );

        // when
        backupService.doIncrementalBackupOrFallbackToFull( BACKUP_HOST, backupPort, backupDir.getAbsolutePath(), false,
                new Config(defaultBackupPortHostParams()));

        // then
        db.shutdown();
        assertEquals( DbRepresentation.of( storeDir ), DbRepresentation.of( backupDir ) );
    }

//    private void rotateLog( GraphDatabaseAPI db ) throws IOException
//    {
//        db.getDependencyResolver().resolveDependency( XaDataSourceManager.class ).getNeoStoreDataSource().rotateLogicalLog();
//    }

    @Test
    public void shouldContainTransactionsThatHappenDuringBackupProcess() throws Throwable
    {
        // given
        Map<String, String> params = defaultBackupPortHostParams();
        params.put( OnlineBackupSettings.online_backup_enabled.name(), "false" );

        final List<String> storesThatHaveBeenStreamed = new ArrayList<>();
        final CountDownLatch firstStoreFinishedStreaming = new CountDownLatch( 1 );
        final CountDownLatch transactionCommitted = new CountDownLatch( 1 );

        final GraphDatabaseAPI db = (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(
                storeDir.getAbsolutePath() ).setConfig( params ).newGraphDatabase();

        try
        {
            Config config = new Config( defaultBackupPortHostParams() );
            Monitors monitors = new Monitors();
            monitors.addMonitorListener( new StoreSnoopingMonitor( firstStoreFinishedStreaming, transactionCommitted,
                    storesThatHaveBeenStreamed ) );
            OnlineBackupKernelExtension backup = new OnlineBackupKernelExtension( config, db, db
                    .getDependencyResolver().resolveDependency( KernelPanicEventGenerator.class ),
                    new DevNullLoggingService(), monitors );
            backup.start();
            // when
            BackupService backupService = new BackupService( fileSystem );
            ExecutorService executor = Executors.newSingleThreadExecutor();
            executor.execute( new Runnable()
            {
                @Override
                public void run()
                {
                    awaitLatch( firstStoreFinishedStreaming );
                    try ( Transaction tx = db.beginTx() )
                    {
                        Node node1 = db.createNode();
                        Node node2 = db.createNode();
                        node1.createRelationshipTo( node2, DynamicRelationshipType.withName( "foobydoo" ) );
                        tx.success();
                    }
                    finally
                    {
                        db.getDependencyResolver().resolveDependency( DataSourceManager.class )
                                .getDataSource().forceEverything();
                        transactionCommitted.countDown();
                    }
                }
            } );
            BackupService.BackupOutcome backupOutcome = backupService.doFullBackup( BACKUP_HOST, backupPort,
                    backupDir.getAbsolutePath(), true, new Config( params ) );
            backup.stop();
            executor.shutdown();
            executor.awaitTermination( 30, TimeUnit.SECONDS );
            db.shutdown();

            // then
            assertEquals( DbRepresentation.of( storeDir ), DbRepresentation.of( backupDir ) );
            assertTrue( backupOutcome.isConsistent() );
        }
        finally
        {
            db.shutdown();
        }
    }

    private Map<String, String> defaultBackupPortHostParams()
    {
        Map<String, String> params = new HashMap<>();
        params.put( OnlineBackupSettings.online_backup_server.name(), BACKUP_HOST + ":" + backupPort );
        return params;
    }

    private GraphDatabaseAPI createDb( File storeDir, Map<String, String> params )
    {
        return (GraphDatabaseAPI) new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( storeDir.getPath() )
                .setConfig( params )
                .newGraphDatabase();
    }

    private void createAndIndexNode( GraphDatabaseService db, int i )
    {
        Transaction tx = db.beginTx();
        try
        {
            Index<Node> index = db.index().forNodes( "delete_me" );
            Node node = db.createNode();
            node.setProperty( "id", System.currentTimeMillis() + i );
            index.add( node, "delete", "me" );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    private BaseMatcher<File[]> hasFile( final String fileName )
    {
        return new BaseMatcher<File[]>()
        {
            @Override
            public boolean matches( Object o )
            {
                File[] files = (File[]) o;
                if ( files == null )
                {
                    return false;
                }
                for ( File file : files )
                {
                    if ( file.getAbsolutePath().contains( fileName ) )
                    {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( String.format( "[%s] in list of copied files", fileName ) );
            }
        };
    }

    private Pair<Integer,Long> getLastMasterForCommittedTx() throws IOException
    {
        GraphDatabaseAPI db = (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabase(
                backupDir.getAbsolutePath() );
        try
        {
            LogicalTransactionStore transactionStore =
                    db.getDependencyResolver().resolveDependency( LogicalTransactionStore.class );
            TransactionIdStore transactionIdStore =
                    db.getDependencyResolver().resolveDependency( TransactionIdStore.class );
            TransactionMetadata metadata = transactionStore.getMetadataFor(
                    transactionIdStore.getLastCommittingTransactionId() );
            return Pair.of( metadata.getMasterId(), metadata.getChecksum() );
        }
        catch ( NoSuchTransactionException e )
        {   // Test assertions seem to want null as an indication that it didn't exist
            return null;
        }
        finally
        {
            db.shutdown();
        }
    }
}
