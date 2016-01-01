/**
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

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.Index;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.core.KernelPanicEventGenerator;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.MismatchingStoreIdException;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.storemigration.LogFiles;
import org.neo4j.kernel.impl.storemigration.legacystore.v20.StoreFile20;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.kernel.monitoring.StoreCopyMonitor;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.Mute;
import org.neo4j.test.TargetDirectory;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.index.impl.lucene.LuceneDataSource.DEFAULT_NAME;
import static org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource.DEFAULT_DATA_SOURCE_NAME;

public class BackupServiceIT
{
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

    private int backupPort = 8200;

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
            new BackupService( fileSystem ).doFullBackup( "", 0, backupDir.getAbsolutePath(), true, new Config(), false );
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
        backupService.doFullBackup( BACKUP_HOST, backupPort, backupDir.getAbsolutePath(), false, defaultConfig(), false );
        db.shutdown();

        // then
        File[] files = fileSystem.listFiles( backupDir );

        for ( final StoreFile20 storeFile : StoreFile20.values() )
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
        backupService.doFullBackup( BACKUP_HOST, backupPort, backupDir.getAbsolutePath(), false, defaultConfig(), false );
        db.shutdown();

        // then
        assertEquals( DbRepresentation.of( storeDir ), DbRepresentation.of( backupDir ) );

        assertNotNull( getLastMasterForCommittedTx( DEFAULT_DATA_SOURCE_NAME ) );
        assertNotNull( getLastMasterForCommittedTx( DEFAULT_NAME ) );
    }

    @Test
    public void shouldFindTransactionLogContainingLastNeoStoreTransaction() throws Throwable
    {
        // given
        GraphDatabaseService db = createDb( storeDir, defaultBackupPortHostParams() );
        createAndIndexNode( db, 1 );

        // when
        BackupService backupService = new BackupService( fileSystem );
        backupService.doFullBackup( BACKUP_HOST, backupPort, backupDir.getAbsolutePath(), false, defaultConfig(), false );
        db.shutdown();

        // then
        assertEquals( DbRepresentation.of( storeDir ), DbRepresentation.of( backupDir ) );
        assertNotNull( getLastMasterForCommittedTx( DEFAULT_DATA_SOURCE_NAME ) );
    }

    @Test
    public void shouldFindValidPreviousCommittedTxIdInFirstNeoStoreLog() throws Throwable
    {
        // given
        GraphDatabaseService db = createDb( storeDir, defaultBackupPortHostParams() );
        createAndIndexNode( db, 1 );
        createAndIndexNode( db, 2 );
        createAndIndexNode( db, 3 );
        createAndIndexNode( db, 4 );

        // when
        BackupService backupService = new BackupService( fileSystem );
        backupService.doFullBackup( BACKUP_HOST, backupPort, backupDir.getAbsolutePath(), false,
                new Config( defaultBackupPortHostParams() ), false );
        db.shutdown();

        // then
        checkPreviousCommittedTxIdFromFirstLog( DEFAULT_DATA_SOURCE_NAME ); // neo store
        checkPreviousCommittedTxIdFromFirstLog( DEFAULT_NAME ); // lucene
    }

    @Test
    public void shouldFindTransactionLogContainingLastLuceneTransaction() throws Throwable
    {
        // given
        GraphDatabaseService db = createDb( storeDir, defaultBackupPortHostParams() );
        createAndIndexNode( db, 1 );

        // when
        BackupService backupService = new BackupService( fileSystem );
        backupService.doFullBackup( BACKUP_HOST, backupPort, backupDir.getAbsolutePath(), false, defaultConfig(), false );
        db.shutdown();

        // then
        assertEquals( DbRepresentation.of( storeDir ), DbRepresentation.of( backupDir ) );
        assertNotNull( getLastMasterForCommittedTx( DEFAULT_NAME ) );
    }

    @Test
    public void shouldGiveHelpfulErrorMessageIfLogsPrunedPastThePointOfNoReturn() throws Exception
    {
        // Given
        Map<String, String> config = defaultBackupPortHostParams();
        config.put( GraphDatabaseSettings.keep_logical_logs.name(), "false" );
        GraphDatabaseAPI db = createDb( storeDir, config );
        BackupService backupService = new BackupService( fileSystem );

        createAndIndexNode( db, 1 );

        // A full backup
        backupService.doFullBackup( BACKUP_HOST, backupPort, backupDir.getAbsolutePath(), false, defaultConfig(), false );

        // And the log the backup uses is rotated out
        createAndIndexNode( db, 2 );
        rotateLog( db );
        createAndIndexNode( db, 3 );
        rotateLog( db );
        createAndIndexNode( db, 3 );
        rotateLog( db );


        // when
        try
        {
            backupService.doIncrementalBackup( BACKUP_HOST, backupPort, backupDir.getAbsolutePath(), false );
            fail( "Should have thrown exception." );
        }

        // Then
        catch ( IncrementalBackupNotPossibleException e )
        {
            assertThat( e.getMessage(), equalTo( BackupService.TOO_OLD_BACKUP ) );
        }
        db.shutdown();
    }

    @Test
    public void shouldFallbackToFullBackupIfIncrementalFailsAndExplicitlyAskedToDoThis() throws Exception
    {
        // Given
        Map<String, String> config = defaultBackupPortHostParams();
        config.put( GraphDatabaseSettings.keep_logical_logs.name(), "false" );
        GraphDatabaseAPI db = createDb( storeDir, config );
        BackupService backupService = new BackupService( fileSystem );

        createAndIndexNode( db, 1 );

        // A full backup
        backupService.doFullBackup( BACKUP_HOST, backupPort, backupDir.getAbsolutePath(), false, defaultConfig(), false );

        // And the log the backup uses is rotated out
        createAndIndexNode( db, 2 );
        rotateLog( db );
        createAndIndexNode( db, 3 );
        rotateLog( db );
        createAndIndexNode( db, 3 );
        rotateLog( db );


        // when
        backupService.doIncrementalBackupOrFallbackToFull(
                BACKUP_HOST, backupPort, backupDir.getAbsolutePath(), false, defaultConfig(), false );


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
        backupService.doIncrementalBackupOrFallbackToFull(
                BACKUP_HOST, backupPort, backupDir.getAbsolutePath(), false, defaultConfig(), false );

        // And the log the backup uses is rotated out
        createAndIndexNode( db, 2 );
        db = deleteLogFilesAndRestart( config, db );

        createAndIndexNode( db, 3 );
        db = deleteLogFilesAndRestart( config, db );

        // when
        backupService.doIncrementalBackupOrFallbackToFull(
                BACKUP_HOST, backupPort, backupDir.getAbsolutePath(), false, defaultConfig(), false );

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
        backupService.doIncrementalBackupOrFallbackToFull(
                BACKUP_HOST, backupPort, backupDir.getAbsolutePath(), false, defaultConfig(), false );

        // then
        db.shutdown();
        assertEquals( DbRepresentation.of( storeDir ), DbRepresentation.of( backupDir ) );
    }

    private void rotateLog( GraphDatabaseAPI db ) throws IOException
    {
        DependencyResolver resolver = db.getDependencyResolver();
        resolver.resolveDependency( XaDataSourceManager.class ).getNeoStoreDataSource().rotateLogicalLog();
    }

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

        createAndIndexNode( db, 1 ); // create some data
        XaDataSourceManager xaDataSourceManager = db.getDependencyResolver().resolveDependency(
                XaDataSourceManager.class );
        long expectedTxIdInNeoStore = xaDataSourceManager.getXaDataSource( DEFAULT_DATA_SOURCE_NAME ).getLastCommittedTxId();
        long expectedTxIdInLuceneStore = xaDataSourceManager.getXaDataSource( DEFAULT_NAME ).getLastCommittedTxId();

        Config config = defaultConfig();

        Monitors monitors = new Monitors();
        monitors.addMonitorListener( new StoreCopyMonitor.Adaptor()
        {
            @Override
            public void streamedFile( File file )
            {
                if ( neitherStoreHasBeenStreamed() )
                {
                    if ( file.getAbsolutePath().contains( NODE_STORE ) )
                    {
                        storesThatHaveBeenStreamed.add( NODE_STORE );
                        firstStoreFinishedStreaming.countDown();
                    }
                    else if ( file.getAbsolutePath().contains( RELATIONSHIP_STORE ) )
                    {
                        storesThatHaveBeenStreamed.add( RELATIONSHIP_STORE );
                        firstStoreFinishedStreaming.countDown();
                    }
                }
            }

            private boolean neitherStoreHasBeenStreamed()
            {
                return storesThatHaveBeenStreamed.isEmpty();
            }

            @Override
            public void streamingFile( File file )
            {
                if ( file.getAbsolutePath().contains( RELATIONSHIP_STORE ) )
                {
                    if ( streamedFirst( NODE_STORE ) )
                    {
                        try
                        {
                            transactionCommitted.await();
                        }
                        catch ( InterruptedException e )
                        {
                            e.printStackTrace();
                        }
                    }
                }
                else if ( file.getAbsolutePath().contains( NODE_STORE ) )
                {
                    if ( streamedFirst( RELATIONSHIP_STORE ) )
                    {
                        try
                        {
                            transactionCommitted.await();
                        }
                        catch ( InterruptedException e )
                        {
                            e.printStackTrace();
                        }
                    }
                }
            }

            private boolean streamedFirst( String store )
            {
                return !storesThatHaveBeenStreamed.isEmpty() && storesThatHaveBeenStreamed.get( 0 ).equals( store );
            }
        } );

        OnlineBackupKernelExtension backup = new OnlineBackupKernelExtension(
                config,
                db,
                db.getDependencyResolver().resolveDependency( XaDataSourceManager.class ),
                db.getDependencyResolver().resolveDependency( KernelPanicEventGenerator.class ),
                new DevNullLoggingService(),
                monitors );

        backup.start();

        // when
        BackupService backupService = new BackupService( fileSystem );

        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.execute( new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    firstStoreFinishedStreaming.await();

                    createAndIndexNode( db, 1 );
                    db.getDependencyResolver().resolveDependency( XaDataSourceManager.class ).getNeoStoreDataSource()
                            .getNeoStore().flushAll();

                    transactionCommitted.countDown();
                }
                catch ( Exception e )
                {
                    e.printStackTrace();
                }
            }
        } );

        BackupService.BackupOutcome backupOutcome = backupService.doFullBackup( BACKUP_HOST, backupPort,
                backupDir.getAbsolutePath(), true,
                new Config( params ), false );

        backup.stop();
        executor.shutdown();
        executor.awaitTermination( 30, TimeUnit.SECONDS );
        db.shutdown();

        // then
        assertEquals( DbRepresentation.of( storeDir ), DbRepresentation.of( backupDir ) );
        assertTrue( backupOutcome.isConsistent() );

        // also verify the last committed tx id is correctly set
        checkPreviousCommittedTxIdFromFirstLog( DEFAULT_DATA_SOURCE_NAME, 0, expectedTxIdInNeoStore );
        checkPreviousCommittedTxIdFromFirstLog( DEFAULT_NAME, 0, expectedTxIdInLuceneStore );
    }

    @Test
    public void incrementalBackupShouldFailWhenTargetDirContainsDifferentStore() throws IOException
    {
        // Given
        GraphDatabaseAPI db1 = createDb( storeDir, defaultBackupPortHostParams() );
        createAndIndexNode( db1, 1 );

        new BackupService( fileSystem ).doFullBackup(
                BACKUP_HOST, backupPort, backupDir.getAbsolutePath(), false, defaultConfig(), false );

        db1.shutdown();

        deleteAllBackedUpTransactionLogs();

        fileSystem.deleteRecursively( storeDir );
        fileSystem.mkdir( storeDir );

        // When
        GraphDatabaseAPI db2 = createDb( storeDir, defaultBackupPortHostParams() );
        createAndIndexNode( db2, 2 );

        try
        {
            new BackupService( fileSystem ).doIncrementalBackupOrFallbackToFull(
                    BACKUP_HOST, backupPort, backupDir.getAbsolutePath(), false, defaultConfig(), false );

            fail( "Should have thrown exception about mismatching store ids" );
        }
        catch ( RuntimeException e )
        {
            // Then
            assertThat( e.getMessage(), equalTo( BackupService.DIFFERENT_STORE ) );
            assertThat( e.getCause(), instanceOf( MismatchingStoreIdException.class ) );
        }
        finally
        {
            db2.shutdown();
        }
    }

    private Map<String, String> defaultBackupPortHostParams()
    {
        Map<String, String> params = new HashMap<>();
        params.put( OnlineBackupSettings.online_backup_server.name(), BACKUP_HOST + ":" + backupPort );
        return params;
    }

    private Config defaultConfig()
    {
        return new Config( defaultBackupPortHostParams() );
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
        try ( Transaction tx = db.beginTx() )
        {
            Index<Node> index = db.index().forNodes( "delete_me" );
            Node node = db.createNode();
            node.setProperty( "id", System.currentTimeMillis() + i );
            index.add( node, "delete", "me" );
            tx.success();
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

    private void checkPreviousCommittedTxIdFromFirstLog( String dataSourceName ) throws IOException
    {
        GraphDatabaseAPI db = (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabase( backupDir
                .getAbsolutePath() );
        XaDataSourceManager xaDataSourceManager = db.getDependencyResolver().resolveDependency(
                XaDataSourceManager.class );
        XaDataSource dataSource = xaDataSourceManager.getXaDataSource( dataSourceName );
        long expectedTxId = dataSource.getLastCommittedTxId() - 1;
        db.shutdown();
        checkPreviousCommittedTxIdFromFirstLog( dataSourceName, 1, expectedTxId );
    }

    private void checkPreviousCommittedTxIdFromFirstLog( String dataSourceName, int logVersion, long expectedTxId ) throws IOException
    {
        GraphDatabaseAPI db = (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabase(
                backupDir.getAbsolutePath() );
        ReadableByteChannel logicalLog = null;
        try
        {
            XaDataSourceManager xaDataSourceManager = db.getDependencyResolver().resolveDependency(
                    XaDataSourceManager.class );
            XaDataSource dataSource = xaDataSourceManager.getXaDataSource( dataSourceName );

            logicalLog = dataSource.getLogicalLog( logVersion );

            ByteBuffer buffer = ByteBuffer.allocate( 64 );
            long[] headerData = VersionAwareLogEntryReader.readLogHeader( buffer, logicalLog, true );

            long previousCommittedTxIdFromFirstLog = headerData[1];

            assertEquals( expectedTxId, previousCommittedTxIdFromFirstLog );
        }
        finally
        {
            db.shutdown();
            if ( logicalLog != null )
            {
                logicalLog.close();
            }
        }
    }

    private Pair<Integer, Long> getLastMasterForCommittedTx( String dataSourceName ) throws IOException
    {
        GraphDatabaseAPI db = (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabase(
                backupDir.getAbsolutePath() );
        try
        {
            XaDataSourceManager xaDataSourceManager = db.getDependencyResolver().resolveDependency(
                    XaDataSourceManager.class );
            XaDataSource dataSource = xaDataSourceManager.getXaDataSource( dataSourceName );
            long lastCommittedTxId = dataSource.getLastCommittedTxId();
            return dataSource.getMasterForCommittedTx( lastCommittedTxId );
        }
        finally
        {
            db.shutdown();
        }
    }

    private void deleteAllBackedUpTransactionLogs()
    {
        for ( File log : fileSystem.listFiles( backupDir, LogFiles.FILENAME_FILTER ) )
        {
            fileSystem.deleteFile( log );
        }
    }
}
