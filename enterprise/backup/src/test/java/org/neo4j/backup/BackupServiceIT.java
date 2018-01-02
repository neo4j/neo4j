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
package org.neo4j.backup;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.internal.matchers.Any;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.net.ConnectException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.neo4j.com.storecopy.StoreCopyClient;
import org.neo4j.com.storecopy.StoreCopyServer;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.Index;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.MetaDataStore.Position;
import org.neo4j.kernel.impl.store.MismatchingStoreIdException;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.storemigration.LogFiles;
import org.neo4j.kernel.impl.storemigration.StoreFile;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.IOCursor;
import org.neo4j.kernel.impl.transaction.log.LogFile;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.log.ReadOnlyTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.impl.transaction.state.NeoStoresSupplier;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.impl.util.DependenciesProxy;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.Logger;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.Barrier;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.EmbeddedDatabaseRule;
import org.neo4j.test.PageCacheRule;
import org.neo4j.test.SuppressOutput;
import org.neo4j.test.TargetDirectory;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.backup.BackupServiceStressTestingBuilder.untilTimeExpired;

public class BackupServiceIT
{
    private static final class StoreSnoopingMonitor extends StoreCopyServer.Monitor.Adapter
    {
        private final Barrier barrier;

        private StoreSnoopingMonitor( Barrier barrier )
        {
            this.barrier = barrier;
        }

        @Override
        public void finishStreamingStoreFile( File storefile )
        {
            if ( storefile.getAbsolutePath().contains( NODE_STORE ) ||
                 storefile.getAbsolutePath().contains( RELATIONSHIP_STORE ) )
            {
                barrier.reached(); // multiple calls to this barrier will not block
            }
        }
    }

    @Rule
    public final TargetDirectory.TestDirectory target = TargetDirectory.testDirForTest( BackupServiceIT.class );
    private static final String NODE_STORE = StoreFactory.NODE_STORE_NAME;
    private static final String RELATIONSHIP_STORE = StoreFactory.RELATIONSHIP_STORE_NAME;
    private static final String BACKUP_HOST = "localhost";

    private final FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
    private final Monitors monitors = new Monitors();
    private File storeDir;
    private File backupDir;
    public int backupPort = 8200;

    @Rule
    public EmbeddedDatabaseRule dbRule = new EmbeddedDatabaseRule( getClass() ).startLazily();
    @Rule
    public SuppressOutput suppressOutput = SuppressOutput.suppressAll();
    @Rule
    public final PageCacheRule pageCacheRule = new PageCacheRule();

    @Before
    public void setup()
    {
        backupPort = backupPort + 1;
        storeDir = dbRule.getStoreDirFile();
        backupDir = target.directory( "backup_dir" );
    }

    private BackupService backupService()
    {
        return new BackupService( fileSystem, FormattedLogProvider.toOutputStream( System.out ), new Monitors() );
    }

    private BackupService backupService( LogProvider logProvider )
    {
        return new BackupService( fileSystem, logProvider, new Monitors() );
    }

    @Test
    public void shouldPrintThatFullBackupIsPerformed() throws Exception
    {
        defaultBackupPortHostParams();
        GraphDatabaseService db = dbRule.getGraphDatabaseAPI();

        final Log log = mock( Log.class );
        LogProvider logProvider = new LogProvider()
        {
            @Override
            public Log getLog( Class loggingClass )
            {
                return log;
            }

            @Override
            public Log getLog( String name )
            {
                return log;
            }
        };

        backupService( logProvider ).doIncrementalBackupOrFallbackToFull( BACKUP_HOST, backupPort,
                backupDir, ConsistencyCheck.NONE, dbRule.getConfigCopy(), BackupClient.BIG_READ_TIMEOUT, false );

        verify( log ).info( "Previous backup not found, a new full backup will be performed." );
    }

    @Test
    public void shouldPrintThatIncrementalBackupIsPerformedAndFallingBackToFull() throws Exception
    {
        defaultBackupPortHostParams();
        Config defaultConfig = dbRule.getConfigCopy();
        dbRule.setConfig( GraphDatabaseSettings.keep_logical_logs, "false" );
        // have logs rotated on every transaction
        GraphDatabaseAPI db = dbRule.getGraphDatabaseAPI();

        createAndIndexNode( db, 1 );

        // A full backup
        backupService().doFullBackup( BACKUP_HOST, backupPort, backupDir, ConsistencyCheck.NONE, defaultConfig,
                BackupClient.BIG_READ_TIMEOUT, false );

        // And the log the backup uses is rotated out
        createAndIndexNode( db, 2 );
        rotateAndCheckPoint( db );
        createAndIndexNode( db, 3 );
        rotateAndCheckPoint( db );
        createAndIndexNode( db, 4 );
        rotateAndCheckPoint( db );

        final Log log = mock( Log.class );
        LogProvider logProvider = new LogProvider()
        {
            @Override
            public Log getLog( Class loggingClass )
            {
                return log;
            }

            @Override
            public Log getLog( String name )
            {
                return log;
            }
        };

        backupService( logProvider ).doIncrementalBackupOrFallbackToFull( BACKUP_HOST, backupPort,
                backupDir, ConsistencyCheck.NONE, dbRule.getConfigCopy(), BackupClient.BIG_READ_TIMEOUT, false );

        verify( log ).info( "Previous backup found, trying incremental backup." );
        verify( log ).info( "Existing backup is too far out of date, a new full backup will be performed." );
    }

    @Test
    public void shouldThrowUsefulMessageWhenCannotConnectDuringFullBackup() throws Exception
    {
        try
        {
            backupService().doIncrementalBackupOrFallbackToFull( BACKUP_HOST, 56789, backupDir, ConsistencyCheck.NONE,
                    dbRule.getConfigCopy(), BackupClient.BIG_READ_TIMEOUT, false );
            fail( "No exception thrown" );
        }
        catch ( RuntimeException e )
        {
            assertThat( e.getMessage(), containsString( "BackupClient could not connect" ) );
            assertThat( e.getCause(), instanceOf( ConnectException.class ) );
        }
    }

    @Test
    public void shouldThrowUsefulMessageWhenCannotConnectDuringIncrementalBackup() throws Exception
    {
        defaultBackupPortHostParams();
        GraphDatabaseAPI db = dbRule.getGraphDatabaseAPI();
        BackupService backupService = backupService();

        createAndIndexNode( db, 1 );

        // A full backup
        backupService.doFullBackup( BACKUP_HOST, backupPort, backupDir,
                ConsistencyCheck.NONE, dbRule.getConfigCopy(), BackupClient.BIG_READ_TIMEOUT, false );
        try
        {
            backupService().doIncrementalBackupOrFallbackToFull( BACKUP_HOST, 56789, backupDir, ConsistencyCheck.NONE,
                    dbRule.getConfigCopy(), BackupClient.BIG_READ_TIMEOUT, false );
            fail( "No exception thrown" );
        }
        catch ( RuntimeException e )
        {
            assertThat( e.getMessage(), containsString( "BackupClient could not connect" ) );
            assertThat( e.getCause(), instanceOf( ConnectException.class ) );
        }
    }

    @Test
    public void shouldThrowExceptionWhenDoingFullBackupWhenDirectoryHasSomeFiles() throws Exception
    {
        // given
        defaultBackupPortHostParams();
        GraphDatabaseAPI db = dbRule.getGraphDatabaseAPI();
        createAndIndexNode( db, 1 );

        // Touch a random file
        assertTrue( new File( backupDir, ".jibberishfile" ).createNewFile() );

        try
        {
            // when
            backupService().doFullBackup( BACKUP_HOST, backupPort, backupDir.getAbsoluteFile(),
                    ConsistencyCheck.DEFAULT, dbRule.getConfigCopy(), BackupClient.BIG_READ_TIMEOUT, false );
            fail( "Should have thrown an exception" );
        }
        catch ( RuntimeException ex )
        {
            // then
            assertThat( ex.getMessage(), containsString( "is not empty" ) );
        }
        finally
        {
            db.shutdown();
        }
    }

    @Test
    public void shouldThrowExceptionWhenDoingFullBackupWhenDirectoryHasSomeDirs() throws Exception
    {
        // given
        defaultBackupPortHostParams();
        GraphDatabaseAPI db = dbRule.getGraphDatabaseAPI();
        createAndIndexNode( db, 1 );

        // Touch a random directory
        assertTrue( new File( backupDir, "jibberishfolder" ).mkdir() );

        try
        {
            // when
            backupService().doFullBackup( BACKUP_HOST, backupPort, backupDir.getAbsoluteFile(),
                    ConsistencyCheck.DEFAULT, dbRule.getConfigCopy(), BackupClient.BIG_READ_TIMEOUT, false );
            fail( "Should have thrown an exception" );
        }
        catch ( RuntimeException ex )
        {
            // then
            assertThat( ex.getMessage(), containsString( "is not empty" ) );
        }
        finally
        {
            db.shutdown();
        }
    }

    @Test
    public void shouldRemoveTempDirectory() throws Throwable
    {
        // given
        defaultBackupPortHostParams();
        GraphDatabaseAPI db = dbRule.getGraphDatabaseAPI();
        createAndIndexNode( db, 1 );

        // when
        BackupService backupService = backupService();
        backupService.doFullBackup( BACKUP_HOST, backupPort, backupDir.getAbsoluteFile(), ConsistencyCheck.NONE,
                dbRule.getConfigCopy(), BackupClient.BIG_READ_TIMEOUT, false );
        db.shutdown();

        // then
        assertFalse( "Temp directory was not removed as expected",
                fileSystem.fileExists( new File( backupDir, StoreCopyClient.TEMP_COPY_DIRECTORY_NAME ) ) );
    }

    @Test
    public void shouldCopyStoreFiles() throws Throwable
    {
        // given
        defaultBackupPortHostParams();
        GraphDatabaseAPI db = dbRule.getGraphDatabaseAPI();
        createAndIndexNode( db, 1 );

        // when
        BackupService backupService = backupService();
        backupService.doFullBackup( BACKUP_HOST, backupPort, backupDir.getAbsoluteFile(), ConsistencyCheck.NONE,
                dbRule.getConfigCopy(), BackupClient.BIG_READ_TIMEOUT, false );
        db.shutdown();

        // then
        File[] files = fileSystem.listFiles( backupDir );

        assertTrue( files.length > 0 );

        for ( final StoreFile storeFile : StoreFile.values() )
        {
            assertThat( files, hasFile( storeFile.storeFileName() ) );
        }

        assertEquals( DbRepresentation.of( storeDir ), DbRepresentation.of( backupDir ) );
    }

    /*
     * During incremental backup destination db should not track free ids independently from source db
     * for now we will always cleanup id files generated after incremental backup and will regenerate them afterwards
     * This should prevent situation when destination db free id following master, but never allocates it from
     * generator till some db will be started on top of it.
     * That will cause all sorts of problems with several entities in a store with same id.
     *
     * As soon as backup will be able to align ids between participants please remove description and adapt test.
     */
    @Test
    public void incrementallyBackupDatabaseShouldNotKeepGeneratedIdFiles()
    {
        defaultBackupPortHostParams();
        GraphDatabaseAPI graphDatabase = dbRule.getGraphDatabaseAPI();
        Label markerLabel = DynamicLabel.label( "marker" );

        try ( Transaction transaction = graphDatabase.beginTx() )
        {
            Node node = graphDatabase.createNode();
            node.addLabel( markerLabel );
            transaction.success();
        }

        try ( Transaction transaction = graphDatabase.beginTx() )
        {
            Node node = findNodeByLabel( graphDatabase, markerLabel );
            for ( int i = 0; i < 10; i++ )
            {
                node.setProperty( "property" + i, "testValue" + i );
            }
            transaction.success();
        }
        // propagate to backup node and properties
        doIncrementalBackupOrFallbackToFull();

        // removing properties will free couple of ids that will be reused during next properties creation
        try ( Transaction transaction = graphDatabase.beginTx() )
        {
            Node node = findNodeByLabel( graphDatabase, markerLabel );
            for ( int i = 0; i < 6; i++ )
            {
                node.removeProperty( "property" + i );
            }

            transaction.success();
        }

        // propagate removed properties
        doIncrementalBackupOrFallbackToFull();

        try ( Transaction transaction = graphDatabase.beginTx() )
        {
            Node node = findNodeByLabel( graphDatabase, markerLabel );
            for ( int i = 10; i < 16; i++ )
            {
                node.setProperty( "property" + i, "updatedValue" + i );
            }

            transaction.success();
        }

        // propagate to backup new properties with reclaimed ids
        doIncrementalBackupOrFallbackToFull();

        // it should be possible to at this point to start db based on our backup and create couple of properties
        // their ids should not clash with already existing
        GraphDatabaseService backupBasedDatabase =
                new GraphDatabaseFactory().newEmbeddedDatabase( backupDir.getAbsolutePath() );
        try
        {
            try ( Transaction transaction = backupBasedDatabase.beginTx() )
            {
                Node node = findNodeByLabel( (GraphDatabaseAPI) backupBasedDatabase, markerLabel );
                Iterable<String> propertyKeys = node.getPropertyKeys();
                for ( String propertyKey : propertyKeys )
                {
                    node.setProperty( propertyKey, "updatedClientValue" + propertyKey );
                }
                node.setProperty( "newProperty", "updatedClientValue" );
                transaction.success();
            }

            try ( Transaction transaction = backupBasedDatabase.beginTx() )
            {
                Node node = findNodeByLabel( (GraphDatabaseAPI) backupBasedDatabase, markerLabel );
                // newProperty + 10 defined properties.
                assertEquals( "We should be able to see all previously defined properties.",
                        11, Iterables.toList( node.getPropertyKeys() ).size() );
            }
        }
        finally
        {
            backupBasedDatabase.shutdown();
        }
    }

    @Test
    public void shouldBeAbleToBackupEvenIfTransactionLogsAreIncomplete() throws Throwable
    {
        /*
        * This test deletes the old persisted log file and expects backup to still be functional. It
        * should not be assumed that the log files have any particular length of history. They could
        * for example have been mangled during backups or removed during pruning.
        */

        // given
        defaultBackupPortHostParams();
        GraphDatabaseAPI db = dbRule.getGraphDatabaseAPI();

        for ( int i = 0; i < 100; i++ )
        {
            createAndIndexNode( db, i );
        }

        final File oldLog = db.getDependencyResolver().resolveDependency( LogFile.class ).currentLogFile();
        rotateAndCheckPoint( db );

        for ( int i = 0; i < 1; i++ )
        {
            createAndIndexNode( db, i );
        }
        rotateAndCheckPoint( db );

        long lastCommittedTxBefore = db.getDependencyResolver().resolveDependency( NeoStores.class ).getMetaDataStore()
                                       .getLastCommittedTransactionId();

        db = dbRule.restartDatabase( new DatabaseRule.RestartAction()
        {
            @Override
            public void run( FileSystemAbstraction fs, File storeDirectory ) throws IOException
            {
                FileUtils.deleteFile( oldLog );
            }
        } );

        long lastCommittedTxAfter = db.getDependencyResolver().resolveDependency( NeoStores.class ).getMetaDataStore()
                                      .getLastCommittedTransactionId();

        // when
        BackupService backupService = backupService();
        BackupService.BackupOutcome outcome = backupService.doFullBackup( BACKUP_HOST, backupPort,
                backupDir.getAbsoluteFile(), ConsistencyCheck.DEFAULT, dbRule.getConfigCopy(),
                BackupClient.BIG_READ_TIMEOUT, false );

        db.shutdown();

        // then
        assertEquals( lastCommittedTxBefore, lastCommittedTxAfter );
        assertTrue( outcome.isConsistent() );
        assertEquals( DbRepresentation.of( storeDir ), DbRepresentation.of( backupDir ) );
    }

    @Test
    public void shouldFindTransactionLogContainingLastNeoStoreTransactionInAnEmptyStore() throws IOException
    {
        // This test highlights a special case where an empty store can return transaction metadata for transaction 0.

        // given
        defaultBackupPortHostParams();
        GraphDatabaseAPI db = dbRule.getGraphDatabaseAPI();

        // when
        BackupService backupService = backupService();
        backupService.doFullBackup( BACKUP_HOST, backupPort, backupDir.getAbsoluteFile(),
                ConsistencyCheck.NONE, dbRule.getConfigCopy(), BackupClient.BIG_READ_TIMEOUT, false );
        db.shutdown();

        // then
        assertEquals( DbRepresentation.of( storeDir ), DbRepresentation.of( backupDir ) );

        assertEquals( 0, getLastTxChecksum( pageCacheRule.getPageCache( fileSystem ) ) );
    }

    @Test
    public void shouldFindTransactionLogContainingLastNeoStoreTransaction() throws Throwable
    {
        // given
        defaultBackupPortHostParams();
        GraphDatabaseAPI db = dbRule.getGraphDatabaseAPI();
        createAndIndexNode( db, 1 );

        // when
        BackupService backupService = backupService();
        backupService.doFullBackup( BACKUP_HOST, backupPort, backupDir.getAbsoluteFile(),
                ConsistencyCheck.NONE, dbRule.getConfigCopy(), BackupClient.BIG_READ_TIMEOUT, false );
        db.shutdown();

        // then
        assertEquals( DbRepresentation.of( storeDir ), DbRepresentation.of( backupDir ) );
        assertNotEquals( 0, getLastTxChecksum( pageCacheRule.getPageCache( fileSystem ) ) );
    }

    @Test
    public void shouldFindValidPreviousCommittedTxIdInFirstNeoStoreLog() throws Throwable
    {
        // given
        defaultBackupPortHostParams();
        GraphDatabaseAPI db = dbRule.getGraphDatabaseAPI();
        createAndIndexNode( db, 1 );
        createAndIndexNode( db, 2 );
        createAndIndexNode( db, 3 );
        createAndIndexNode( db, 4 );

        NeoStores neoStores = db.getDependencyResolver().resolveDependency( NeoStores.class );
        neoStores.flush();
        long txId = neoStores.getMetaDataStore().getLastCommittedTransactionId();

        // when
        BackupService backupService = backupService();
        backupService.doFullBackup( BACKUP_HOST, backupPort, backupDir.getAbsoluteFile(),
                ConsistencyCheck.NONE, dbRule.getConfigCopy(), BackupClient.BIG_READ_TIMEOUT, false );
        db.shutdown();

        // then
        checkPreviousCommittedTxIdFromLog( 0, TransactionIdStore.BASE_TX_ID );
    }

    @Test
    public void shouldFindTransactionLogContainingLastLuceneTransaction() throws Throwable
    {
        // given
        defaultBackupPortHostParams();
        Config defaultConfig = dbRule.getConfigCopy();
        GraphDatabaseAPI db = dbRule.getGraphDatabaseAPI();
        createAndIndexNode( db, 1 );

        // when
        BackupService backupService = backupService();
        backupService.doFullBackup( BACKUP_HOST, backupPort, backupDir.getAbsoluteFile(),
                ConsistencyCheck.NONE, defaultConfig, BackupClient.BIG_READ_TIMEOUT, false );
        db.shutdown();

        // then
        assertEquals( DbRepresentation.of( storeDir ), DbRepresentation.of( backupDir ) );
        assertNotEquals( 0, getLastTxChecksum( pageCacheRule.getPageCache( fileSystem ) ) );
    }

    @Test
    public void shouldGiveHelpfulErrorMessageIfLogsPrunedPastThePointOfNoReturn() throws Exception
    {
        // Given
        defaultBackupPortHostParams();
        Config defaultConfig = dbRule.getConfigCopy();
        dbRule.setConfig( GraphDatabaseSettings.keep_logical_logs, "false" );
        // have logs rotated on every transaction
        GraphDatabaseAPI db = dbRule.getGraphDatabaseAPI();
        BackupService backupService = backupService();

        createAndIndexNode( db, 1 );
        rotateAndCheckPoint( db );

        // A full backup
        backupService.doFullBackup( BACKUP_HOST, backupPort, backupDir.getAbsoluteFile(),
                ConsistencyCheck.NONE, defaultConfig, BackupClient.BIG_READ_TIMEOUT, false );

        // And the log the backup uses is rotated out
        createAndIndexNode( db, 2 );
        rotateAndCheckPoint( db );
        createAndIndexNode( db, 3 );
        rotateAndCheckPoint( db );
        createAndIndexNode( db, 4 );
        rotateAndCheckPoint( db );
        createAndIndexNode( db, 5 );
        rotateAndCheckPoint( db );

        // when
        try
        {
            backupService.doIncrementalBackup( BACKUP_HOST, backupPort, backupDir.getAbsoluteFile(),
                    BackupClient.BIG_READ_TIMEOUT, defaultConfig );
            fail( "Should have thrown exception." );
        }
        // Then
        catch ( IncrementalBackupNotPossibleException e )
        {
            assertThat( e.getMessage(), equalTo( BackupService.TOO_OLD_BACKUP ) );
        }
    }

    @Test
    public void shouldFallbackToFullBackupIfIncrementalFailsAndExplicitlyAskedToDoThis() throws Exception
    {
        // Given
        defaultBackupPortHostParams();
        Config defaultConfig = dbRule.getConfigCopy();
        dbRule.setConfig( GraphDatabaseSettings.keep_logical_logs, "false" );
        // have logs rotated on every transaction
        GraphDatabaseAPI db = dbRule.getGraphDatabaseAPI();
        BackupService backupService = backupService();

        createAndIndexNode( db, 1 );

        // A full backup
        backupService.doFullBackup( BACKUP_HOST, backupPort, backupDir.getAbsoluteFile(),
                ConsistencyCheck.NONE, defaultConfig, BackupClient.BIG_READ_TIMEOUT, false );

        // And the log the backup uses is rotated out
        createAndIndexNode( db, 2 );
        rotateAndCheckPoint( db );
        createAndIndexNode( db, 3 );
        rotateAndCheckPoint( db );
        createAndIndexNode( db, 4 );
        rotateAndCheckPoint( db );

        // when
        backupService.doIncrementalBackupOrFallbackToFull(
                BACKUP_HOST, backupPort, backupDir.getAbsoluteFile(), ConsistencyCheck.NONE, defaultConfig,
                BackupClient.BIG_READ_TIMEOUT, false );

        // Then
        db.shutdown();
        assertEquals( DbRepresentation.of( storeDir ), DbRepresentation.of( backupDir ) );
    }

    private void rotateAndCheckPoint( GraphDatabaseAPI db ) throws IOException
    {
        db.getDependencyResolver().resolveDependency( LogRotation.class ).rotateLogFile();
        db.getDependencyResolver().resolveDependency( CheckPointer.class ).forceCheckPoint(
                new SimpleTriggerInfo( "test" )
        );
    }

    @Test
    public void shouldHandleBackupWhenLogFilesHaveBeenDeleted() throws Exception
    {
        // Given
        defaultBackupPortHostParams();
        Config defaultConfig = dbRule.getConfigCopy();
        dbRule.setConfig( GraphDatabaseSettings.keep_logical_logs, "false" );
        GraphDatabaseAPI db = dbRule.getGraphDatabaseAPI();
        BackupService backupService = backupService();

        createAndIndexNode( db, 1 );

        // A full backup
        backupService.doIncrementalBackupOrFallbackToFull(
                BACKUP_HOST, backupPort, backupDir.getAbsoluteFile(), ConsistencyCheck.NONE, defaultConfig,
                BackupClient.BIG_READ_TIMEOUT, false );

        // And the log the backup uses is rotated out
        createAndIndexNode( db, 2 );
        db = deleteLogFilesAndRestart();

        createAndIndexNode( db, 3 );
        db = deleteLogFilesAndRestart();

        // when
        backupService.doIncrementalBackupOrFallbackToFull(
                BACKUP_HOST, backupPort, backupDir.getAbsoluteFile(), ConsistencyCheck.NONE, defaultConfig,
                BackupClient.BIG_READ_TIMEOUT, false );

        // Then
        db.shutdown();
        assertEquals( DbRepresentation.of( storeDir ), DbRepresentation.of( backupDir ) );
    }

    private GraphDatabaseAPI deleteLogFilesAndRestart()
            throws IOException
    {
        final FileFilter logFileFilter = new FileFilter()
        {
            @Override
            public boolean accept( File pathname )
            {
                return pathname.getName().contains( "logical" );
            }
        };
        return dbRule.restartDatabase( new DatabaseRule.RestartAction()
        {
            @Override
            public void run( FileSystemAbstraction fs, File storeDirectory ) throws IOException
            {
                for ( File logFile : storeDir.listFiles( logFileFilter ) )
                {
                    logFile.delete();
                }
            }
        } );
    }

    @Test
    public void shouldDoFullBackupOnIncrementalFallbackToFullIfNoBackupFolderExists() throws Exception
    {
        // Given
        defaultBackupPortHostParams();
        Config defaultConfig = dbRule.getConfigCopy();
        dbRule.setConfig( GraphDatabaseSettings.keep_logical_logs, "false" );
        GraphDatabaseAPI db = dbRule.getGraphDatabaseAPI();
        BackupService backupService = backupService();

        createAndIndexNode( db, 1 );

        // when
        backupService.doIncrementalBackupOrFallbackToFull(
                BACKUP_HOST, backupPort, backupDir.getAbsoluteFile(), ConsistencyCheck.NONE, defaultConfig,
                BackupClient.BIG_READ_TIMEOUT, false );

        // then
        db.shutdown();
        assertEquals( DbRepresentation.of( storeDir ), DbRepresentation.of( backupDir ) );
    }

    @Test
    public void shouldContainTransactionsThatHappenDuringBackupProcess() throws Throwable
    {
        // given
        defaultBackupPortHostParams();
        Config defaultConfig = dbRule.getConfigCopy();
        dbRule.setConfig( OnlineBackupSettings.online_backup_enabled, "false" );
        Config withOnlineBackupDisabled = dbRule.getConfigCopy();

        final Barrier.Control barrier = new Barrier.Control();
        final GraphDatabaseAPI db = dbRule.getGraphDatabaseAPI();

        createAndIndexNode( db, 1 ); // create some data

        final DependencyResolver resolver = db.getDependencyResolver();
        NeoStoreDataSource ds = resolver.resolveDependency( DataSourceManager.class ).getDataSource();
        long expectedLastTxId = ds.getNeoStores().getMetaDataStore().getLastCommittedTransactionId();

        // This monitor is added server-side...
        monitors.addMonitorListener( new StoreSnoopingMonitor( barrier ) );

        Dependencies dependencies = new Dependencies( resolver );
        dependencies.satisfyDependencies( defaultConfig, monitors, NullLogProvider.getInstance() );

        OnlineBackupKernelExtension backup = (OnlineBackupKernelExtension) new OnlineBackupExtensionFactory().newKernelExtension(
                DependenciesProxy.dependencies(dependencies, OnlineBackupExtensionFactory.Dependencies.class));
        backup.start();

        // when
        BackupService backupService = backupService();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.execute( new Runnable()
        {
            @Override
            public void run()
            {
                barrier.awaitUninterruptibly();

                createAndIndexNode( db, 1 );
                resolver.resolveDependency( NeoStoresSupplier.class ).get().flush();

                barrier.release();
            }
        } );

        BackupService.BackupOutcome backupOutcome = backupService.doFullBackup( BACKUP_HOST, backupPort,
                backupDir.getAbsoluteFile(), ConsistencyCheck.DEFAULT, withOnlineBackupDisabled,
                BackupClient.BIG_READ_TIMEOUT, false );

        backup.stop();
        executor.shutdown();
        executor.awaitTermination( 30, TimeUnit.SECONDS );

        // then
        checkPreviousCommittedTxIdFromLog( 0, expectedLastTxId );
        File neoStore = new File( storeDir, MetaDataStore.DEFAULT_NAME );
        long txIdFromOrigin = MetaDataStore
                .getRecord( resolver.resolveDependency( PageCache.class ), neoStore, Position.LAST_TRANSACTION_ID );
        checkLastCommittedTxIdInLogAndNeoStore( expectedLastTxId+1, txIdFromOrigin );
        assertEquals( DbRepresentation.of( db ), DbRepresentation.of( backupDir ) );
        assertTrue( backupOutcome.isConsistent() );
    }

    @Test
    public void backupsShouldBeMentionedInServerConsoleLog() throws Throwable
    {
        // given
        defaultBackupPortHostParams();
        Config config = dbRule.getConfigCopy();
        dbRule.setConfig( OnlineBackupSettings.online_backup_enabled, "false" );
        Config withOnlineBackupDisabled = dbRule.getConfigCopy();
        createAndIndexNode( dbRule, 1 );

        final Log log = mock( Log.class );
        LogProvider logProvider = new LogProvider()
        {
            @Override
            public Log getLog( Class loggingClass )
            {
                return log;
            }

            @Override
            public Log getLog( String name )
            {
                return log;
            }
        };
        Logger logger = mock( Logger.class );
        when( log.infoLogger() ).thenReturn( logger );
        LogService logService = mock( LogService.class );
        when( logService.getInternalLogProvider() ).thenReturn( logProvider );

        Dependencies dependencies = new Dependencies( dbRule.getDependencyResolver() );
        dependencies.satisfyDependencies( config, monitors, logService );

        OnlineBackupKernelExtension backup = (OnlineBackupKernelExtension) new OnlineBackupExtensionFactory()
                .newKernelExtension( DependenciesProxy
                        .dependencies( dependencies, OnlineBackupExtensionFactory.Dependencies.class ) );

        backup.start();

        // when
        backupService().doFullBackup( BACKUP_HOST, backupPort, backupDir, ConsistencyCheck.NONE,
                withOnlineBackupDisabled, BackupClient.BIG_READ_TIMEOUT, false );

        // then
        verify( logger ).log( "Full backup started..." );
        verify( logger ).log( "Full backup finished." );

        // when
        createAndIndexNode( dbRule, 2 );

        backupService().doIncrementalBackupOrFallbackToFull( BACKUP_HOST, backupPort, backupDir,
                ConsistencyCheck.NONE, withOnlineBackupDisabled, BackupClient.BIG_READ_TIMEOUT, false );

        backup.stop();

        // then
        verify( logger ).log( "Incremental backup started..." );
        verify( logger ).log( "Incremental backup finished." );
    }

    @Test
    public void incrementalBackupShouldFailWhenTargetDirContainsDifferentStore() throws IOException
    {
        // Given
        defaultBackupPortHostParams();
        Config defaultConfig = dbRule.getConfigCopy();
        GraphDatabaseAPI db1 = dbRule.getGraphDatabaseAPI();
        createAndIndexNode( db1, 1 );

        backupService().doFullBackup( BACKUP_HOST, backupPort, backupDir.getAbsoluteFile(), ConsistencyCheck.NONE,
                defaultConfig, BackupClient.BIG_READ_TIMEOUT, false );

        // When
        GraphDatabaseAPI db2 = dbRule.restartDatabase( new DatabaseRule.RestartAction()
        {
            @Override
            public void run( FileSystemAbstraction fs, File storeDirectory ) throws IOException
            {
                deleteAllBackedUpTransactionLogs();

                fileSystem.deleteRecursively( storeDir );
                fileSystem.mkdir( storeDir );
            }
        } );
        createAndIndexNode( db2, 2 );

        try
        {
            backupService().doIncrementalBackupOrFallbackToFull( BACKUP_HOST, backupPort,
                    backupDir.getAbsoluteFile(), ConsistencyCheck.NONE, defaultConfig,
                    BackupClient.BIG_READ_TIMEOUT, false );

            fail( "Should have thrown exception about mismatching store ids" );
        }
        catch ( RuntimeException e )
        {
            // Then
            assertThat( e.getMessage(), equalTo( BackupService.DIFFERENT_STORE ) );
            assertThat( e.getCause(), instanceOf( MismatchingStoreIdException.class ) );
        }
    }

    @Test
    public void theBackupServiceShouldBeHappyUnderStress() throws Exception
    {
        Callable<Integer> callable = new BackupServiceStressTestingBuilder()
                .until( untilTimeExpired( 10, SECONDS ) )
                .withStore( storeDir )
                .withBackupDirectory( backupDir )
                .withBackupAddress( BACKUP_HOST, backupPort )
                .build();

        int brokenStores = callable.call();
        assertEquals( 0, brokenStores );
    }

    private void defaultBackupPortHostParams()
    {
        dbRule.setConfig( OnlineBackupSettings.online_backup_server, BACKUP_HOST + ":" + backupPort );
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

    private void checkPreviousCommittedTxIdFromLog( long logVersion, long txId ) throws IOException
    {
        // Assert header of specified log version containing correct txId
        PhysicalLogFiles logFiles = new PhysicalLogFiles( backupDir, fileSystem );
        LogHeader logHeader = LogHeaderReader.readLogHeader( fileSystem, logFiles.getLogFileForVersion( logVersion ) );
        assertEquals( txId, logHeader.lastCommittedTxId );
    }

    private void checkLastCommittedTxIdInLogAndNeoStore( long txId, long txIdFromOrigin ) throws IOException
    {
        // Assert last committed transaction can be found in tx log and is the last tx in the log
        LifeSupport life = new LifeSupport();
        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        LogicalTransactionStore transactionStore =
                life.add( new ReadOnlyTransactionStore( pageCache, fileSystem, backupDir, monitors ) );
        life.start();
        try ( IOCursor<CommittedTransactionRepresentation> cursor =
                      transactionStore.getTransactions( txId ) )
        {
            assertTrue( cursor.next() );
            assertEquals( txId, cursor.get().getCommitEntry().getTxId() );
            assertFalse( cursor.next() );
        }
        finally
        {
            life.shutdown();
        }

        // Assert last committed transaction is correct in neostore
        assertEquals( txId, txIdFromOrigin );
    }

    private long getLastTxChecksum( PageCache pageCache ) throws IOException
    {
        File neoStore = new File( backupDir, MetaDataStore.DEFAULT_NAME );
        return MetaDataStore.getRecord( pageCache, neoStore, Position.LAST_TRANSACTION_CHECKSUM );
    }

    private void deleteAllBackedUpTransactionLogs()
    {
        for ( File log : fileSystem.listFiles( backupDir, LogFiles.FILENAME_FILTER ) )
        {
            fileSystem.deleteFile( log );
        }
    }

    private void doIncrementalBackupOrFallbackToFull()
    {
        BackupService backupService = backupService();
        backupService.doIncrementalBackupOrFallbackToFull( BACKUP_HOST, backupPort,
                backupDir, ConsistencyCheck.NONE, new Config(), BackupClient.BIG_READ_TIMEOUT, false );
    }

    private Node findNodeByLabel( GraphDatabaseAPI graphDatabase, Label label )
    {
        try ( ResourceIterator<Node> nodes = graphDatabase.findNodes( label ) )
        {
            return nodes.next();
        }
    }
}
