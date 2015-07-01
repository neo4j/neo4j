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

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.neo4j.embedded.CommunityTestGraphDatabase;
import org.neo4j.embedded.TestGraphDatabase;
import org.neo4j.com.storecopy.StoreCopyServer;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.Index;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.MismatchingStoreIdException;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.store.NeoStore.Position;
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
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.impl.transaction.state.NeoStoreSupplier;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.impl.util.DependenciesProxy;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.Barrier;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.PageCacheRule;
import org.neo4j.test.SuppressOutput;
import org.neo4j.test.TargetDirectory;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.backup.BackupServiceStressTestingBuilder.untilTimeExpired;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

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

    private static final TargetDirectory target = TargetDirectory.forTest( BackupServiceIT.class );
    private static final String NODE_STORE = StoreFactory.NODE_STORE_NAME;
    private static final String RELATIONSHIP_STORE = StoreFactory.RELATIONSHIP_STORE_NAME;
    private static final String BACKUP_HOST = "localhost";

    private final FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
    private final Monitors monitors = new Monitors();
    private final File storeDir = target.cleanDirectory( "store_dir" ) ;
    private final File backupDir = target.cleanDirectory( "backup_dir" );
    private int backupPort = 8200;
    private Map<String,String> defaultParams;
    private TestGraphDatabase db;

    @Rule
    public SuppressOutput suppressOutput = SuppressOutput.suppressAll();
    @Rule
    public final PageCacheRule pageCacheRule = new PageCacheRule();

    @Before
    public void setup()
    {
        backupPort = backupPort + 1;
        defaultParams = stringMap( OnlineBackupSettings.online_backup_server.name(), BACKUP_HOST + ":" + backupPort );
    }

    @After
    public void teardown() throws Exception
    {
        if ( db != null )
        {
            db.shutdown();
        }
        FileUtils.deleteRecursively( storeDir );
        FileUtils.deleteRecursively( backupDir );
    }

    private BackupService backupService()
    {
        return new BackupService( fileSystem, FormattedLogProvider.toOutputStream( System.out ), new Monitors() );
    }

    private TestGraphDatabase startDB()
    {
        return CommunityTestGraphDatabase.build()
                .withParams( defaultParams )
                .open( storeDir );
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
            backupService().doFullBackup( "", 0, backupDir.getAbsoluteFile(), true, new Config(),
                    BackupClient.BIG_READ_TIMEOUT, false );
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
        db = startDB();
        createAndIndexNode( db, 1 );

        // when
        BackupService backupService = backupService();
        backupService.doFullBackup( BACKUP_HOST, backupPort, backupDir.getAbsoluteFile(), false, new Config(),
                BackupClient.BIG_READ_TIMEOUT, false );
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
    public void shouldBeAbleToBackupEvenIfTransactionLogsAreIncomplete() throws Throwable
    {
        /*
        * This test deletes the old persisted log file and expects backup to still be functional. It
        * should not be assumed that the log files have any particular length of history. They could
        * for example have been mangled during backups or removed during pruning.
        */

        // given
        db = startDB();

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

        long lastCommittedTxBefore = db.getDependencyResolver().resolveDependency( NeoStore.class )
                .getLastCommittedTransactionId();

        db.shutdown();
        FileUtils.deleteFile( oldLog );
        db = startDB();

        long lastCommittedTxAfter = db.getDependencyResolver().resolveDependency( NeoStore.class )
                .getLastCommittedTransactionId();

        // when
        BackupService backupService = backupService();
        BackupService.BackupOutcome outcome = backupService.doFullBackup( BACKUP_HOST, backupPort,
                backupDir.getAbsoluteFile(), true, new Config(), BackupClient.BIG_READ_TIMEOUT, false );

        db.shutdown();

        // then
        assertEquals( lastCommittedTxBefore, lastCommittedTxAfter );
        assertTrue( outcome.isConsistent() );
        assertEquals( DbRepresentation.of( storeDir ), DbRepresentation.of( backupDir ) );
    }

    @Test
    public void shouldFindTransactionLogContainingLastNeoStoreTransactionInAnEmptyStore()
    {
        // This test highlights a special case where an empty store can return transaction metadata for transaction 0.

        // given
        db = startDB();

        // when
        BackupService backupService = backupService();
        backupService.doFullBackup( BACKUP_HOST, backupPort, backupDir.getAbsoluteFile(), false, new Config(),
                BackupClient.BIG_READ_TIMEOUT, false );
        db.shutdown();

        // then
        assertEquals( DbRepresentation.of( storeDir ), DbRepresentation.of( backupDir ) );

        assertEquals( 0, getLastTxChecksum( pageCacheRule.getPageCache( fileSystem ) ) );
    }

    @Test
    public void shouldFindTransactionLogContainingLastNeoStoreTransaction() throws Throwable
    {
        // given
        db = startDB();
        createAndIndexNode( db, 1 );

        // when
        BackupService backupService = backupService();
        backupService.doFullBackup( BACKUP_HOST, backupPort, backupDir.getAbsoluteFile(), false, new Config(),
                BackupClient.BIG_READ_TIMEOUT, false );
        db.shutdown();

        // then
        assertEquals( DbRepresentation.of( storeDir ), DbRepresentation.of( backupDir ) );
        assertNotEquals( 0, getLastTxChecksum( pageCacheRule.getPageCache( fileSystem ) ) );
    }

    @Test
    public void shouldFindValidPreviousCommittedTxIdInFirstNeoStoreLog() throws Throwable
    {
        // given
        db = startDB();
        createAndIndexNode( db, 1 );
        createAndIndexNode( db, 2 );
        createAndIndexNode( db, 3 );
        createAndIndexNode( db, 4 );

        NeoStore neoStore = db.getDependencyResolver().resolveDependency( NeoStore.class );
        neoStore.flush();
        long txId = neoStore.getLastCommittedTransactionId();

        // when
        BackupService backupService = backupService();
        backupService.doFullBackup( BACKUP_HOST, backupPort, backupDir.getAbsoluteFile(), false, new Config(),
                BackupClient.BIG_READ_TIMEOUT, false );
        db.shutdown();

        // then
        checkPreviousCommittedTxIdFromLog( 0, TransactionIdStore.BASE_TX_ID );
    }

    @Test
    public void shouldFindTransactionLogContainingLastLuceneTransaction() throws Throwable
    {
        // given
        db = startDB();
        createAndIndexNode( db, 1 );

        // when
        BackupService backupService = backupService();
        backupService.doFullBackup( BACKUP_HOST, backupPort, backupDir.getAbsoluteFile(), false, new Config(),
                BackupClient.BIG_READ_TIMEOUT, false );
        db.shutdown();

        // then
        assertEquals( DbRepresentation.of( storeDir ), DbRepresentation.of( backupDir ) );
        assertNotEquals( 0, getLastTxChecksum( pageCacheRule.getPageCache( fileSystem ) ) );
    }

    @Test
    public void shouldGiveHelpfulErrorMessageIfLogsPrunedPastThePointOfNoReturn() throws Exception
    {
        // Given
        // have logs rotated on every transaction
        db = CommunityTestGraphDatabase.build()
                .withParams( defaultParams )
                .withSetting( GraphDatabaseSettings.keep_logical_logs, "0 files" )
                .open( storeDir );
        BackupService backupService = backupService();

        createAndIndexNode( db, 1 );
        rotateAndCheckPoint( db );

        // A full backup
        backupService.doFullBackup( BACKUP_HOST, backupPort, backupDir.getAbsoluteFile(),
                false, new Config(), BackupClient.BIG_READ_TIMEOUT, false );

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
                    false, BackupClient.BIG_READ_TIMEOUT, new Config() );
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
        // have logs rotated on every transaction
        db = CommunityTestGraphDatabase.build()
                .withParams( defaultParams )
                .open( storeDir );
        BackupService backupService = backupService();

        createAndIndexNode( db, 1 );

        // A full backup
        backupService.doFullBackup( BACKUP_HOST, backupPort, backupDir.getAbsoluteFile(),
                false, new Config(), BackupClient.BIG_READ_TIMEOUT, false );

        // And the log the backup uses is rotated out
        createAndIndexNode( db, 2 );
        rotateAndCheckPoint( db );
        createAndIndexNode( db, 3 );
        rotateAndCheckPoint( db );
        createAndIndexNode( db, 4 );
        rotateAndCheckPoint( db );

        // when
        backupService.doIncrementalBackupOrFallbackToFull(
                BACKUP_HOST, backupPort, backupDir.getAbsoluteFile(), false, new Config(),
                BackupClient.BIG_READ_TIMEOUT, false );

        // Then
        db.shutdown();
        assertEquals( DbRepresentation.of( storeDir ), DbRepresentation.of( backupDir ) );
    }

    private void rotateAndCheckPoint( TestGraphDatabase db ) throws IOException
    {
        db.getDependencyResolver().resolveDependency( LogRotation.class ).rotateLogFile();
        db.getDependencyResolver().resolveDependency( CheckPointer.class ).forceCheckPoint();
    }

    @Test
    public void shouldHandleBackupWhenLogFilesHaveBeenDeleted() throws Exception
    {
        // Given
        db = CommunityTestGraphDatabase.build()
                .withParams( defaultParams )
                .open( storeDir );
        BackupService backupService = backupService();

        createAndIndexNode( db, 1 );

        // A full backup
        backupService.doIncrementalBackupOrFallbackToFull(
                BACKUP_HOST, backupPort, backupDir.getAbsoluteFile(), false, new Config( defaultParams ),
                BackupClient.BIG_READ_TIMEOUT, false );

        // And the log the backup uses is rotated out
        createAndIndexNode( db, 2 );
        db = deleteLogFilesAndRestart( db );

        createAndIndexNode( db, 3 );
        db = deleteLogFilesAndRestart( db );

        // when
        backupService.doIncrementalBackupOrFallbackToFull(
                BACKUP_HOST, backupPort, backupDir.getAbsoluteFile(), false, new Config(),
                BackupClient.BIG_READ_TIMEOUT, false );

        // Then
        db.shutdown();
        assertEquals( DbRepresentation.of( storeDir ), DbRepresentation.of( backupDir ) );
    }

    private TestGraphDatabase deleteLogFilesAndRestart( TestGraphDatabase db ) throws IOException
    {
        final FileFilter logFileFilter = new FileFilter()
        {
            @Override
            public boolean accept( File pathname )
            {
                return pathname.getName().contains( "logical" );
            }
        };
        db.shutdown();
        for ( File logFile : storeDir.listFiles( logFileFilter ) )
        {
            logFile.delete();
        }
        return CommunityTestGraphDatabase.build()
                .withParams( defaultParams )
                .open( storeDir );
    }

    @Test
    public void shouldDoFullBackupOnIncrementalFallbackToFullIfNoBackupFolderExists() throws Exception
    {
        // Given
        db = CommunityTestGraphDatabase.build()
                .withParams( defaultParams )
                .open( storeDir );
        BackupService backupService = backupService();

        createAndIndexNode( db, 1 );

        // when
        backupService.doIncrementalBackupOrFallbackToFull(
                BACKUP_HOST, backupPort, backupDir.getAbsoluteFile(), false, new Config(),
                BackupClient.BIG_READ_TIMEOUT, false );

        // then
        db.shutdown();
        assertEquals( DbRepresentation.of( storeDir ), DbRepresentation.of( backupDir ) );
    }

    @Test
    public void shouldContainTransactionsThatHappenDuringBackupProcess() throws Throwable
    {
        // given
        db = CommunityTestGraphDatabase.build()
                .withParams( defaultParams )
                .withSetting( OnlineBackupSettings.online_backup_enabled, "false" )
                .open( storeDir );

        final Barrier.Control barrier = new Barrier.Control();

        createAndIndexNode( db, 1 ); // create some data

        NeoStoreDataSource ds = db.getDependencyResolver().resolveDependency( DataSourceManager.class ).getDataSource();
        long expectedLastTxId = ds.getNeoStore().getLastCommittedTransactionId();

        // This monitor is added server-side...
        monitors.addMonitorListener( new StoreSnoopingMonitor( barrier ) );

        Dependencies dependencies = new Dependencies(db.getDependencyResolver());
        dependencies.satisfyDependencies( new Config( defaultParams ), monitors, NullLogProvider.getInstance() );

        OnlineBackupKernelExtension backup = (OnlineBackupKernelExtension) new OnlineBackupExtensionFactory().newKernelExtension(
                DependenciesProxy.dependencies(dependencies, OnlineBackupExtensionFactory.Dependencies.class));
        backup.start();

        // when
        BackupService backupService =backupService();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.execute( new Runnable()
        {
            @Override
            public void run()
            {
                barrier.awaitUninterruptibly();

                createAndIndexNode( db, 1 );
                db.getDependencyResolver().resolveDependency( NeoStoreSupplier.class ).get().flush();

                barrier.release();
            }
        } );

        BackupService.BackupOutcome backupOutcome = backupService.doFullBackup( BACKUP_HOST, backupPort,
                backupDir.getAbsoluteFile(), true, new Config(), BackupClient.BIG_READ_TIMEOUT, false );

        backup.stop();
        executor.shutdown();
        executor.awaitTermination( 30, TimeUnit.SECONDS );

        // then
        checkPreviousCommittedTxIdFromLog( 0, expectedLastTxId );
        checkLastCommittedTxIdInLogAndNeoStore( expectedLastTxId+1 );
        assertEquals( DbRepresentation.of( db ), DbRepresentation.of( backupDir ) );
        assertTrue( backupOutcome.isConsistent() );
    }

    @Test
    public void incrementalBackupShouldFailWhenTargetDirContainsDifferentStore() throws IOException
    {
        // Given
        db = startDB();
        createAndIndexNode( db, 1 );

        backupService().doFullBackup( BACKUP_HOST, backupPort, backupDir.getAbsoluteFile(), false,
                new Config(), BackupClient.BIG_READ_TIMEOUT, false );

        // When
        db.shutdown();
        deleteAllBackedUpTransactionLogs();
        fileSystem.deleteRecursively( storeDir );
        fileSystem.mkdir( storeDir );

        db = startDB();
        createAndIndexNode( db, 2 );

        try
        {
            backupService().doIncrementalBackupOrFallbackToFull( BACKUP_HOST, backupPort,
                    backupDir.getAbsoluteFile(), false, new Config(), BackupClient.BIG_READ_TIMEOUT, false );

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
                .withWorkingDirectory( backupDir )
                .withBackupAddress( BACKUP_HOST, backupPort )
                .build();

        int brokenStores = callable.call();
        assertEquals( 0, brokenStores );
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

    private void checkLastCommittedTxIdInLogAndNeoStore( long txId ) throws IOException
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
        File neoStore = new File( storeDir, NeoStore.DEFAULT_NAME );
        assertEquals( txId, NeoStore.getRecord( pageCache, neoStore, Position.LAST_TRANSACTION_ID ) );
    }

    private long getLastTxChecksum( PageCache pageCache )
    {
        File neoStore = new File( backupDir, NeoStore.DEFAULT_NAME );
        return NeoStore.getRecord( pageCache, neoStore, Position.LAST_TRANSACTION_CHECKSUM );
    }

    private void deleteAllBackedUpTransactionLogs()
    {
        for ( File log : fileSystem.listFiles( backupDir, LogFiles.FILENAME_FILTER ) )
        {
            fileSystem.deleteFile( log );
        }
    }
}
