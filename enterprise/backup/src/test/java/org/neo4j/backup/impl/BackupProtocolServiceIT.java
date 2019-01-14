/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.backup.impl;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ConnectException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.neo4j.backup.IncrementalBackupNotPossibleException;
import org.neo4j.backup.OnlineBackupExtensionFactory;
import org.neo4j.backup.OnlineBackupKernelExtension;
import org.neo4j.com.storecopy.StoreCopyServer;
import org.neo4j.com.storecopy.StoreUtil;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.cursor.IOCursor;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.mockfs.UncloseableDelegatingFileSystemAbstraction;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.spi.SimpleKernelContext;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.MetaDataStore.Position;
import org.neo4j.kernel.impl.store.MismatchingStoreIdException;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreFile;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.ReadOnlyTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.impl.util.DependenciesProxy;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.Logger;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.ports.allocation.PortAuthority;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StoreFileMetadata;
import org.neo4j.test.Barrier;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.EmbeddedDatabaseRule;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static java.lang.Boolean.TRUE;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.store.StoreFile.COUNTS_STORE_LEFT;
import static org.neo4j.kernel.impl.store.StoreFile.COUNTS_STORE_RIGHT;

public class BackupProtocolServiceIT
{
    private static final String NODE_STORE = StoreFactory.NODE_STORE_NAME;
    private static final String RELATIONSHIP_STORE = StoreFactory.RELATIONSHIP_STORE_NAME;
    private static final String BACKUP_HOST = "localhost";
    private static final OutputStream NULL_OUTPUT = new OutputStream()
    {
        @Override
        public void write( int b )
        {
        }
    };
    private static final String PROP = "id";
    private static final Label LABEL = Label.label( "LABEL" );

    private final Monitors monitors = new Monitors();
    private final IOLimiter limiter = IOLimiter.unlimited();
    private FileSystemAbstraction fileSystem;
    private Path storeDir;
    private Path backupDir;
    private int backupPort = -1;

    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();
    private final TestDirectory target = TestDirectory.testDirectory();
    private final EmbeddedDatabaseRule dbRule = new EmbeddedDatabaseRule().startLazily();
    private final SuppressOutput suppressOutput = SuppressOutput.suppressAll();
    private final PageCacheRule pageCacheRule = new PageCacheRule();

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( fileSystemRule )
                                                .around( target )
                                                .around( dbRule )
                                                .around( pageCacheRule )
                                                .around( suppressOutput );

    @Before
    public void setup()
    {
        fileSystem = fileSystemRule.get();
        backupPort = PortAuthority.allocatePort();
        storeDir = dbRule.getStoreDirFile().toPath();
        backupDir = target.directory( "backup_dir" ).toPath();
    }

    private BackupProtocolService backupService()
    {
        return new BackupProtocolService( () -> new UncloseableDelegatingFileSystemAbstraction( fileSystemRule.get() ),
                FormattedLogProvider.toOutputStream( NULL_OUTPUT ), NULL_OUTPUT, new Monitors(), pageCacheRule.getPageCache( fileSystemRule.get() ) );
    }

    private BackupProtocolService backupService( LogProvider logProvider )
    {
        return new BackupProtocolService( () -> new UncloseableDelegatingFileSystemAbstraction( fileSystemRule.get() ), logProvider, NULL_OUTPUT,
                new Monitors(), pageCacheRule.getPageCache( fileSystemRule.get() ) );
    }

    @Test
    public void performConsistencyCheckAfterIncrementalBackup()
    {
        defaultBackupPortHostParams();
        Config defaultConfig = dbRule.getConfigCopy();

        GraphDatabaseAPI db = dbRule.getGraphDatabaseAPI();
        createSchemaIndex( db );
        createAndIndexNode( db, 1 );

        backupService().doFullBackup( BACKUP_HOST, backupPort, backupDir, ConsistencyCheck.NONE, defaultConfig,
                BackupClient.BIG_READ_TIMEOUT, false );

        createAndIndexNode( db, 1 );
        TestFullConsistencyCheck consistencyCheck = new TestFullConsistencyCheck();
        BackupOutcome backupOutcome = backupService()
                .doIncrementalBackupOrFallbackToFull( BACKUP_HOST, backupPort, backupDir, consistencyCheck,
                        defaultConfig, BackupClient.BIG_READ_TIMEOUT, false );
        assertTrue( "Consistency check invoked for incremental backup, ", consistencyCheck.isChecked() );
        assertTrue( backupOutcome.isConsistent() );
    }

    @Test
    public void shouldPrintThatFullBackupIsPerformed()
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
        createSchemaIndex( db );

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
    public void shouldThrowUsefulMessageWhenCannotConnectDuringFullBackup()
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
    public void shouldThrowUsefulMessageWhenCannotConnectDuringIncrementalBackup()
    {
        defaultBackupPortHostParams();
        GraphDatabaseAPI db = dbRule.getGraphDatabaseAPI();
        createSchemaIndex( db );
        BackupProtocolService backupProtocolService = backupService();

        createAndIndexNode( db, 1 );

        // A full backup
        backupProtocolService.doFullBackup( BACKUP_HOST, backupPort, backupDir,
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
        createSchemaIndex( db );
        createAndIndexNode( db, 1 );

        // Touch a random file
        Files.createFile( backupDir.resolve( ".jibberishfile" ) );

        try
        {
            // when
            backupService().doFullBackup( BACKUP_HOST, backupPort, backupDir,
                    ConsistencyCheck.FULL, dbRule.getConfigCopy(), BackupClient.BIG_READ_TIMEOUT, false );
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
        createSchemaIndex( db );
        createAndIndexNode( db, 1 );

        // Touch a random directory
        Files.createDirectory( backupDir.resolve( "jibberishfolder" ) );

        try
        {
            // when
            backupService().doFullBackup( BACKUP_HOST, backupPort, backupDir,
                    ConsistencyCheck.FULL, dbRule.getConfigCopy(), BackupClient.BIG_READ_TIMEOUT, false );
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
    public void shouldRemoveTempDirectory()
    {
        // given
        defaultBackupPortHostParams();
        GraphDatabaseAPI db = dbRule.getGraphDatabaseAPI();
        createSchemaIndex( db );
        createAndIndexNode( db, 1 );

        // when
        BackupProtocolService backupProtocolService = backupService();
        backupProtocolService.doFullBackup( BACKUP_HOST, backupPort, backupDir, ConsistencyCheck.NONE,
                dbRule.getConfigCopy(), BackupClient.BIG_READ_TIMEOUT, false );
        db.shutdown();

        // then
        assertFalse( "Temp directory was not removed as expected",
                Files.exists( backupDir.resolve( StoreUtil.TEMP_COPY_DIRECTORY_NAME ) ) );
    }

    @Test
    public void shouldCopyStoreFiles() throws Throwable
    {
        // given
        defaultBackupPortHostParams();
        GraphDatabaseAPI db = dbRule.getGraphDatabaseAPI();
        createSchemaIndex( db );
        createAndIndexNode( db, 1 );

        // when
        BackupProtocolService backupProtocolService = backupService();
        backupProtocolService.doFullBackup( BACKUP_HOST, backupPort, backupDir, ConsistencyCheck.NONE,
                dbRule.getConfigCopy(), BackupClient.BIG_READ_TIMEOUT, false );
        db.shutdown();

        // then
        File[] files;
        try ( Stream<Path> listing = Files.list( backupDir ) )
        {
            files = listing.map( Path::toFile ).toArray( File[]::new );
        }

        assertTrue( files.length > 0 );

        for ( final StoreFile storeFile : StoreFile.values() )
        {
            if ( storeFile == COUNTS_STORE_LEFT ||
                 storeFile == COUNTS_STORE_RIGHT )
            {
                assertThat( files, anyOf( hasFile( COUNTS_STORE_LEFT.storeFileName() ),
                                          hasFile( COUNTS_STORE_RIGHT.storeFileName() ) ) );
            }
            else
            {
                assertThat( files, hasFile( storeFile.storeFileName() ) );
            }
        }

        assertEquals( getDbRepresentation(), getBackupDbRepresentation() );
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
        Label markerLabel = Label.label( "marker" );

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
        GraphDatabaseService backupBasedDatabase = new TestGraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( backupDir.toFile() )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
                .newGraphDatabase();
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

            try ( Transaction ignored = backupBasedDatabase.beginTx() )
            {
                Node node = findNodeByLabel( (GraphDatabaseAPI) backupBasedDatabase, markerLabel );
                // newProperty + 10 defined properties.
                assertEquals( "We should be able to see all previously defined properties.",
                        11, Iterables.asList( node.getPropertyKeys() ).size() );
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
        createSchemaIndex( db );

        for ( int i = 0; i < 100; i++ )
        {
            createAndIndexNode( db, i );
        }

        final File oldLog = db.getDependencyResolver().resolveDependency( LogFiles.class ).getHighestLogFile();
        rotateAndCheckPoint( db );

        for ( int i = 0; i < 1; i++ )
        {
            createAndIndexNode( db, i );
        }
        rotateAndCheckPoint( db );

        long lastCommittedTxBefore = db.getDependencyResolver().resolveDependency( TransactionIdStore.class )
                .getLastCommittedTransactionId();

        db = dbRule.restartDatabase( ( fs, storeDirectory ) -> FileUtils.deleteFile( oldLog ) );

        long lastCommittedTxAfter = db.getDependencyResolver().resolveDependency( TransactionIdStore.class )
                .getLastCommittedTransactionId();

        // when
        BackupProtocolService backupProtocolService = backupService();
        BackupOutcome outcome = backupProtocolService.doFullBackup( BACKUP_HOST, backupPort,
                backupDir, ConsistencyCheck.FULL, dbRule.getConfigCopy(),
                BackupClient.BIG_READ_TIMEOUT, false );

        db.shutdown();

        // then
        assertEquals( lastCommittedTxBefore, lastCommittedTxAfter );
        assertTrue( outcome.isConsistent() );
        assertEquals( getDbRepresentation(), getBackupDbRepresentation() );
    }

    @Test
    public void shouldFindTransactionLogContainingLastNeoStoreTransactionInAnEmptyStore() throws IOException
    {
        // This test highlights a special case where an empty store can return transaction metadata for transaction 0.

        // given
        defaultBackupPortHostParams();
        GraphDatabaseAPI db = dbRule.getGraphDatabaseAPI();

        // when
        BackupProtocolService backupProtocolService = backupService();
        backupProtocolService.doFullBackup( BACKUP_HOST, backupPort, backupDir,
                ConsistencyCheck.NONE, dbRule.getConfigCopy(), BackupClient.BIG_READ_TIMEOUT, false );
        db.shutdown();

        // then
        assertEquals( getDbRepresentation(), getBackupDbRepresentation() );

        assertEquals( 0, getLastTxChecksum( pageCacheRule.getPageCache( fileSystem ) ) );
    }

    @Test
    public void shouldFindTransactionLogContainingLastNeoStoreTransaction() throws Throwable
    {
        // given
        defaultBackupPortHostParams();
        GraphDatabaseAPI db = dbRule.getGraphDatabaseAPI();
        createSchemaIndex( db );
        createAndIndexNode( db, 1 );

        // when
        BackupProtocolService backupProtocolService = backupService();
        backupProtocolService.doFullBackup( BACKUP_HOST, backupPort, backupDir,
                ConsistencyCheck.NONE, dbRule.getConfigCopy(), BackupClient.BIG_READ_TIMEOUT, false );
        db.shutdown();

        // then
        assertEquals( getDbRepresentation(), getBackupDbRepresentation() );
        assertNotEquals( 0, getLastTxChecksum( pageCacheRule.getPageCache( fileSystem ) ) );
    }

    @Test
    public void shouldFindValidPreviousCommittedTxIdInFirstNeoStoreLog() throws Throwable
    {
        // given
        defaultBackupPortHostParams();
        GraphDatabaseAPI db = dbRule.getGraphDatabaseAPI();
        createSchemaIndex( db );
        createAndIndexNode( db, 1 );
        createAndIndexNode( db, 2 );
        createAndIndexNode( db, 3 );
        createAndIndexNode( db, 4 );

        db.getDependencyResolver().resolveDependency( StorageEngine.class ).flushAndForce( limiter );
        long txId = db.getDependencyResolver().resolveDependency( TransactionIdStore.class )
                .getLastCommittedTransactionId();

        // when
        BackupProtocolService backupProtocolService = backupService();
        backupProtocolService.doFullBackup( BACKUP_HOST, backupPort, backupDir,
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
        createSchemaIndex( db );
        createAndIndexNode( db, 1 );

        // when
        BackupProtocolService backupProtocolService = backupService();
        backupProtocolService.doFullBackup( BACKUP_HOST, backupPort, backupDir,
                ConsistencyCheck.NONE, defaultConfig, BackupClient.BIG_READ_TIMEOUT, false );
        db.shutdown();

        // then
        assertEquals( getDbRepresentation(), getBackupDbRepresentation() );
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
        createSchemaIndex( db );
        BackupProtocolService backupProtocolService = backupService();

        createAndIndexNode( db, 1 );
        rotateAndCheckPoint( db );

        // A full backup
        backupProtocolService.doFullBackup( BACKUP_HOST, backupPort, backupDir,
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
            backupProtocolService.doIncrementalBackup( BACKUP_HOST, backupPort, backupDir,
                    ConsistencyCheck.NONE, BackupClient.BIG_READ_TIMEOUT, defaultConfig );
            fail( "Should have thrown exception." );
        }
        // Then
        catch ( IncrementalBackupNotPossibleException e )
        {
            assertThat( e.getMessage(), equalTo( BackupProtocolService.TOO_OLD_BACKUP ) );
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
        createSchemaIndex( db );
        BackupProtocolService backupProtocolService = backupService();

        createAndIndexNode( db, 1 );

        // A full backup
        backupProtocolService.doFullBackup( BACKUP_HOST, backupPort, backupDir,
                ConsistencyCheck.NONE, defaultConfig, BackupClient.BIG_READ_TIMEOUT, false );

        // And the log the backup uses is rotated out
        createAndIndexNode( db, 2 );
        rotateAndCheckPoint( db );
        createAndIndexNode( db, 3 );
        rotateAndCheckPoint( db );
        createAndIndexNode( db, 4 );
        rotateAndCheckPoint( db );

        // when
        backupProtocolService.doIncrementalBackupOrFallbackToFull(
                BACKUP_HOST, backupPort, backupDir, ConsistencyCheck.NONE, defaultConfig,
                BackupClient.BIG_READ_TIMEOUT, false );

        // Then
        db.shutdown();
        assertEquals( getDbRepresentation(), getBackupDbRepresentation() );
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
        createSchemaIndex( db );
        BackupProtocolService backupProtocolService = backupService();

        createAndIndexNode( db, 1 );

        // A full backup
        backupProtocolService.doIncrementalBackupOrFallbackToFull(
                BACKUP_HOST, backupPort, backupDir, ConsistencyCheck.NONE, defaultConfig,
                BackupClient.BIG_READ_TIMEOUT, false );

        // And the log the backup uses is rotated out
        createAndIndexNode( db, 2 );
        db = deleteLogFilesAndRestart();

        createAndIndexNode( db, 3 );
        db = deleteLogFilesAndRestart();

        // when
        backupProtocolService.doIncrementalBackupOrFallbackToFull(
                BACKUP_HOST, backupPort, backupDir, ConsistencyCheck.NONE, defaultConfig,
                BackupClient.BIG_READ_TIMEOUT, false );

        // Then
        db.shutdown();
        assertEquals( getDbRepresentation(), getBackupDbRepresentation() );
    }

    private GraphDatabaseAPI deleteLogFilesAndRestart()
            throws IOException
    {
        List<File> logFiles = new ArrayList<>();
        NeoStoreDataSource dataSource = dbRule.resolveDependency( NeoStoreDataSource.class );
        try ( ResourceIterator<StoreFileMetadata> files = dataSource.listStoreFiles( true ) )
        {
            files.stream().filter( StoreFileMetadata::isLogFile )
                 .map( StoreFileMetadata::file )
                 .forEach( logFiles::add );
        }
        return dbRule.restartDatabase( ( fs, storeDirectory ) ->
        {
            for ( File logFile : logFiles )
            {
                fs.deleteFile( logFile );
            }
        } );
    }

    @Test
    public void backupShouldWorkWithReadOnlySourceDatabases() throws Exception
    {
        // Create some data
        defaultBackupPortHostParams();
        GraphDatabaseAPI db = dbRule.getGraphDatabaseAPI();
        createSchemaIndex( db );
        createAndIndexNode( db, 1 );

        // Make it read-only
        db = dbRule.restartDatabase( GraphDatabaseSettings.read_only.name(), TRUE.toString() );

        // Take a backup
        Config defaultConfig = dbRule.getConfigCopy();
        BackupProtocolService backupProtocolService = backupService();
        backupProtocolService.doFullBackup( BACKUP_HOST, backupPort, backupDir, ConsistencyCheck.FULL,
                defaultConfig, BackupClient.BIG_READ_TIMEOUT, false );

        // Then
        db.shutdown();
        assertEquals( getDbRepresentation(), getBackupDbRepresentation() );
    }

    @Test
    public void shouldDoFullBackupOnIncrementalFallbackToFullIfNoBackupFolderExists()
    {
        // Given
        defaultBackupPortHostParams();
        Config defaultConfig = dbRule.getConfigCopy();
        dbRule.setConfig( GraphDatabaseSettings.keep_logical_logs, "false" );
        GraphDatabaseAPI db = dbRule.getGraphDatabaseAPI();
        createSchemaIndex( db );
        BackupProtocolService backupProtocolService = backupService();

        createAndIndexNode( db, 1 );

        // when
        backupProtocolService.doIncrementalBackupOrFallbackToFull(
                BACKUP_HOST, backupPort, backupDir, ConsistencyCheck.NONE, defaultConfig,
                BackupClient.BIG_READ_TIMEOUT, false );

        // then
        db.shutdown();
        assertEquals( getDbRepresentation(), getBackupDbRepresentation() );
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
        createSchemaIndex( db );

        createAndIndexNode( db, 1 ); // create some data

        final DependencyResolver resolver = db.getDependencyResolver();
        long expectedLastTxId = resolver.resolveDependency( TransactionIdStore.class ).getLastClosedTransactionId();

        // This monitor is added server-side...
        monitors.addMonitorListener( new StoreSnoopingMonitor( barrier ) );

        Dependencies dependencies = new Dependencies( resolver );
        dependencies.satisfyDependencies( defaultConfig, monitors, NullLogProvider.getInstance() );

        OnlineBackupKernelExtension backup = (OnlineBackupKernelExtension)
                new OnlineBackupExtensionFactory().newInstance(
                        new SimpleKernelContext( storeDir.toFile(), DatabaseInfo.UNKNOWN, dependencies ),
                        DependenciesProxy.dependencies( dependencies, OnlineBackupExtensionFactory.Dependencies.class )
                );
        backup.start();

        // when
        BackupProtocolService backupProtocolService = backupService();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.execute( () ->
        {
            barrier.awaitUninterruptibly();

            createAndIndexNode( db, 1 );
            resolver.resolveDependency( StorageEngine.class ).flushAndForce( limiter );

            barrier.release();
        } );

        BackupOutcome backupOutcome = backupProtocolService.doFullBackup( BACKUP_HOST, backupPort,
                backupDir, ConsistencyCheck.FULL, withOnlineBackupDisabled,
                BackupClient.BIG_READ_TIMEOUT, false );

        backup.stop();
        executor.shutdown();
        executor.awaitTermination( 30, TimeUnit.SECONDS );

        // then
        checkPreviousCommittedTxIdFromLog( 0, expectedLastTxId );
        Path neoStore = storeDir.resolve( MetaDataStore.DEFAULT_NAME );
        PageCache pageCache = resolver.resolveDependency( PageCache.class );
        long txIdFromOrigin = MetaDataStore.getRecord( pageCache, neoStore.toFile(), Position.LAST_TRANSACTION_ID );
        checkLastCommittedTxIdInLogAndNeoStore( expectedLastTxId + 1, txIdFromOrigin );
        assertEquals( DbRepresentation.of( db ), getBackupDbRepresentation() );
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

        OnlineBackupKernelExtension backup = (OnlineBackupKernelExtension)
                new OnlineBackupExtensionFactory().newInstance(
                        new SimpleKernelContext( storeDir.toFile(), DatabaseInfo.UNKNOWN, dependencies ),
                        DependenciesProxy.dependencies( dependencies, OnlineBackupExtensionFactory.Dependencies.class )
                );
        try
        {
            backup.start();

            // when
            backupService()
                    .doFullBackup( BACKUP_HOST, backupPort, backupDir, ConsistencyCheck.NONE, withOnlineBackupDisabled,
                            BackupClient.BIG_READ_TIMEOUT, false );

            // then
            verify( logger ).log( eq( "%s: Full backup started...") , Mockito.startsWith( "BackupServer" ) );
            verify( logger ).log( eq( "%s: Full backup finished." ), Mockito.startsWith( "BackupServer" ) );

            // when
            createAndIndexNode( dbRule, 2 );

            backupService().doIncrementalBackupOrFallbackToFull( BACKUP_HOST, backupPort, backupDir, ConsistencyCheck.NONE,
                    withOnlineBackupDisabled, BackupClient.BIG_READ_TIMEOUT, false );

            // then
            verify( logger ).log( eq( "%s: Incremental backup started..."), Mockito.startsWith( "BackupServer" ) );
            verify( logger ).log( eq( "%s: Incremental backup finished." ), Mockito.startsWith( "BackupServer" ) );
        }
        finally
        {
            backup.stop();
        }
    }

    @Test
    public void incrementalBackupShouldFailWhenTargetDirContainsDifferentStore() throws IOException
    {
        // Given
        defaultBackupPortHostParams();
        Config defaultConfig = dbRule.getConfigCopy();
        GraphDatabaseAPI db1 = dbRule.getGraphDatabaseAPI();
        createSchemaIndex( db1 );
        createAndIndexNode( db1, 1 );

        backupService().doFullBackup( BACKUP_HOST, backupPort, backupDir, ConsistencyCheck.NONE,
                defaultConfig, BackupClient.BIG_READ_TIMEOUT, false );

        // When
        GraphDatabaseAPI db2 = dbRule.restartDatabase( ( fs, storeDirectory ) ->
        {
            deleteAllBackedUpTransactionLogs();
            FileUtils.deletePathRecursively( storeDir );
            Files.createDirectory( storeDir );
        } );
        createAndIndexNode( db2, 2 );

        try
        {
            backupService().doIncrementalBackupOrFallbackToFull( BACKUP_HOST, backupPort,
                    backupDir, ConsistencyCheck.NONE, defaultConfig,
                    BackupClient.BIG_READ_TIMEOUT, false );

            fail( "Should have thrown exception about mismatching store ids" );
        }
        catch ( RuntimeException e )
        {
            // Then
            assertThat( e.getMessage(), equalTo( BackupProtocolService.DIFFERENT_STORE_MESSAGE ) );
            assertThat( e.getCause(), instanceOf( MismatchingStoreIdException.class ) );
        }
    }

    private void defaultBackupPortHostParams()
    {
        dbRule.setConfig( OnlineBackupSettings.online_backup_server, BACKUP_HOST + ":" + backupPort );
    }

    private void createSchemaIndex( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( LABEL ).on( PROP ).create();
            tx.success();
        }
        try ( Transaction ignore = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
        }
    }

    private void createAndIndexNode( GraphDatabaseService db, int i )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Index<Node> index = db.index().forNodes( "delete_me" );
            Node node = db.createNode();
            node.setProperty( PROP, System.currentTimeMillis() + i );
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
        LogFiles logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( backupDir.toFile(), fileSystem ).build();
        LogHeader logHeader = LogHeaderReader.readLogHeader( fileSystem, logFiles.getLogFileForVersion( logVersion ) );
        assertEquals( txId, logHeader.lastCommittedTxId );
    }

    private void checkLastCommittedTxIdInLogAndNeoStore( long txId, long txIdFromOrigin ) throws Exception
    {
        // Assert last committed transaction can be found in tx log and is the last tx in the log
        LifeSupport life = new LifeSupport();
        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        LogicalTransactionStore transactionStore = life.add( new ReadOnlyTransactionStore(
                pageCache, fileSystem, backupDir.toFile(), Config.defaults(), monitors ) );
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
        Path neoStore = backupDir.resolve( MetaDataStore.DEFAULT_NAME );
        return MetaDataStore.getRecord( pageCache, neoStore.toFile(), Position.LAST_TRANSACTION_CHECKSUM );
    }

    private void deleteAllBackedUpTransactionLogs() throws IOException
    {
        LogFiles logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( backupDir.toFile(), fileSystem ).build();
        for ( File log : logFiles.logFiles() )
        {
            fileSystem.deleteFile( log );
        }
    }

    private void doIncrementalBackupOrFallbackToFull()
    {
        BackupProtocolService backupProtocolService = backupService();
        backupProtocolService.doIncrementalBackupOrFallbackToFull( BACKUP_HOST, backupPort,
                backupDir, ConsistencyCheck.NONE, Config.defaults(), BackupClient.BIG_READ_TIMEOUT, false );
    }

    private Node findNodeByLabel( GraphDatabaseAPI graphDatabase, Label label )
    {
        try ( ResourceIterator<Node> nodes = graphDatabase.findNodes( label ) )
        {
            return nodes.next();
        }
    }

    private DbRepresentation getBackupDbRepresentation()
    {
        Config config = Config.defaults( OnlineBackupSettings.online_backup_enabled, Settings.FALSE );
        return DbRepresentation.of( backupDir.toFile(), config );
    }

    private DbRepresentation getDbRepresentation()
    {
        Config config = Config.defaults( OnlineBackupSettings.online_backup_enabled, Settings.FALSE );
        return DbRepresentation.of( storeDir.toFile(), config );
    }

    private static final class StoreSnoopingMonitor extends StoreCopyServer.Monitor.Adapter
    {
        private final Barrier barrier;

        private StoreSnoopingMonitor( Barrier barrier )
        {
            this.barrier = barrier;
        }

        @Override
        public void finishStreamingStoreFile( File storefile, String storeCopyIdentifier )
        {
            if ( storefile.getAbsolutePath().contains( NODE_STORE ) ||
                    storefile.getAbsolutePath().contains( RELATIONSHIP_STORE ) )
            {
                barrier.reached(); // multiple calls to this barrier will not block
            }
        }
    }

    private static class TestFullConsistencyCheck implements ConsistencyCheck
    {
        private boolean checked;
        @Override
        public String name()
        {
            return "testFull";
        }

        @Override
        public boolean runFull( Path storeDir, Config tuningConfiguration, ProgressMonitorFactory progressFactory,
                LogProvider logProvider, FileSystemAbstraction fileSystem, PageCache pageCache, boolean verbose,
                ConsistencyFlags consistencyFlags ) throws ConsistencyCheckFailedException
        {
            markAsChecked();
            return ConsistencyCheck.FULL.runFull( storeDir, tuningConfiguration, progressFactory, logProvider,
                    fileSystem, pageCache, verbose,
                    consistencyFlags );
        }

        private void markAsChecked()
        {
            checked = true;
        }

        boolean isChecked()
        {
            return checked;
        }
    }
}
