/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import junit.framework.AssertionFailedError;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.net.ConnectException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.com.storecopy.StoreCopyServer;
import org.neo4j.com.storecopy.StoreUtil;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.cursor.IOCursor;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.Index;
import org.neo4j.helpers.OptionalHostnamePort;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
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
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.Logger;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.ports.allocation.PortAuthority;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.test.Barrier;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.causalclustering.ClusterRule;
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
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.store.StoreFile.COUNTS_STORE_LEFT;
import static org.neo4j.kernel.impl.store.StoreFile.COUNTS_STORE_RIGHT;

@RunWith( Parameterized.class )
public class BackupStrategyWrapperIT
{
    private static final String NODE_STORE = StoreFactory.NODE_STORE_NAME;
    private static final String RELATIONSHIP_STORE = StoreFactory.RELATIONSHIP_STORE_NAME;
    private static final String BACKUP_HOST = "localhost";
    private static final String PROP = "id";
    private static final Label LABEL = Label.label( "LABEL" );

    private final Monitors monitors = new Monitors();
    private final IOLimiter limiter = IOLimiter.unlimited();

    // helpers

    private FileSystemAbstraction fileSystem;
    private File storeDir;
    private File backupDir;
    private int backupPort = -1;

    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();
    private final TestDirectory target = TestDirectory.testDirectory();
    private final EmbeddedDatabaseRule dbRule = new EmbeddedDatabaseRule().startLazily();
    private final ClusterRule clusterRule = new ClusterRule( BackupStrategyWrapperIT.class );
    private final SuppressOutput suppressOutput = SuppressOutput.suppressAll();
    private final PageCacheRule pageCacheRule = new PageCacheRule();

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( fileSystemRule ).around( target ).around( dbRule ).around( pageCacheRule ).around( suppressOutput );

    @Parameterized.Parameter
    public BackupStrategyCoordinatorBuilder.StrategyEnum strategyEnum;

    @Parameterized.Parameters( name = "{0}" )
    public static List<BackupStrategyCoordinatorBuilder.StrategyEnum> getStrategies()
    {
//        return Arrays.asList( BackupStrategyCoordinatorBuilder.StrategyEnum.values() );
//        return Arrays.asList( BackupStrategyCoordinatorBuilder.StrategyEnum.CC );
        return Arrays.asList( BackupStrategyCoordinatorBuilder.StrategyEnum.HA );
    }

    private BackupStrategyCoordinatorBuilder backupStrategyCoordinatorBuilder = new BackupStrategyCoordinatorBuilder();

    @Before
    public void setup()
    {
        fileSystem = fileSystemRule.get();
        backupPort = PortAuthority.allocatePort();
        storeDir = dbRule.getStoreDirFile();
        backupDir = target.directory( "backup_dir" + strategyEnum );
    }

    @Test
    public void performConsistencyCheckAfterIncrementalBackup() throws CommandFailed
    {
        // builder setup
        ConsistencyCheckServiceSpy consistencyCheckService = new ConsistencyCheckServiceSpy();
        Config defaultConfig = dbRule.getConfigCopy();
        backupStrategyCoordinatorBuilder.withConfig( defaultConfig );
        OptionalHostnamePort hostnamePort = new OptionalHostnamePort( BACKUP_HOST, backupPort, null );
        backupStrategyCoordinatorBuilder.withOptionalHostnamePort( hostnamePort )
                .withConsistencyCheckService( consistencyCheckService )
                .withConsistencyCheck( true );

        // builder init
        BackupStrategy backupStrategy = strategyEnum.getSolution().apply( backupStrategyCoordinatorBuilder );
        BackupStrategyCoordinator backupStrategyCoordinator = backupStrategyCoordinatorBuilder.fromSingleStrategy( backupStrategy );

        GraphDatabaseAPI db = startDb( backupPort );
        createSchemaIndex( db );
        createAndIndexNode( db, 1 );

        backupStrategy.performFullBackup( backupDir, defaultConfig, hostnamePort );
        createAndIndexNode( db, 1 );
        backupStrategyCoordinatorBuilder.withBackupDirectory( backupDir.toPath() );
        backupStrategyCoordinator.performBackup( backupStrategyCoordinatorBuilder.getOnlineBackupContext() );
        backupStrategy.performIncrementalBackup( backupDir, defaultConfig, hostnamePort );
        assertTrue( "Consistency check invoked for incremental backup, ", consistencyCheckService.isChecked() );
    }

    @Test
    public void shouldPrintThatFullBackupIsPerformed() throws Exception // TODO doesnt need network
    {
        // given a way to capture logs
        final Log log = mock( Log.class );
        backupStrategyCoordinatorBuilder.withLogProvider( logProviderForMock( log ) );

        // and config is set up as expected
        Config config = dbRule.getConfigCopy();
        backupStrategyCoordinatorBuilder.withConfig( config );

        // and expected backup parameters are set
        OptionalHostnamePort optionalHostnamePort = validBackupAddress();
        backupStrategyCoordinatorBuilder.withFallbackToFull( true )
                .withOptionalHostnamePort( optionalHostnamePort )
                .withBackupDirectory( backupDir.toPath() )
                .withTimeout( BackupClient.BIG_READ_TIMEOUT );

        // when backup is performed
        GraphDatabaseService db = startDb( backupPort );
        backupStrategyCoordinatorBuilder.fromSingleStrategy( strategyEnum ).performBackup( backupStrategyCoordinatorBuilder.getOnlineBackupContext() );

        // then fallback to full was logged
        verify( log ).info( "Previous backup not found, a new full backup will be performed." );
    }

    @Test
    public void shouldPrintThatIncrementalBackupIsPerformedAndFallingBackToFull() throws Exception
    {
        //given backup is setup as expected
        OptionalHostnamePort optionalHostnamePort = new OptionalHostnamePort( BACKUP_HOST, backupPort, null );
        Config defaultConfig = dbRule.getConfigCopy();
        dbRule.setConfig( GraphDatabaseSettings.keep_logical_logs, "false" );
        backupStrategyCoordinatorBuilder.withConfig( defaultConfig )
                .withOptionalHostnamePort( optionalHostnamePort )
                .withBackupDirectory( backupDir.toPath() )
                .withTimeout( BackupClient.BIG_READ_TIMEOUT )
                .withFallbackToFull( true );

        // and we have a way of recording logs
        final Log log = mock( Log.class );
        backupStrategyCoordinatorBuilder.withLogProvider( logProviderForMock( log ) );

        // and logs rotated on every transaction
        GraphDatabaseAPI db = startDb( backupPort );
        createSchemaIndex( db );
        createAndIndexNode( db, 1 );

        // when we perform full backup
        BackupStrategy backupStrategy = strategyEnum.getSolution().apply( backupStrategyCoordinatorBuilder );
        BackupStrategyCoordinator backupStrategyCoordinator = backupStrategyCoordinatorBuilder.fromSingleStrategy( backupStrategy );
        backupStrategy.performFullBackup( backupDir, defaultConfig, optionalHostnamePort );

        // And the log the backup uses is rotated out
        createAndIndexNode( db, 2 );
        rotateAndCheckPoint( db );
        createAndIndexNode( db, 3 );
        rotateAndCheckPoint( db );
        createAndIndexNode( db, 4 );
        rotateAndCheckPoint( db );

        backupStrategyCoordinator.performBackup( backupStrategyCoordinatorBuilder.getOnlineBackupContext() );
        // TODO this is failing due to unupdated transaction id translating to outcome = correct strategy failed
        verify( log ).info( "Previous backup found, trying incremental backup." );
        verify( log ).info( "Existing backup is too far out of date, a new full backup will be performed." );
    }

    @Test
    public void shouldThrowUsefulMessageWhenCannotConnectDuringFullBackup() throws Exception
    {
        // given backup is configured
        OptionalHostnamePort hostnamePort = new OptionalHostnamePort( BACKUP_HOST, 56789, null );
        backupStrategyCoordinatorBuilder.withBackupDirectory( backupDir.toPath() )
                .withOptionalHostnamePort( hostnamePort )
                .withTimeout( BackupClient.BIG_READ_TIMEOUT )
                .withConfig( dbRule.getConfigCopy() );
        BackupStrategy backupStrategy = strategyEnum.getSolution().apply( backupStrategyCoordinatorBuilder );

        // when
        PotentiallyErroneousState<BackupStageOutcome> outcome = backupStrategy.performFullBackup( backupDir, dbRule.getConfigCopy(), hostnamePort );
        Throwable e = outcome.getCause().orElseThrow( () -> new AssertionFailedError( "No exception thrown" ) );
        assertThat( e.getMessage(), containsString( "BackupClient could not connect" ) );
        assertThat( e.getCause(), instanceOf( ConnectException.class ) );
    }

    @Test
    public void shouldThrowUsefulMessageWhenCannotConnectDuringIncrementalBackup() throws Exception
    {
        // given backup is configured
        OptionalHostnamePort hostnamePort = new OptionalHostnamePort( BACKUP_HOST, backupPort, null );
        backupStrategyCoordinatorBuilder.withBackupDirectory( backupDir.toPath() )
                .withTimeout( BackupClient.BIG_READ_TIMEOUT )
                .withConfig( dbRule.getConfigCopy() );

        GraphDatabaseAPI db = startDb( backupPort );
        createSchemaIndex( db );
        BackupStrategy backupStrategy = strategyEnum.getSolution().apply( backupStrategyCoordinatorBuilder );

        createAndIndexNode( db, 1 );

        // and a full backup exists
        backupStrategy.performFullBackup( backupDir, dbRule.getConfigCopy(), hostnamePort );

        // when an incremental backup is performed against a non existing db
        hostnamePort = new OptionalHostnamePort( hostnamePort.getHostname(), Optional.of( 56789 ), Optional.empty() );
        PotentiallyErroneousState<BackupStageOutcome> outcome = backupStrategy.performIncrementalBackup( backupDir, dbRule.getConfigCopy(), hostnamePort );
        assertEquals( BackupStageOutcome.FAILURE, outcome.getState() );
        Throwable e = outcome.getCause().orElseThrow( () -> new AssertionFailedError( "No exception thrown" ) );
        assertThat( e.getMessage(), containsString( "BackupClient could not connect" ) );
        assertThat( e.getCause(), instanceOf( ConnectException.class ) );
    }

    @Test
    public void shouldThrowExceptionWhenDoingFullBackupWhenDirectoryHasSomeFiles() throws Exception
    {
        // given
        OptionalHostnamePort optionalHostnamePort = new OptionalHostnamePort( BACKUP_HOST, backupPort, null );
        backupStrategyCoordinatorBuilder.withConfig( dbRule.getConfigCopy() ).withOptionalHostnamePort( optionalHostnamePort );

        // and db is live
        GraphDatabaseAPI db = startDb( backupPort );
        createSchemaIndex( db );
        createAndIndexNode( db, 1 );

        // Touch a random file
        assertTrue( new File( backupDir, ".jibberishfile" ).createNewFile() );

        try
        {
            // when full backup is performed
            strategyEnum.getSolution().apply( backupStrategyCoordinatorBuilder ).performFullBackup( backupDir, dbRule.getConfigCopy(), optionalHostnamePort );
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
        GraphDatabaseAPI db = startDb( backupPort );
        OptionalHostnamePort optionalHostnamePort = new OptionalHostnamePort( BACKUP_HOST, backupPort, null );
        backupStrategyCoordinatorBuilder.withConfig( dbRule.getConfigCopy() )
                .withOptionalHostnamePort( optionalHostnamePort )
                .withBackupDirectory( backupDir.toPath() )
                .withTimeout( BackupClient.BIG_READ_TIMEOUT );

        // and rotations will happen ?
        createSchemaIndex( db );
        createAndIndexNode( db, 1 );

        // Touch a random directory
        assertTrue( new File( backupDir, "jibberishfolder" ).mkdir() );

        try
        {
            // when
            strategyEnum.getSolution().apply( backupStrategyCoordinatorBuilder ).performFullBackup( backupDir, dbRule.getConfigCopy(), optionalHostnamePort );
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
        GraphDatabaseAPI db = startDb( backupPort );
        OptionalHostnamePort optionalHostnamePort = new OptionalHostnamePort( BACKUP_HOST, backupPort, null );
        backupStrategyCoordinatorBuilder.withConfig( dbRule.getConfigCopy() )
                .withOptionalHostnamePort( optionalHostnamePort )
                .withBackupDirectory( backupDir.toPath() )
                .withTimeout( BackupClient.BIG_READ_TIMEOUT );

        createSchemaIndex( db );
        createAndIndexNode( db, 1 );

        // when
        strategyEnum.getSolution().apply( backupStrategyCoordinatorBuilder ).performFullBackup( backupDir, dbRule.getConfigCopy(), optionalHostnamePort );
        db.shutdown();

        // then
        assertFalse( "Temp directory was not removed as expected", fileSystem.fileExists( new File( backupDir, StoreUtil.TEMP_COPY_DIRECTORY_NAME ) ) );
    }

    @Test
    public void shouldCopyStoreFiles() throws Throwable
    {
        // given
        GraphDatabaseAPI db = startDb( backupPort );
        OptionalHostnamePort optionalHostnamePort = new OptionalHostnamePort( BACKUP_HOST, backupPort, null );
        backupStrategyCoordinatorBuilder.withConfig( dbRule.getConfigCopy() )
                .withOptionalHostnamePort( optionalHostnamePort )
                .withBackupDirectory( backupDir.toPath() )
                .withTimeout( BackupClient.BIG_READ_TIMEOUT )
                .withFallbackToFull( true );

        createSchemaIndex( db );
        createAndIndexNode( db, 1 );

        // when
        backupStrategyCoordinatorBuilder.fromSingleStrategy( strategyEnum ).performBackup( backupStrategyCoordinatorBuilder.getOnlineBackupContext() );
        db.shutdown();

        // then
        File[] files = fileSystem.listFiles( backupDir );

        assertTrue( files.length > 0 );

        for ( final StoreFile storeFile : StoreFile.values() )
        {
            if ( storeFile == COUNTS_STORE_LEFT || storeFile == COUNTS_STORE_RIGHT )
            {
                assertThat( files, anyOf( hasFile( COUNTS_STORE_LEFT.storeFileName() ), hasFile( COUNTS_STORE_RIGHT.storeFileName() ) ) );
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
    public void incrementallyBackupDatabaseShouldNotKeepGeneratedIdFiles() throws CommandFailed
    {
        OptionalHostnamePort optionalHostnamePort = new OptionalHostnamePort( BACKUP_HOST, backupPort, null );
        backupStrategyCoordinatorBuilder.withConfig( dbRule.getConfigCopy() )
                .withFallbackToFull( true )
                .withBackupDirectory( backupDir.toPath() )
                .withOptionalHostnamePort( optionalHostnamePort );
        GraphDatabaseAPI graphDatabase = startDb( backupPort );
        Label markerLabel = Label.label( "marker" );

        createNodeWithMarker( graphDatabase, markerLabel );
        createNodeProperties( graphDatabase, markerLabel, 0, 10 );

        // propagate to backup node and properties
        backupStrategyCoordinatorBuilder.fromSingleStrategy( strategyEnum ).performBackup( backupStrategyCoordinatorBuilder.getOnlineBackupContext() );

        // removing properties will free couple of ids that will be reused during next properties creation
        removePropertiesFromNodes( graphDatabase, markerLabel );

        // propagate removed properties
        backupStrategyCoordinatorBuilder.fromSingleStrategy( strategyEnum ).performBackup( backupStrategyCoordinatorBuilder.getOnlineBackupContext() );

        createNodeProperties( graphDatabase, markerLabel, 10, 16 );

        // propagate to backup new properties with reclaimed ids
        backupStrategyCoordinatorBuilder.fromSingleStrategy( strategyEnum ).performBackup( backupStrategyCoordinatorBuilder.getOnlineBackupContext() );

        // it should be possible to at this point to start db based on our backup and create couple of properties
        // their ids should not clash with already existing
        GraphDatabaseService backupBasedDatabase = new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( backupDir.getAbsoluteFile() )
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
                assertEquals( "We should be able to see all previously defined properties.", 11, Iterables.asList( node.getPropertyKeys() ).size() );
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
        final GraphDatabaseAPI db = startDb( backupPort );
        createSchemaIndex( db );

        // and backup params are configured
        OptionalHostnamePort optionalHostnamePort = validBackupAddress();
        backupStrategyCoordinatorBuilder.withOptionalHostnamePort( optionalHostnamePort )
                .withConfig( dbRule.getConfigCopy() )
                .withConsistencyCheck( true )
                .withBackupDirectory( backupDir.toPath() )
                .withFallbackToFull( true )
                .withTimeout( BackupClient.BIG_READ_TIMEOUT );

        // and
        IntStream.range( 0, 100 ).forEach( i -> createAndIndexNode( db, i ) );

        final File oldLog = db.getDependencyResolver().resolveDependency( LogFiles.class ).getHighestLogFile();
        rotateAndCheckPoint( db );

        createAndIndexNode( db, 0 );
        rotateAndCheckPoint( db );

        long lastCommittedTxBefore = db.getDependencyResolver().resolveDependency( TransactionIdStore.class ).getLastCommittedTransactionId();

        GraphDatabaseAPI restartedDb = dbRule.restartDatabase( ( fs, storeDirectory ) -> FileUtils.deleteFile( oldLog ) ); // TODO
        long lastCommittedTxAfter = restartedDb.getDependencyResolver().resolveDependency( TransactionIdStore.class ).getLastCommittedTransactionId();

        // when
        backupStrategyCoordinatorBuilder.fromSingleStrategy( strategyEnum ).performBackup( backupStrategyCoordinatorBuilder.getOnlineBackupContext() );
        restartedDb.shutdown();

        // then
        assertEquals( lastCommittedTxBefore, lastCommittedTxAfter );
//        assertTrue( outcome.isConsistent() ); TODO
        assertEquals( getDbRepresentation(), getBackupDbRepresentation() );
    }

    @Test
    public void shouldFindTransactionLogContainingLastNeoStoreTransactionInAnEmptyStore() throws IOException
    {
        // This test highlights a special case where an empty store can return transaction metadata for transaction 0.

        // given
        GraphDatabaseAPI db = startDb( backupPort );
        OptionalHostnamePort optionalHostnamePort = new OptionalHostnamePort( BACKUP_HOST, backupPort, null );
        backupStrategyCoordinatorBuilder.withConfig( dbRule.getConfigCopy() ).withOptionalHostnamePort( optionalHostnamePort );

        // when
        strategyEnum.getSolution().apply( backupStrategyCoordinatorBuilder ).performFullBackup( backupDir, dbRule.getConfigCopy(), optionalHostnamePort );
        db.shutdown();

        // then
        assertEquals( getDbRepresentation(), getBackupDbRepresentation() );

        assertEquals( 0, getLastTxChecksum( pageCacheRule.getPageCache( fileSystem ) ) );
    }

    @Test
    public void shouldFindTransactionLogContainingLastNeoStoreTransaction() throws Throwable
    {
        // given
        GraphDatabaseAPI db = startDb( backupPort );
        createSchemaIndex( db );
        createAndIndexNode( db, 1 );

        // and backup set up correctly
        OptionalHostnamePort optionalHostnamePort = new OptionalHostnamePort( BACKUP_HOST, backupPort, null );
        backupStrategyCoordinatorBuilder.withOptionalHostnamePort( optionalHostnamePort )
                .withBackupDirectory( backupDir.toPath() )
                .withConfig( dbRule.getConfigCopy() );

        // when
        strategyEnum.getSolution().apply( backupStrategyCoordinatorBuilder ).performFullBackup( backupDir, dbRule.getConfigCopy(), optionalHostnamePort );
        db.shutdown();

        // then
        assertEquals( getDbRepresentation(), getBackupDbRepresentation() );
        assertNotEquals( 0, getLastTxChecksum( pageCacheRule.getPageCache( fileSystem ) ) );
    }

    @Test
    public void shouldFindValidPreviousCommittedTxIdInFirstNeoStoreLog() throws Throwable
    {
        // given
        GraphDatabaseAPI db = startDb( backupPort );
        createSchemaIndex( db );
        createAndIndexNode( db, 1 );
        createAndIndexNode( db, 2 );
        createAndIndexNode( db, 3 );
        createAndIndexNode( db, 4 );

        // and backups set up correctly
        OptionalHostnamePort optionalHostnamePort = new OptionalHostnamePort( BACKUP_HOST, backupPort, null );
        backupStrategyCoordinatorBuilder.withBackupDirectory( backupDir.toPath() )
                .withConfig( dbRule.getConfigCopy() )
                .withOptionalHostnamePort( optionalHostnamePort );

        db.getDependencyResolver().resolveDependency( StorageEngine.class ).flushAndForce( limiter );
        long txId = db.getDependencyResolver().resolveDependency( TransactionIdStore.class ).getLastCommittedTransactionId();

        // when
        strategyEnum.getSolution().apply( backupStrategyCoordinatorBuilder ).performFullBackup( backupDir, dbRule.getConfigCopy(), optionalHostnamePort );
        db.shutdown();

        // then
        checkPreviousCommittedTxIdFromLog( 0, TransactionIdStore.BASE_TX_ID );
    }

    @Test
    public void shouldFindTransactionLogContainingLastLuceneTransaction() throws Throwable
    {
        // given
        Config defaultConfig = dbRule.getConfigCopy();
        GraphDatabaseAPI db = startDb( backupPort );
        createSchemaIndex( db );
        createAndIndexNode( db, 1 );
        OptionalHostnamePort optionalHostnamePort = new OptionalHostnamePort( BACKUP_HOST, backupPort, null );

        // when
        strategyEnum.getSolution().apply( backupStrategyCoordinatorBuilder ).performFullBackup( backupDir, defaultConfig, optionalHostnamePort );
        db.shutdown();

        // then
        assertEquals( getDbRepresentation(), getBackupDbRepresentation() );
        assertNotEquals( 0, getLastTxChecksum( pageCacheRule.getPageCache( fileSystem ) ) );
    }

    private OptionalHostnamePort validBackupAddress()
    {
        return new OptionalHostnamePort( BACKUP_HOST, backupPort, null );
    }

    @Test
    public void shouldGiveHelpfulErrorMessageIfLogsPrunedPastThePointOfNoReturn() throws Exception
    {
        // Given
        Config defaultConfig = dbRule.getConfigCopy();
        dbRule.setConfig( GraphDatabaseSettings.keep_logical_logs, "false" );
        OptionalHostnamePort optionalHostnamePort = validBackupAddress();

        backupStrategyCoordinatorBuilder.withOptionalHostnamePort( optionalHostnamePort ).withBackupDirectory( backupDir.toPath() );

        // have logs rotated on every transaction
        GraphDatabaseAPI db = startDb( backupPort );
//        createSchemaIndex( db );

        createAndIndexNode( db, 1 );
        rotateAndCheckPoint( db );

        // A full backup
        strategyEnum.getSolution().apply( backupStrategyCoordinatorBuilder ).performFullBackup( backupDir, defaultConfig, optionalHostnamePort );

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
        Throwable e = strategyEnum.getSolution()
                .apply( backupStrategyCoordinatorBuilder )
                .performIncrementalBackup( backupDir, defaultConfig, optionalHostnamePort )
                .getCause()
                .orElseThrow( () -> new AssertionError( "Should have thrown exception." ) );
        // Then
        assertThat( e.getMessage(), equalTo( BackupProtocolService.TOO_OLD_BACKUP ) );

        backupStrategyCoordinatorBuilder.withFallbackToFull( true );
        backupStrategyCoordinatorBuilder.fromSingleStrategy( strategyEnum ).performBackup( backupStrategyCoordinatorBuilder.getOnlineBackupContext() );
    }

    @Test
    public void shouldFallbackToFullBackupIfIncrementalFailsAndExplicitlyAskedToDoThis() throws Exception
    {
        // Given
        Config defaultConfig = dbRule.getConfigCopy();
        dbRule.setConfig( GraphDatabaseSettings.keep_logical_logs, "false" );
        OptionalHostnamePort optionalHostnamePort = validBackupAddress();
        backupStrategyCoordinatorBuilder.withConfig( defaultConfig )
                .withFallbackToFull( true )
                .withOptionalHostnamePort( optionalHostnamePort )
                .withBackupDirectory( backupDir.toPath() );
        // have logs rotated on every transaction
        GraphDatabaseAPI db = startDb( backupPort );
        createSchemaIndex( db );

        createAndIndexNode( db, 1 );

        // A full backup
        strategyEnum.getSolution().apply( backupStrategyCoordinatorBuilder ).performFullBackup( backupDir, defaultConfig, optionalHostnamePort );

        // And the log the backup uses is rotated out
        createAndIndexNode( db, 2 );
        rotateAndCheckPoint( db );
        createAndIndexNode( db, 3 );
        rotateAndCheckPoint( db );
        createAndIndexNode( db, 4 );
        rotateAndCheckPoint( db );

        // when
        backupStrategyCoordinatorBuilder.fromSingleStrategy( strategyEnum ).performBackup( backupStrategyCoordinatorBuilder.getOnlineBackupContext() );

        // Then
        db.shutdown();
        assertEquals( getDbRepresentation(), getBackupDbRepresentation() );
    }

    private void rotateAndCheckPoint( GraphDatabaseAPI db ) throws IOException
    {
        db.getDependencyResolver().resolveDependency( LogRotation.class ).rotateLogFile();
        db.getDependencyResolver().resolveDependency( CheckPointer.class ).forceCheckPoint( new SimpleTriggerInfo( "test" ) );
    }

    @Test
    public void shouldHandleBackupWhenLogFilesHaveBeenDeleted() throws Exception
    {
        // Given
        Config defaultConfig = dbRule.getConfigCopy();
        dbRule.setConfig( GraphDatabaseSettings.keep_logical_logs, "false" );
        GraphDatabaseAPI db = startDb( backupPort );
        createSchemaIndex( db );
        OptionalHostnamePort optionalHostnamePort = validBackupAddress();
        backupStrategyCoordinatorBuilder.withBackupDirectory( backupDir.toPath() ).withOptionalHostnamePort( optionalHostnamePort );

        createAndIndexNode( db, 1 );

        // A full backup
        strategyEnum.getSolution().apply( backupStrategyCoordinatorBuilder ).performFullBackup( backupDir, defaultConfig, optionalHostnamePort );

        // And the log the backup uses is rotated out
        createAndIndexNode( db, 2 );
        db = deleteLogFilesAndRestart();

        createAndIndexNode( db, 3 );
        db = deleteLogFilesAndRestart();

        // when
        backupStrategyCoordinatorBuilder.fromSingleStrategy( strategyEnum ).performBackup( backupStrategyCoordinatorBuilder.getOnlineBackupContext() );

        // Then
        db.shutdown();
        assertEquals( getDbRepresentation(), getBackupDbRepresentation() );
    }

    private GraphDatabaseAPI deleteLogFilesAndRestart() throws IOException
    {
        final FileFilter logFileFilter = pathname -> pathname.getName().contains( "logical" );
        return dbRule.restartDatabase( ( fs, storeDirectory ) ->
        {
            for ( File logFile : storeDir.listFiles( logFileFilter ) )
            {
                logFile.delete();
            }
        } );
    }

    @Test
    public void backupShouldWorkWithReadOnlySourceDatabases() throws Exception
    {
        // Create some data
        GraphDatabaseAPI db = startDb( backupPort );
        createSchemaIndex( db );
        createAndIndexNode( db, 1 );
        OptionalHostnamePort optionalHostnamePort = validBackupAddress();
        backupStrategyCoordinatorBuilder.withBackupDirectory( backupDir.toPath() ).withFallbackToFull( true ).withOptionalHostnamePort( optionalHostnamePort );

        // Make it read-only
        db = dbRule.restartDatabase( GraphDatabaseSettings.read_only.name(), TRUE.toString() );

        // Take a backup
        backupStrategyCoordinatorBuilder.withOptionalHostnamePort( optionalHostnamePort );
        backupStrategyCoordinatorBuilder.fromSingleStrategy( strategyEnum ).performBackup( backupStrategyCoordinatorBuilder.getOnlineBackupContext() );

        // Then
        db.shutdown();
        assertEquals( getDbRepresentation(), getBackupDbRepresentation() );
    }

    @Test
    public void shouldDoFullBackupOnIncrementalFallbackToFullIfNoBackupFolderExists() throws Exception
    {
        // Given
        Config defaultConfig = dbRule.getConfigCopy();
        dbRule.setConfig( GraphDatabaseSettings.keep_logical_logs, "false" );
        OptionalHostnamePort optionalHostnamePort = validBackupAddress();
        backupStrategyCoordinatorBuilder.withOptionalHostnamePort( optionalHostnamePort );
        GraphDatabaseAPI db = startDb( backupPort );
        createSchemaIndex( db );

        createAndIndexNode( db, 1 );
        backupStrategyCoordinatorBuilder.withConfig( defaultConfig ).withFallbackToFull( true ).withBackupDirectory( backupDir.toPath() );

        // when
        backupStrategyCoordinatorBuilder.fromSingleStrategy( strategyEnum ).performBackup( backupStrategyCoordinatorBuilder.getOnlineBackupContext() );

        // then
        db.shutdown();
        assertEquals( getDbRepresentation(), getBackupDbRepresentation() );
    }

    @Test
    @Ignore( "This is green, but shouldn't be. See last assert" )
    public void shouldContainTransactionsThatHappenDuringBackupProcess() throws Throwable
    {
        // given
        Config defaultConfig = dbRule.getConfigCopy();
        dbRule.setConfig( OnlineBackupSettings.online_backup_enabled, "false" );
        Config withOnlineBackupDisabled = dbRule.getConfigCopy();

        OptionalHostnamePort optionalHostnamePort = validBackupAddress();
        backupStrategyCoordinatorBuilder.withFallbackToFull( true ).withOptionalHostnamePort( optionalHostnamePort ).withBackupDirectory( backupDir.toPath() );

        final Barrier.Control barrier = new Barrier.Control();
        final GraphDatabaseAPI db = startDb( backupPort );
        createSchemaIndex( db );

        createAndIndexNode( db, 1 ); // create some data

        final DependencyResolver resolver = db.getDependencyResolver();
        long expectedLastTxId = resolver.resolveDependency( TransactionIdStore.class ).getLastClosedTransactionId();

        // This monitor is added server-side...
        monitors.addMonitorListener( new StoreSnoopingMonitor( barrier ) );

        Dependencies dependencies = new Dependencies( resolver );
        dependencies.satisfyDependencies( defaultConfig, monitors, NullLogProvider.getInstance() );

        OnlineBackupKernelExtension backup = (OnlineBackupKernelExtension) new OnlineBackupExtensionFactory().newInstance(
                new SimpleKernelContext( storeDir, DatabaseInfo.UNKNOWN, dependencies ),
                DependenciesProxy.dependencies( dependencies, OnlineBackupExtensionFactory.Dependencies.class ) );
        backup.start();

        // when
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.execute( () ->
        {
            barrier.awaitUninterruptibly();

            createAndIndexNode( db, 1 );
            resolver.resolveDependency( StorageEngine.class ).flushAndForce( limiter );

            barrier.release();
        } );

        BackupOutcome backupOutcome = null;
        backupStrategyCoordinatorBuilder.fromSingleStrategy( strategyEnum ).performBackup( backupStrategyCoordinatorBuilder.getOnlineBackupContext() );

        backup.stop();
        executor.shutdown();
        executor.awaitTermination( 30, TimeUnit.SECONDS );

        // then
        checkPreviousCommittedTxIdFromLog( 0, expectedLastTxId );
        File neoStore = new File( storeDir, MetaDataStore.DEFAULT_NAME );
        long txIdFromOrigin = MetaDataStore.getRecord( resolver.resolveDependency( PageCache.class ), neoStore, Position.LAST_TRANSACTION_ID );
        checkLastCommittedTxIdInLogAndNeoStore( expectedLastTxId + 1, txIdFromOrigin );
        assertEquals( DbRepresentation.of( db ), getBackupDbRepresentation() );
        assertTrue( backupOutcome.isConsistent() ); // TODO figure out if this is relevant
    }

    @Test
    public void backupsShouldBeMentionedInServerConsoleLog() throws Throwable
    {
        if ( strategyEnum != BackupStrategyCoordinatorBuilder.StrategyEnum.HA )
        {
            return; // This is only important for single instance/HA because CC always has the port available
        }

        // given
//        databaseConfigurationIsSetupToUseCustomPorts(); // TODO
        Config config = dbRule.getConfigCopy();
        dbRule.setConfig( OnlineBackupSettings.online_backup_enabled, "false" );
        Config withOnlineBackupDisabled = dbRule.getConfigCopy();
        createAndIndexNode( dbRule, 1 );

        final Log log = mock( Log.class );
        LogProvider logProvider = logProviderForMock( log );
        Logger logger = mock( Logger.class );
        when( log.infoLogger() ).thenReturn( logger );
        LogService logService = mock( LogService.class );
        when( logService.getInternalLogProvider() ).thenReturn( logProvider );

        OptionalHostnamePort optionalHostnamePort = validBackupAddress();
        backupStrategyCoordinatorBuilder.withBackupDirectory( backupDir.toPath() ).withOptionalHostnamePort( optionalHostnamePort );

        Dependencies dependencies = new Dependencies( dbRule.getDependencyResolver() );
        dependencies.satisfyDependencies( config, monitors, logService );

        OnlineBackupKernelExtension backup = (OnlineBackupKernelExtension) new OnlineBackupExtensionFactory().newInstance(
                new SimpleKernelContext( storeDir, DatabaseInfo.UNKNOWN, dependencies ),
                DependenciesProxy.dependencies( dependencies, OnlineBackupExtensionFactory.Dependencies.class ) );
        try
        {
            backup.start();

            // when
            strategyEnum.getSolution().apply( backupStrategyCoordinatorBuilder ).performFullBackup( backupDir, withOnlineBackupDisabled, optionalHostnamePort );

            // then
            verify( logger ).log( eq( "%s: Full backup started..." ), Mockito.startsWith( "BackupServer" ) );
            verify( logger ).log( eq( "%s: Full backup finished." ), Mockito.startsWith( "BackupServer" ) );

            // when
            createAndIndexNode( dbRule, 2 );

            backupStrategyCoordinatorBuilder.fromSingleStrategy( strategyEnum ).performBackup( backupStrategyCoordinatorBuilder.getOnlineBackupContext() );

            // then
            verify( logger ).log( eq( "%s: Incremental backup started..." ), Mockito.startsWith( "BackupServer" ) );
            verify( logger ).log( eq( "%s: Incremental backup finished." ), Mockito.startsWith( "BackupServer" ) );
        }
        finally
        {
            backup.stop();
        }
    }

    @Test
    public void incrementalBackupShouldFailWhenTargetDirContainsDifferentStore() throws IOException, CommandFailed
    {
        // Given
        Config defaultConfig = dbRule.getConfigCopy();
        OptionalHostnamePort optionalHostnamePort = validBackupAddress();
        backupStrategyCoordinatorBuilder.withFallbackToFull( true ).withBackupDirectory( backupDir.toPath() ).withOptionalHostnamePort( optionalHostnamePort );

        GraphDatabaseAPI db1 = startDb( backupPort );
        createSchemaIndex( db1 );
        createAndIndexNode( db1, 1 );

        strategyEnum.getSolution().apply( backupStrategyCoordinatorBuilder ).performFullBackup( backupDir, defaultConfig, optionalHostnamePort );

        // When A database is started containing different store IDs
        GraphDatabaseAPI db2 = dbRule.restartDatabase( ( fs, storeDirectory ) ->
        {
            deleteAllBackedUpTransactionLogs();

            fileSystem.deleteRecursively( storeDir );
            fileSystem.mkdir( storeDir );
        } );
        createAndIndexNode( db2, 2 );

        PotentiallyErroneousState<BackupStageOutcome> outcome =
                strategyEnum.getSolution().apply( backupStrategyCoordinatorBuilder ).performIncrementalBackup( backupDir, defaultConfig, optionalHostnamePort );

        // Then
        Throwable e = outcome.getCause().orElseThrow( () -> new AssertionFailedError( "Should have thrown exception about mismatching store ids" ) );
        assertThat( e.getMessage(), equalTo( BackupProtocolService.DIFFERENT_STORE_MESSAGE ) );
        assertThat( e.getCause(), instanceOf( MismatchingStoreIdException.class ) );
    }

    private GraphDatabaseAPI startDb( int port )
    {
        switch ( strategyEnum )
        {
        case CC:
            try
            {
                return clusterRule.withInstanceCoreParam( CausalClusteringSettings.transaction_listen_address, i -> ":" + (port + i) )
                        .startCluster()
                        .awaitLeader()
                        .database();
            }
            catch ( Exception e )
            {
                throw new RuntimeException( e );
            }
        case HA:
            dbRule.setConfig( OnlineBackupSettings.online_backup_server, BACKUP_HOST + ":" + port );
            return dbRule.withSetting( OnlineBackupSettings.online_backup_server, ":" + port ).startLazily();
        default:
            throw new RuntimeException( "Unhandled db type" );
        }
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
        LogFiles logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( backupDir, fileSystem ).build();
        LogHeader logHeader = LogHeaderReader.readLogHeader( fileSystem, logFiles.getLogFileForVersion( logVersion ) );
        assertEquals( txId, logHeader.lastCommittedTxId );
    }

    private void checkLastCommittedTxIdInLogAndNeoStore( long txId, long txIdFromOrigin ) throws Exception
    {
        // Assert last committed transaction can be found in tx log and is the last tx in the log
        LifeSupport life = new LifeSupport();
        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        LogicalTransactionStore transactionStore = life.add( new ReadOnlyTransactionStore( pageCache, fileSystem, backupDir, monitors ) );
        life.start();
        try ( IOCursor<CommittedTransactionRepresentation> cursor = transactionStore.getTransactions( txId ) )
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

    private void deleteAllBackedUpTransactionLogs() throws IOException
    {
        LogFiles logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( backupDir, fileSystem ).build();
        for ( File log : logFiles.logFiles() )
        {
            fileSystem.deleteFile( log );
        }
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
        return DbRepresentation.of( backupDir, Config.defaults( OnlineBackupSettings.online_backup_enabled, Settings.FALSE ) );
    }

    private DbRepresentation getDbRepresentation()
    {
        return DbRepresentation.of( storeDir, Config.defaults( OnlineBackupSettings.online_backup_enabled, Settings.FALSE ) );
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
            if ( storefile.getAbsolutePath().contains( NODE_STORE ) || storefile.getAbsolutePath().contains( RELATIONSHIP_STORE ) )
            {
                barrier.reached(); // multiple calls to this barrier will not block
            }
        }
    }

    private LogProvider logProviderForMock( Log mockLog )
    {
        return new LogProvider()
        {
            @Override
            public Log getLog( Class loggingClass )
            {
                return mockLog;
            }

            @Override
            public Log getLog( String name )
            {
                return mockLog;
            }
        };
    }

    private void removePropertiesFromNodes( GraphDatabaseAPI graphDatabase, Label markerLabel )
    {
        try ( Transaction transaction = graphDatabase.beginTx() )
        {
            Node node = findNodeByLabel( graphDatabase, markerLabel );
            for ( int i = 0; i < 6; i++ )
            {
                node.removeProperty( "property" + i );
            }

            transaction.success();
        }
    }

    private void createNodeWithMarker( GraphDatabaseAPI graphDatabase, Label markerLabel )
    {
        try ( Transaction transaction = graphDatabase.beginTx() )
        {
            Node node = graphDatabase.createNode();
            node.addLabel( markerLabel );
            transaction.success();
        }
    }

    private void createNodeProperties( GraphDatabaseAPI graphDatabase, Label markerLabel, int startProperty, int endProperty )
    {
        try ( Transaction transaction = graphDatabase.beginTx() )
        {
            Node node = findNodeByLabel( graphDatabase, markerLabel );
            for ( int i = startProperty; i < endProperty; i++ )
            {
                node.setProperty( "property" + i, "testValue" + i );
            }
            transaction.success();
        }
    }
}
