/**
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package upgrade;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.DefaultTxHook;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.nioneo.store.DefaultWindowPoolFactory;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.nioneo.xa.command.Command;
import org.neo4j.kernel.impl.storemigration.MigrationTestUtils;
import org.neo4j.kernel.impl.storemigration.StoreMigrator;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.test.CleanupRule;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.ha.ClusterManager;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.emptyCollectionOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static upgrade.StoreMigratorTestUtil.buildClusterWithMasterDirIn;
import static upgrade.StoreMigratorTestUtil.findAllMatchingFiles;
import static upgrade.StoreMigratorTestUtil.readLuceneLogEntriesFrom;
import static upgrade.StoreMigratorTestUtil.readTransactionLogEntriesFrom;

import static org.neo4j.consistency.store.StoreAssertions.assertConsistentStore;
import static org.neo4j.kernel.impl.nioneo.store.CommonAbstractStore.ALL_STORES_VERSION;
import static org.neo4j.kernel.impl.nioneo.store.NeoStore.versionLongToString;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.find20FormatStoreDirectory;
import static org.neo4j.kernel.impl.storemigration.UpgradeConfiguration.ALLOW_UPGRADE;
import static org.neo4j.test.ha.ClusterManager.allSeesAllAsAvailable;

public class StoreMigratorFrom20IT
{
    @Test
    public void shouldMigrate() throws IOException, ConsistencyCheckIncompleteException
    {
        // WHEN
        upgrader( new StoreMigrator( monitor, fs ) ).migrateIfNeeded( find20FormatStoreDirectory( storeDir ) );

        // THEN
        assertEquals( 100, monitor.eventSize() );
        assertTrue( monitor.isStarted() );
        assertTrue( monitor.isFinished() );

        GraphDatabaseService database = cleanup.add( new GraphDatabaseFactory().newEmbeddedDatabase(
                storeDir.getAbsolutePath() ) );

        try
        {
            verifyDatabaseContents( database );
        }
        finally
        {
            // CLEANUP
            database.shutdown();
        }

        NeoStore neoStore = cleanup.add( storeFactory.newNeoStore( storeFileName ) );
        verifyNeoStore( neoStore );
        neoStore.close();
        assertConsistentStore( storeDir );
    }

    @Test
    public void shouldMigrateCluster() throws Throwable
    {
        // Given
        File legacyStoreDir = find20FormatStoreDirectory( storeDir );

        // When
        upgrader( new StoreMigrator( monitor, fs ) ).migrateIfNeeded( legacyStoreDir );
        ClusterManager.ManagedCluster cluster =
                cleanup.add( buildClusterWithMasterDirIn( fs, legacyStoreDir, cleanup ) );
        cluster.await( allSeesAllAsAvailable() );
        cluster.sync();

        // Then
        HighlyAvailableGraphDatabase slave1 = cluster.getAnySlave();
        verifySlaveContents( slave1 );
        verifySlaveContents( cluster.getAnySlave( slave1 ) );
        verifyDatabaseContents( cluster.getMaster() );
    }

    @Test
    public void shouldRewriteTransactionLogsDuringMigration() throws Throwable
    {
        // Given
        File legacyStoreDir = find20FormatStoreDirectory( storeDir );

        // When
        upgrader( new StoreMigrator( monitor, fs ) ).migrateIfNeeded( legacyStoreDir );

        // Then
        File[] transactionLogs = findAllMatchingFiles( legacyStoreDir, "nioneo_logical\\.log\\.v.*" );
        assertThat( transactionLogs, arrayWithSize( 3 ) );

        List<LogEntry> logEntries = readTransactionLogEntriesFrom( fs, transactionLogs[1] );
        assertThat( logEntries, not( emptyCollectionOf( LogEntry.class ) ) );

        assertThat( logEntries.get( 0 ), instanceOf( LogEntry.Start.class ) );

        assertThat( logEntries.get( 1 ), instanceOf( LogEntry.Command.class ) );
        assertThat( ((LogEntry.Command) logEntries.get( 1 )).getXaCommand(),
                instanceOf( Command.LabelTokenCommand.class ) );

        assertThat( ((LogEntry.Command) logEntries.get( logEntries.size() - 3 )).getXaCommand(),
                instanceOf( Command.SchemaRuleCommand.class ) );

        assertThat( logEntries.get( logEntries.size() - 2 ), instanceOf( LogEntry.OnePhaseCommit.class ) );

        assertThat( logEntries.get( logEntries.size() - 1 ), instanceOf( LogEntry.Done.class ) );
    }

    @Test
    public void shouldRewriteLuceneLogsDuringMigration() throws Throwable
    {
        // Given
        File legacyStoreDir = find20FormatStoreDirectory( storeDir );

        // When
        upgrader( new StoreMigrator( monitor, fs ) ).migrateIfNeeded( legacyStoreDir );

        // Then
        File indexDir = new File( legacyStoreDir, "index" );
        File[] luceneLogs = findAllMatchingFiles( indexDir, "lucene\\.log\\.v.*" );
        assertThat( luceneLogs, arrayWithSize( 3 ) );

        List<LogEntry> logEntries = readLuceneLogEntriesFrom( fs, luceneLogs[2] );
        assertThat( logEntries, hasSize( 4 ) );

        LogEntry log1 = logEntries.get( 0 );
        assertThat( log1, instanceOf( LogEntry.Start.class ) );
        assertThat( ((LogEntry.Start) log1).getMasterId(), equalTo( -1 ) );
        assertThat( ((LogEntry.Start) log1).getLocalId(), equalTo( -1 ) );

        LogEntry log2 = logEntries.get( 1 );
        assertThat( log2, instanceOf( LogEntry.Command.class ) );
        assertThat( ((LogEntry.Command) log2).getXaCommand().getClass().getSimpleName(),
                equalTo( "CreateIndexCommand" ) );

        assertThat( logEntries.get( logEntries.size() - 2 ), instanceOf( LogEntry.OnePhaseCommit.class ) );

        assertThat( logEntries.get( logEntries.size() - 1 ), instanceOf( LogEntry.Done.class ) );
    }

    private static void verifyDatabaseContents( GraphDatabaseService database )
    {
        DatabaseContentVerifier verifier = new DatabaseContentVerifier( database );
        verifyNumberOfNodesAndRelationships( verifier );
        verifier.verifyNodeIdsReused();
        verifier.verifyRelationshipIdsReused();
        verifier.verifyLegacyIndex();
        verifier.verifyIndex();
    }

    private static void verifySlaveContents( HighlyAvailableGraphDatabase haDb )
    {
        DatabaseContentVerifier verifier = new DatabaseContentVerifier( haDb );
        verifyNumberOfNodesAndRelationships( verifier );
    }

    private static void verifyNumberOfNodesAndRelationships( DatabaseContentVerifier verifier )
    {
        verifier.verifyNodes( 501 );
        verifier.verifyRelationships( 500 );
    }

    public static void verifyNeoStore( NeoStore neoStore )
    {
        assertEquals( 1317392957120L, neoStore.getCreationTime() );
        assertEquals( -472309512128245482l, neoStore.getRandomNumber() );
        assertEquals( 5l, neoStore.getVersion() );
        assertEquals( ALL_STORES_VERSION, versionLongToString( neoStore.getStoreVersion() ) );
        assertEquals( 1010l, neoStore.getLastCommittedTx() );
    }

    private StoreUpgrader upgrader( StoreMigrator storeMigrator )
    {
        DevNullLoggingService logging = new DevNullLoggingService();
        StoreUpgrader upgrader = new StoreUpgrader( ALLOW_UPGRADE, fs, StoreUpgrader.NO_MONITOR, logging );
        upgrader.addParticipant( storeMigrator );
        return upgrader;
    }

    private final FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
    private final File storeDir = TargetDirectory.forTest( getClass() ).makeGraphDbDir();
    private final ListAccumulatorMigrationProgressMonitor monitor = new ListAccumulatorMigrationProgressMonitor();
    private StoreFactory storeFactory;
    private File storeFileName;

    @Before
    public void setUp()
    {
        Config config = MigrationTestUtils.defaultConfig();
        storeFileName = new File( storeDir, NeoStore.DEFAULT_NAME );
        storeFactory = new StoreFactory( config, new DefaultIdGeneratorFactory(),
                new DefaultWindowPoolFactory(), fs, StringLogger.DEV_NULL, new DefaultTxHook() );
    }

    public final
    @Rule
    CleanupRule cleanup = new CleanupRule();
}
