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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.DefaultTxHook;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.core.Token;
import org.neo4j.kernel.impl.nioneo.store.DefaultWindowPoolFactory;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyKeyTokenStore;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.nioneo.xa.command.Command;
import org.neo4j.kernel.impl.storemigration.MigrationTestUtils;
import org.neo4j.kernel.impl.storemigration.StoreMigrator;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.impl.storemigration.legacystore.v19.Legacy19Store;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.test.CleanupRule;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.Unzip;
import org.neo4j.test.ha.ClusterManager;
import org.neo4j.tooling.GlobalGraphOperations;

import static java.lang.Integer.MAX_VALUE;

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
import static org.neo4j.graphdb.Neo4jMatchers.hasProperty;
import static org.neo4j.graphdb.Neo4jMatchers.inTx;
import static org.neo4j.kernel.impl.nioneo.store.CommonAbstractStore.ALL_STORES_VERSION;
import static org.neo4j.kernel.impl.nioneo.store.NeoStore.versionLongToString;
import static org.neo4j.kernel.impl.nioneo.store.StoreFactory.PROPERTY_KEY_TOKEN_STORE_NAME;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.find19FormatStoreDirectory;
import static org.neo4j.kernel.impl.storemigration.UpgradeConfiguration.ALLOW_UPGRADE;
import static org.neo4j.test.ha.ClusterManager.allSeesAllAsAvailable;

public class StoreMigratorFrom19IT
{
    @Test
    public void shouldMigrate() throws IOException, ConsistencyCheckIncompleteException
    {
        // GIVEN
        File legacyStoreDir = find19FormatStoreDirectory( storeDir );

        // WHEN
        newStoreUpgrader().migrateIfNeeded( legacyStoreDir );

        // THEN
        assertEquals( 100, monitor.eventSize() );
        assertTrue( monitor.isStarted() );
        assertTrue( monitor.isFinished() );
        GraphDatabaseService database = cleanup.add(
                new GraphDatabaseFactory().newEmbeddedDatabase( storeDir.getAbsolutePath() )
        );

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
        File legacyStoreDir = find19FormatStoreDirectory( storeDir );

        // When
        newStoreUpgrader().migrateIfNeeded( legacyStoreDir );

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
        File legacyStoreDir = find19FormatStoreDirectory( storeDir );

        // When
        newStoreUpgrader().migrateIfNeeded( legacyStoreDir );

        // Then
        File[] transactionLogs = findAllMatchingFiles( legacyStoreDir, "nioneo_logical\\.log\\.v.*" );
        assertThat( transactionLogs, arrayWithSize( 1 ) );

        List<LogEntry> logEntries = readTransactionLogEntriesFrom( fs, transactionLogs[0] );
        assertThat( logEntries, not( emptyCollectionOf( LogEntry.class ) ) );

        assertThat( logEntries.get( 0 ), instanceOf( LogEntry.Start.class ) );

        assertThat( logEntries.get( 1 ), instanceOf( LogEntry.Command.class ) );
        assertThat( ((LogEntry.Command) logEntries.get( 1 )).getXaCommand(), instanceOf( Command.NodeCommand
                .class ) );

        assertThat( ((LogEntry.Command) logEntries.get( logEntries.size() - 3 )).getXaCommand(),
                instanceOf( Command.PropertyCommand.class ) );

        assertThat( logEntries.get( logEntries.size() - 2 ), instanceOf( LogEntry.OnePhaseCommit.class ) );

        assertThat( logEntries.get( logEntries.size() - 1 ), instanceOf( LogEntry.Done.class ) );
    }

    @Test
    public void shouldRewriteLuceneLogsDuringMigration() throws Throwable
    {
        // Given
        File legacyStoreDir = find19FormatStoreDirectory( storeDir );

        // When
        newStoreUpgrader().migrateIfNeeded( legacyStoreDir );

        // Then
        File indexDir = new File( legacyStoreDir, "index" );
        File[] luceneLogs = findAllMatchingFiles( indexDir, "lucene\\.log\\.v.*" );
        assertThat( luceneLogs, arrayWithSize( 1 ) );

        List<LogEntry> logEntries = readLuceneLogEntriesFrom( fs, luceneLogs[0] );
        assertThat( logEntries, hasSize( 12 ) );

        LogEntry log1 = logEntries.get( 0 );
        assertThat( log1, instanceOf( LogEntry.Start.class ) );
        assertThat( ((LogEntry.Start) log1).getMasterId(), equalTo( -1 ) );
        assertThat( ((LogEntry.Start) log1).getLocalId(), equalTo( -1 ) );

        LogEntry log2 = logEntries.get( 1 );
        assertThat( log2, instanceOf( LogEntry.Command.class ) );
        LogEntry.Command log2Cmd = (LogEntry.Command) log2;
        assertThat( log2Cmd.getXaCommand().getClass().getSimpleName(), equalTo( "CreateIndexCommand" ) );

        assertThat( logEntries.get( logEntries.size() - 3 ), instanceOf( LogEntry.Command.class ) );

        assertThat( logEntries.get( logEntries.size() - 2 ), instanceOf( LogEntry.OnePhaseCommit.class ) );

        assertThat( logEntries.get( logEntries.size() - 1 ), instanceOf( LogEntry.Done.class ) );
    }

    @Test
    public void shouldDeduplicateUniquePropertyIndexKeys() throws Exception
    {
        // GIVEN
        // a store that contains two nodes with property "name" of which there are two key tokens
        Unzip.unzip( Legacy19Store.class, "propkeydupdb.zip", storeDir );

        // WHEN
        // upgrading that store, the two key tokens for "name" should be merged
        newStoreUpgrader().migrateIfNeeded( storeDir );

        // THEN
        // verify that the "name" property for both the involved nodes
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( storeDir.getAbsolutePath() );
        try
        {
            Node nodeA = getNodeWithName( db, "A" );
            assertThat( nodeA, inTx( db, hasProperty( "name" ).withValue( "A" ) ) );

            Node nodeB = getNodeWithName( db, "B" );
            assertThat( nodeB, inTx( db, hasProperty( "name" ).withValue( "B" ) ) );

            Node nodeC = getNodeWithName( db, "C" );
            assertThat( nodeC, inTx( db, hasProperty( "name" ).withValue( "C" ) ) );
            assertThat( nodeC, inTx( db, hasProperty( "other" ).withValue( "a value" ) ) );
            assertThat( nodeC, inTx( db, hasProperty( "third" ).withValue( "something" ) ) );
        }
        finally
        {
            db.shutdown();
        }

        // THEN
        // verify that there are no duplicate keys in the store
        PropertyKeyTokenStore tokenStore = cleanup.add(
                storeFactory.newPropertyKeyTokenStore( new File( storeFileName + PROPERTY_KEY_TOKEN_STORE_NAME ) ) );
        Token[] tokens = tokenStore.getTokens( MAX_VALUE );
        tokenStore.close();
        assertNoDuplicates( tokens );

        assertConsistentStore( storeDir );
    }

    private static void verifyDatabaseContents( GraphDatabaseService db )
    {
        DatabaseContentVerifier verifier = new DatabaseContentVerifier( db );
        verifyNumberOfNodesAndRelationships( verifier );
        verifier.verifyNodeIdsReused();
        verifier.verifyRelationshipIdsReused();
        verifier.verifyLegacyIndex();
    }

    private static void verifySlaveContents( HighlyAvailableGraphDatabase haDb )
    {
        DatabaseContentVerifier verifier = new DatabaseContentVerifier( haDb );
        verifyNumberOfNodesAndRelationships( verifier );
        verifier.verifyLegacyIndex();
    }

    private static void verifyNumberOfNodesAndRelationships( DatabaseContentVerifier verifier )
    {
        verifier.verifyNodes( 1_000 );
        verifier.verifyRelationships( 500 );
    }

    private static void verifyNeoStore( NeoStore neoStore )
    {
        assertEquals( 1409818980890L, neoStore.getCreationTime() );
        assertEquals( 7528833218632030901L, neoStore.getRandomNumber() );
        assertEquals( 2L, neoStore.getVersion() );
        assertEquals( ALL_STORES_VERSION, versionLongToString( neoStore.getStoreVersion() ) );
        assertEquals( 8L + 3, neoStore.getLastCommittedTx() ); // prior verifications add 3 transactions
    }

    private void assertNoDuplicates( Token[] tokens )
    {
        Set<String> visited = new HashSet<>();
        for ( Token token : tokens )
        {
            assertTrue( visited.add( token.name() ) );
        }
    }

    private Node getNodeWithName( GraphDatabaseService db, String name )
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( Node node : GlobalGraphOperations.at( db ).getAllNodes() )
            {
                if ( name.equals( node.getProperty( "name", null ) ) )
                {
                    tx.success();
                    return node;
                }
            }
        }
        throw new IllegalArgumentException( name + " not found" );
    }

    private StoreUpgrader newStoreUpgrader()
    {
        DevNullLoggingService logging = new DevNullLoggingService();
        StoreUpgrader upgrader = new StoreUpgrader( ALLOW_UPGRADE, fs, StoreUpgrader.NO_MONITOR, logging );
        upgrader.addParticipant( new StoreMigrator( monitor, fs ) );
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

    @Rule
    public final CleanupRule cleanup = new CleanupRule();
}
