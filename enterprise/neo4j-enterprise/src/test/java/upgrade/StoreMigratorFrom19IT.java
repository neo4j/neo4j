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
package upgrade;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.api.index.inmemory.InMemoryIndexProvider;
import org.neo4j.kernel.impl.api.scan.InMemoryLabelScanStore;
import org.neo4j.kernel.impl.api.scan.LabelScanStoreProvider;
import org.neo4j.kernel.impl.core.Token;
import org.neo4j.kernel.impl.ha.ClusterManager;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.PropertyKeyTokenStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.storemigration.MigrationTestUtils;
import org.neo4j.kernel.impl.storemigration.participant.SchemaIndexMigrator;
import org.neo4j.kernel.impl.storemigration.participant.StoreMigrator;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.impl.storemigration.StoreVersionCheck;
import org.neo4j.kernel.impl.storemigration.UpgradableDatabase;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyStoreVersionCheck;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.PageCacheRule;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import static upgrade.StoreMigratorTestUtil.buildClusterWithMasterDirIn;

import static java.lang.Integer.MAX_VALUE;

import static org.neo4j.consistency.store.StoreAssertions.assertConsistentStore;
import static org.neo4j.graphdb.Neo4jMatchers.hasProperty;
import static org.neo4j.graphdb.Neo4jMatchers.inTx;
import static org.neo4j.kernel.impl.ha.ClusterManager.allSeesAllAsAvailable;
import static org.neo4j.kernel.impl.store.CommonAbstractStore.ALL_STORES_VERSION;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.find19FormatHugeStoreDirectory;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.find19FormatStoreDirectory;

public class StoreMigratorFrom19IT
{
    @Rule
    public final TargetDirectory.TestDirectory storeDir = TargetDirectory.testDirForTest( getClass() );
    @Rule
    public final PageCacheRule pageCacheRule = new PageCacheRule();

    private final Config config = MigrationTestUtils.defaultConfig();
    private final SchemaIndexProvider schemaIndexProvider = new InMemoryIndexProvider();
    private final LabelScanStoreProvider labelScanStoreProvider = new LabelScanStoreProvider( new
            InMemoryLabelScanStore(), 1 );
    private final FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
    private final ListAccumulatorMigrationProgressMonitor monitor = new ListAccumulatorMigrationProgressMonitor();
    private PageCache pageCache;
    private StoreFactory storeFactory;
    private UpgradableDatabase upgradableDatabase;
    private final LifeSupport life = new LifeSupport();

    @Before
    public void setUp()
    {
        pageCache = pageCacheRule.getPageCache( fs );
        storeFactory = new StoreFactory( storeDir.directory(), config, new DefaultIdGeneratorFactory( fs ),
                pageCache, fs, NullLogProvider.getInstance() );
        upgradableDatabase =
                new UpgradableDatabase( fs, new StoreVersionCheck( pageCache ), new LegacyStoreVersionCheck( fs ) );
    }

    @After
    public void tearDown()
    {
        life.shutdown();
    }

    @Test
    public void shouldMigrate() throws IOException, ConsistencyCheckIncompleteException
    {
        // GIVEN
        File legacyStoreDir = find19FormatHugeStoreDirectory( storeDir.directory() );

        // WHEN
        newStoreUpgrader().migrateIfNeeded( legacyStoreDir );

        // THEN
        assertEquals( 2, monitor.progresses().size() );
        assertTrue( monitor.isStarted() );
        assertTrue( monitor.isFinished() );

        GraphDatabaseService database = new GraphDatabaseFactory().newEmbeddedDatabase( storeDir.absolutePath() );

        try
        {
            verifyDatabaseContents( database );
        }
        finally
        {
            // CLEANUP
            database.shutdown();
        }

        try ( NeoStores neoStores = storeFactory.openAllNeoStores( true ) )
        {
            verifyNeoStore( neoStores );
        }

        assertConsistentStore( storeDir.directory() );
    }

    @Test
    public void shouldMigrateCluster() throws Throwable
    {
        // Given
        File legacyStoreDir = find19FormatHugeStoreDirectory( storeDir.directory() );

        // When
        newStoreUpgrader().migrateIfNeeded( legacyStoreDir );

        ClusterManager.ManagedCluster cluster = buildClusterWithMasterDirIn( fs, legacyStoreDir, life );
        cluster.await( allSeesAllAsAvailable() );
        cluster.sync();

        // Then
        HighlyAvailableGraphDatabase slave1 = cluster.getAnySlave();
        verifySlaveContents( slave1 );
        verifySlaveContents( cluster.getAnySlave( slave1 ) );
        verifyDatabaseContents( cluster.getMaster() );
    }

    @Test
    public void shouldDeduplicateUniquePropertyIndexKeys() throws Exception
    {
        // GIVEN
        // a store that contains two nodes with property "name" of which there are two key tokens
        File legacyStoreDir = find19FormatStoreDirectory( storeDir.directory() );

        // WHEN
        // upgrading that store, the two key tokens for "name" should be merged

        newStoreUpgrader().migrateIfNeeded( storeDir.directory() );

        // THEN
        // verify that the "name" property for both the involved nodes
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( storeDir.absolutePath() );
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
        try ( NeoStores neoStores = storeFactory.openAllNeoStores() )
        {
            PropertyKeyTokenStore tokenStore = neoStores.getPropertyKeyTokenStore();
            List<Token> tokens = tokenStore.getTokens( MAX_VALUE );
            assertNoDuplicates( tokens );
        }

        assertConsistentStore( storeDir.directory() );
    }

    private static void verifyDatabaseContents( GraphDatabaseService db )
    {
        DatabaseContentVerifier verifier = new DatabaseContentVerifier( db, 1 );
        verifyNumberOfNodesAndRelationships( verifier );
        verifier.verifyNodeIdsReused();
        verifier.verifyRelationshipIdsReused();
        verifier.verifyLegacyIndex();
    }

    private static void verifySlaveContents( GraphDatabaseService haDb )
    {
        DatabaseContentVerifier verifier = new DatabaseContentVerifier( haDb, 1 );
        verifyNumberOfNodesAndRelationships( verifier );
        verifier.verifyLegacyIndex();
    }


    private static void verifyNumberOfNodesAndRelationships( DatabaseContentVerifier verifier )
    {
        verifier.verifyNodes( 1_000 );
        verifier.verifyRelationships( 500 );
    }

    private static void verifyNeoStore( NeoStores neoStores )
    {
        MetaDataStore metaDataStore = neoStores.getMetaDataStore();
        assertEquals( 1409818980890L, metaDataStore.getCreationTime() );
        assertEquals( 7528833218632030901L, metaDataStore.getRandomNumber() );
        assertEquals( 1L, metaDataStore.getCurrentLogVersion() );
        assertEquals( ALL_STORES_VERSION, MetaDataStore.versionLongToString(
                metaDataStore.getStoreVersion() ) );
        assertEquals( 8L + 3, metaDataStore.getLastCommittedTransactionId() ); // prior verifications add 3 transactions
    }

    private void assertNoDuplicates( List<Token> tokens )
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
            for ( Node node : db.getAllNodes() )
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
        Config allowUpgrade = new Config( MapUtil.stringMap( GraphDatabaseSettings
                .allow_store_upgrade.name(), "true" ) );
        StoreUpgrader upgrader =
                new StoreUpgrader(upgradableDatabase, monitor, allowUpgrade, fs, NullLogProvider.getInstance() );
        upgrader.addParticipant( new SchemaIndexMigrator( fs, schemaIndexProvider, labelScanStoreProvider ) );
        upgrader.addParticipant(
                new StoreMigrator( fs, pageCache, config, NullLogService.getInstance(), schemaIndexProvider ) );
        return upgrader;
    }
}
