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
package upgrade;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.EnterpriseGraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.schema.LuceneSchemaIndexProvider;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.api.scan.InMemoryLabelScanStore;
import org.neo4j.kernel.impl.api.scan.LabelScanStoreProvider;
import org.neo4j.kernel.impl.ha.ClusterManager;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_0;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.impl.storemigration.StoreVersionCheck;
import org.neo4j.kernel.impl.storemigration.UpgradableDatabase;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyStoreVersionCheck;
import org.neo4j.kernel.impl.storemigration.participant.SchemaIndexMigrator;
import org.neo4j.kernel.impl.storemigration.participant.StoreMigrator;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.PageCacheRule;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.consistency.store.StoreAssertions.assertConsistentStore;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.ha.ClusterManager.allSeesAllAsAvailable;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.find20FormatStoreDirectory;
import static upgrade.StoreMigratorTestUtil.buildClusterWithMasterDirIn;

public class StoreMigratorFrom20IT
{
    @Rule
    public final TargetDirectory.TestDirectory storeDir = TargetDirectory.testDirForTest( getClass() );
    @Rule
    public final PageCacheRule pageCacheRule = new PageCacheRule();

    private final Config config = new Config( configMap(), GraphDatabaseSettings.class );

    private final FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
    private final ListAccumulatorMigrationProgressMonitor monitor = new ListAccumulatorMigrationProgressMonitor();
    private StoreFactory storeFactory;
    private PageCache pageCache;
    private final LifeSupport life = new LifeSupport();
    private UpgradableDatabase upgradableDatabase;

    private SchemaIndexProvider schemaIndexProvider;
    private LabelScanStoreProvider labelScanStoreProvider;

    @Before
    public void setUp()
    {
        pageCache = pageCacheRule.getPageCache( fs );

        schemaIndexProvider = new LuceneSchemaIndexProvider( fs, DirectoryFactory.PERSISTENT, storeDir.directory() );
        labelScanStoreProvider = new LabelScanStoreProvider( new InMemoryLabelScanStore(), 1 );

        storeFactory = new StoreFactory( storeDir.directory(), config, new DefaultIdGeneratorFactory( fs ),
                pageCache, fs, StandardV3_0.RECORD_FORMATS, NullLogProvider.getInstance() );
        upgradableDatabase = new UpgradableDatabase( fs, new StoreVersionCheck( pageCache ),
                new LegacyStoreVersionCheck( fs ), StandardV3_0.RECORD_FORMATS );
    }

    @After
    public void tearDown()
    {
        life.shutdown();
    }

    @Test
    public void shouldMigrate() throws IOException, ConsistencyCheckIncompleteException
    {
        // WHEN
        StoreMigrator storeMigrator = new StoreMigrator( fs, pageCache, config, NullLogService.getInstance(),
                schemaIndexProvider );
        SchemaIndexMigrator indexMigrator = new SchemaIndexMigrator( fs, schemaIndexProvider, labelScanStoreProvider );
        upgrader( indexMigrator, storeMigrator ).migrateIfNeeded( find20FormatStoreDirectory( storeDir.directory() ) );

        // THEN
        assertEquals( 2, monitor.progresses().size() );
        assertTrue( monitor.isStarted() );
        assertTrue( monitor.isFinished() );

        GraphDatabaseService database = new EnterpriseGraphDatabaseFactory().newEmbeddedDatabase( storeDir.absolutePath() );
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
        assertConsistentStore( storeDir.directory(), config );
    }

    @Test
    public void shouldMigrateCluster() throws Throwable
    {
        // Given
        File legacyStoreDir = find20FormatStoreDirectory( storeDir.directory() );

        // When
        StoreMigrator storeMigrator = new StoreMigrator( fs, pageCache, config, NullLogService.getInstance(),
                schemaIndexProvider );
        SchemaIndexMigrator indexMigrator = new SchemaIndexMigrator( fs, schemaIndexProvider, labelScanStoreProvider );
        upgrader( indexMigrator, storeMigrator ).migrateIfNeeded( legacyStoreDir );
        ClusterManager.ManagedCluster cluster = buildClusterWithMasterDirIn( fs, legacyStoreDir, life );
        cluster.await( allSeesAllAsAvailable() );
        cluster.sync();

        // Then
        HighlyAvailableGraphDatabase slave1 = cluster.getAnySlave();
        verifySlaveContents( slave1 );
        verifySlaveContents( cluster.getAnySlave( slave1 ) );
        verifyDatabaseContents( cluster.getMaster() );
    }

    private static void verifyDatabaseContents( GraphDatabaseService database )
    {
        DatabaseContentVerifier verifier = new DatabaseContentVerifier( database, 2 );
        verifyNumberOfNodesAndRelationships( verifier );
        verifier.verifyNodeIdsReused();
        verifier.verifyRelationshipIdsReused();
        verifier.verifyLegacyIndex();
        verifier.verifyIndex();
        verifier.verifyJohnnyLabels();
    }

    private static void verifySlaveContents( HighlyAvailableGraphDatabase haDb )
    {
        DatabaseContentVerifier verifier = new DatabaseContentVerifier( haDb, 2 );
        verifyNumberOfNodesAndRelationships( verifier );
    }

    private static void verifyNumberOfNodesAndRelationships( DatabaseContentVerifier verifier )
    {
        verifier.verifyNodes( 502 );
        verifier.verifyRelationships( 500 );
    }

    public static void verifyNeoStore( NeoStores neoStores )
    {
        MetaDataStore metaDataStore = neoStores.getMetaDataStore();
        assertEquals( 1317392957120L, metaDataStore.getCreationTime() );
        assertEquals( -472309512128245482L, metaDataStore.getRandomNumber() );
        assertEquals( 5L, metaDataStore.getCurrentLogVersion() );
        assertEquals( StandardV3_0.RECORD_FORMATS.storeVersion(),
                MetaDataStore.versionLongToString( metaDataStore.getStoreVersion() ) );
        assertEquals( 1042L, metaDataStore.getLastCommittedTransactionId() );
    }

    private StoreUpgrader upgrader( SchemaIndexMigrator indexMigrator, StoreMigrator storeMigrator )
    {
        Map<String,String> params = config.getParams();
        params.put( GraphDatabaseSettings.allow_store_upgrade.name(), "true" );
        Config config = new Config( params );
        StoreUpgrader upgrader = new StoreUpgrader( upgradableDatabase, monitor, config, fs,
                NullLogProvider.getInstance() );
        upgrader.addParticipant( indexMigrator );
        upgrader.addParticipant( storeMigrator );
        return upgrader;
    }

    private Map<String,String> configMap()
    {
        return stringMap( GraphDatabaseSettings.record_format.name(), StandardV3_0.NAME );
    }
}
