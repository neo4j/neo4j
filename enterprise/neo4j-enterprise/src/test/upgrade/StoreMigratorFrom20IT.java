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

import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.api.index.inmemory.InMemoryIndexProvider;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.storemigration.MigrationTestUtils;
import org.neo4j.kernel.impl.storemigration.StoreMigrator;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.PageCacheRule;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.ha.ClusterManager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.consistency.store.StoreAssertions.assertConsistentStore;
import static org.neo4j.kernel.impl.store.CommonAbstractStore.ALL_STORES_VERSION;
import static org.neo4j.kernel.impl.store.NeoStore.versionLongToString;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.find20FormatStoreDirectory;
import static org.neo4j.kernel.impl.storemigration.UpgradeConfiguration.ALLOW_UPGRADE;
import static org.neo4j.test.ha.ClusterManager.allSeesAllAsAvailable;
import static upgrade.StoreMigratorTestUtil.buildClusterWithMasterDirIn;

public class StoreMigratorFrom20IT
{
    private final SchemaIndexProvider schemaIndexProvider = new InMemoryIndexProvider();

    @Test
    public void shouldMigrate() throws IOException, ConsistencyCheckIncompleteException
    {
        // WHEN
        upgrader( new StoreMigrator( monitor, fs, DevNullLoggingService.DEV_NULL ) )
                .migrateIfNeeded(
                find20FormatStoreDirectory( storeDir.directory() ), schemaIndexProvider, pageCache );

        // THEN
        assertEquals( 100, monitor.eventSize() );
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

        try ( NeoStore neoStore = storeFactory.newNeoStore( true ) )
        {
            verifyNeoStore( neoStore );
        }
        assertConsistentStore( storeDir.directory() );
    }

    @Test
    public void shouldMigrateCluster() throws Throwable
    {
        // Given
        File legacyStoreDir = find20FormatStoreDirectory( storeDir.directory() );

        // When
        upgrader( new StoreMigrator( monitor, fs, DevNullLoggingService.DEV_NULL ) ).migrateIfNeeded(
                legacyStoreDir, schemaIndexProvider, pageCache );
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

    public static void verifyNeoStore( NeoStore neoStore )
    {
        assertEquals( 1317392957120L, neoStore.getCreationTime() );
        assertEquals( -472309512128245482l, neoStore.getRandomNumber() );
        assertEquals( 6l, neoStore.getCurrentLogVersion() );
        assertEquals( ALL_STORES_VERSION, versionLongToString( neoStore.getStoreVersion() ) );
        assertEquals( 1042l, neoStore.getLastCommittedTransactionId() );
    }

    private StoreUpgrader upgrader( StoreMigrator storeMigrator )
    {
        DevNullLoggingService logging = new DevNullLoggingService();
        StoreUpgrader upgrader = new StoreUpgrader( ALLOW_UPGRADE, fs, StoreUpgrader.NO_MONITOR, logging );
        upgrader.addParticipant( storeMigrator );
        return upgrader;
    }

    @Rule
    public final TargetDirectory.TestDirectory storeDir = TargetDirectory.testDirForTest( getClass() );
    @Rule
    public final PageCacheRule pageCacheRule = new PageCacheRule();

    private final FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
    private final ListAccumulatorMigrationProgressMonitor monitor = new ListAccumulatorMigrationProgressMonitor();
    private StoreFactory storeFactory;
    private PageCache pageCache;
    private final LifeSupport life = new LifeSupport();

    @Before
    public void setUp()
    {
        Config config = MigrationTestUtils.defaultConfig();
        pageCache = pageCacheRule.getPageCache( fs );

        storeFactory = new StoreFactory(
                StoreFactory.configForStoreDir( config, storeDir.directory() ),
                new DefaultIdGeneratorFactory(),
                pageCache,
                fs,
                StringLogger.DEV_NULL,
                new Monitors() );
    }

    @After
    public void tearDown()
    {
        life.shutdown();
    }
}
