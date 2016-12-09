/*
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

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.inmemory.InMemoryIndexProvider;
import org.neo4j.kernel.impl.api.scan.InMemoryLabelScanStore;
import org.neo4j.kernel.impl.api.scan.LabelScanStoreProvider;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_0;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_1;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_2;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_3;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_0_7;
import org.neo4j.kernel.impl.storemigration.MigrationTestUtils;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.impl.storemigration.StoreVersionCheck;
import org.neo4j.kernel.impl.storemigration.UpgradableDatabase;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyStoreVersionCheck;
import org.neo4j.kernel.impl.storemigration.monitoring.MigrationProgressMonitor;
import org.neo4j.kernel.impl.storemigration.monitoring.SilentMigrationProgressMonitor;
import org.neo4j.kernel.impl.storemigration.participant.SchemaIndexMigrator;
import org.neo4j.kernel.impl.storemigration.participant.StoreMigrator;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.PageCacheRule;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TargetDirectory.TestDirectory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.consistency.store.StoreAssertions.assertConsistentStore;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.allLegacyStoreFilesHaveVersion;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.allStoreFilesHaveNoTrailer;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.checkNeoStoreHasDefaultFormatVersion;

@RunWith( Parameterized.class )
public class StoreUpgraderInterruptionTestIT
{
    @Parameterized.Parameter
    public String version;
    private final SchemaIndexProvider schemaIndexProvider = new InMemoryIndexProvider();
    private final LabelScanStoreProvider labelScanStoreProvider = new LabelScanStoreProvider( new
            InMemoryLabelScanStore(), 2 );

    @Parameters( name = "{0}" )
    public static Collection<String> versions()
    {
        return Arrays.asList(
                StandardV2_0.STORE_VERSION,
                StandardV2_1.STORE_VERSION,
                StandardV2_2.STORE_VERSION,
                StandardV2_3.STORE_VERSION
        );
    }

    @Rule
    public final TestDirectory directory = TargetDirectory.testDirForTest( getClass() );
    @Rule
    public final PageCacheRule pageCacheRule = new PageCacheRule();
    private final FileSystemAbstraction fs = new DefaultFileSystemAbstraction();

    @Test
    public void shouldSucceedWithUpgradeAfterPreviousAttemptDiedDuringMigration()
            throws IOException, ConsistencyCheckIncompleteException
    {
        File workingDirectory = directory.directory( "working" );
        File prepareDirectory = directory.directory( "prepare" );
        MigrationTestUtils.prepareSampleLegacyDatabase( version, fs, workingDirectory, prepareDirectory );
        PageCache pageCache = pageCacheRule.getPageCache( fs );
        StoreVersionCheck check = new StoreVersionCheck( pageCache );
        UpgradableDatabase upgradableDatabase =
                new UpgradableDatabase( fs, check, new LegacyStoreVersionCheck( fs ), StandardV3_0_7.RECORD_FORMATS );
        SilentMigrationProgressMonitor progressMonitor = new SilentMigrationProgressMonitor();
        LogService logService = NullLogService.getInstance();
        final Config config = Config.empty();
        StoreMigrator failingStoreMigrator = new StoreMigrator(fs, pageCache, config, logService, schemaIndexProvider )
        {
            @Override
            public void migrate( File sourceStoreDir, File targetStoreDir,
                    MigrationProgressMonitor.Section progressMonitor,
                    String versionToMigrateFrom, String versionToMigrateTo ) throws IOException
            {
                super.migrate( sourceStoreDir, targetStoreDir, progressMonitor, versionToMigrateFrom,
                        versionToMigrateTo );
                throw new RuntimeException( "This upgrade is failing" );
            }
        };

        assertEquals( !StandardV2_3.STORE_VERSION.equals( version ),
                allLegacyStoreFilesHaveVersion( fs, workingDirectory, version ) );

        try
        {
            newUpgrader( upgradableDatabase, progressMonitor, createIndexMigrator(), failingStoreMigrator )
                    .migrateIfNeeded( workingDirectory );
            fail( "Should throw exception" );
        }
        catch ( RuntimeException e )
        {
            assertEquals( "This upgrade is failing", e.getMessage() );
        }

        assertEquals( !StandardV2_3.STORE_VERSION.equals( version ),
                allLegacyStoreFilesHaveVersion( fs, workingDirectory, version ) );

        progressMonitor = new SilentMigrationProgressMonitor();
        StoreMigrator migrator = new StoreMigrator( fs, pageCache, config, logService, schemaIndexProvider );
        SchemaIndexMigrator indexMigrator = createIndexMigrator();
        newUpgrader(upgradableDatabase, progressMonitor, indexMigrator, migrator ).migrateIfNeeded( workingDirectory );

        assertTrue( checkNeoStoreHasDefaultFormatVersion( check, workingDirectory ) );
        assertTrue( allStoreFilesHaveNoTrailer( fs, workingDirectory ) );

        // Since consistency checker is in read only mode we need to start/stop db to generate label scan store.
        startStopDatabase( workingDirectory );
        assertConsistentStore( workingDirectory );
    }

    private SchemaIndexMigrator createIndexMigrator()
    {
        return new SchemaIndexMigrator( fs, schemaIndexProvider, labelScanStoreProvider );
    }

    @Test
    public void shouldSucceedWithUpgradeAfterPreviousAttemptDiedDuringMovingFiles()
            throws IOException, ConsistencyCheckIncompleteException
    {
        File workingDirectory = directory.directory( "working" );
        File prepareDirectory = directory.directory( "prepare" );
        MigrationTestUtils.prepareSampleLegacyDatabase( version, fs, workingDirectory, prepareDirectory );
        PageCache pageCache = pageCacheRule.getPageCache( fs );
        StoreVersionCheck check = new StoreVersionCheck( pageCache );
        UpgradableDatabase upgradableDatabase =
                new UpgradableDatabase( fs, check, new LegacyStoreVersionCheck( fs ), StandardV3_0_7.RECORD_FORMATS );
        SilentMigrationProgressMonitor progressMonitor = new SilentMigrationProgressMonitor();
        LogService logService = NullLogService.getInstance();
        final Config config = Config.empty();
        StoreMigrator failingStoreMigrator = new StoreMigrator( fs, pageCache, config, logService, schemaIndexProvider )
        {
            @Override
            public void moveMigratedFiles( File migrationDir, File storeDir, String versionToUpgradeFrom,
                    String versionToMigrateTo ) throws IOException
            {
                super.moveMigratedFiles( migrationDir, storeDir, versionToUpgradeFrom, versionToMigrateTo );
                throw new RuntimeException( "This upgrade is failing" );
            }
        };

        assertEquals( !StandardV2_3.STORE_VERSION.equals( version ),
                allLegacyStoreFilesHaveVersion( fs, workingDirectory, version ) );

        try
        {
            newUpgrader( upgradableDatabase, progressMonitor, createIndexMigrator(), failingStoreMigrator )
                    .migrateIfNeeded( workingDirectory );
            fail( "Should throw exception" );
        }
        catch ( RuntimeException e )
        {
            assertEquals( "This upgrade is failing", e.getMessage() );
        }

        assertTrue( checkNeoStoreHasDefaultFormatVersion( check, workingDirectory ) );
        assertTrue( allStoreFilesHaveNoTrailer( fs, workingDirectory ) );

        progressMonitor = new SilentMigrationProgressMonitor();
        StoreMigrator migrator = new StoreMigrator( fs, pageCache, config, logService, schemaIndexProvider );
        newUpgrader( upgradableDatabase, progressMonitor, createIndexMigrator(), migrator )
                .migrateIfNeeded( workingDirectory );

        assertTrue( checkNeoStoreHasDefaultFormatVersion( check, workingDirectory ) );
        assertTrue( allStoreFilesHaveNoTrailer( fs, workingDirectory ) );

        pageCache.close();

        // Since consistency checker is in read only mode we need to start/stop db to generate label scan store.
        startStopDatabase( workingDirectory );
        assertConsistentStore( workingDirectory );
    }

    private StoreUpgrader newUpgrader(UpgradableDatabase upgradableDatabase, MigrationProgressMonitor progressMonitor,
            SchemaIndexMigrator indexMigrator,
            StoreMigrator migrator )
    {
        Config allowUpgrade = new Config( MapUtil.stringMap( GraphDatabaseSettings
                .allow_store_upgrade.name(), "true" ) );

        StoreUpgrader upgrader = new StoreUpgrader( upgradableDatabase, progressMonitor, allowUpgrade, fs,
                NullLogProvider.getInstance() );
        upgrader.addParticipant( indexMigrator );
        upgrader.addParticipant( migrator );
        return upgrader;
    }

    private void startStopDatabase( File workingDirectory )
    {
        GraphDatabaseService databaseService = new GraphDatabaseFactory().newEmbeddedDatabase( workingDirectory );
        databaseService.shutdown();
    }
}
