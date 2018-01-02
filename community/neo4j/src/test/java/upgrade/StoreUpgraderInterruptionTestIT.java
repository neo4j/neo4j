/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.inmemory.InMemoryIndexProvider;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.storemigration.MigrationTestUtils;
import org.neo4j.kernel.impl.storemigration.StoreMigrator;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.impl.storemigration.StoreVersionCheck;
import org.neo4j.kernel.impl.storemigration.UpgradableDatabase;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyStoreVersionCheck;
import org.neo4j.kernel.impl.storemigration.legacystore.v19.Legacy19Store;
import org.neo4j.kernel.impl.storemigration.legacystore.v20.Legacy20Store;
import org.neo4j.kernel.impl.storemigration.legacystore.v21.Legacy21Store;
import org.neo4j.kernel.impl.storemigration.legacystore.v22.Legacy22Store;
import org.neo4j.kernel.impl.storemigration.monitoring.SilentMigrationProgressMonitor;
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
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.checkNeoStoreHasLatestVersion;
import static org.neo4j.kernel.impl.storemigration.UpgradeConfiguration.ALLOW_UPGRADE;

@RunWith( Parameterized.class )
public class StoreUpgraderInterruptionTestIT
{
    @Parameterized.Parameter( 0 )
    public String version;
    private final SchemaIndexProvider schemaIndexProvider = new InMemoryIndexProvider();

    @Parameters( name = "{0}" )
    public static Collection<Object[]> versions()
    {
        return Arrays.asList(
                new Object[]{Legacy19Store.LEGACY_VERSION},
                new Object[]{Legacy20Store.LEGACY_VERSION},
                new Object[]{Legacy21Store.LEGACY_VERSION},
                new Object[]{Legacy22Store.LEGACY_VERSION}
        );
    }

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
                new UpgradableDatabase( check, new LegacyStoreVersionCheck( fs ) );
        SilentMigrationProgressMonitor progressMonitor = new SilentMigrationProgressMonitor();
        LogService logService = NullLogService.getInstance();
        final Config config = new Config();
        StoreMigrator failingStoreMigrator = new StoreMigrator( progressMonitor, fs, pageCache, config, logService )
        {
            @Override
            public void migrate( File sourceStoreDir, File targetStoreDir, SchemaIndexProvider schemaIndexProvider,
                    String versionToMigrateFrom ) throws IOException
            {
                super.migrate( sourceStoreDir, targetStoreDir, schemaIndexProvider, versionToMigrateFrom );
                throw new RuntimeException( "This upgrade is failing" );
            }
        };

        assertTrue( allLegacyStoreFilesHaveVersion( fs, workingDirectory, version ) );

        try
        {
            newUpgrader( failingStoreMigrator )
                    .migrateIfNeeded( workingDirectory, upgradableDatabase, schemaIndexProvider );
            fail( "Should throw exception" );
        }
        catch ( RuntimeException e )
        {
            assertEquals( "This upgrade is failing", e.getMessage() );
        }

        assertTrue( allLegacyStoreFilesHaveVersion( fs, workingDirectory, version ) );

        progressMonitor = new SilentMigrationProgressMonitor();
        StoreMigrator migrator = new StoreMigrator( progressMonitor, fs, pageCache, config, logService );
        newUpgrader( migrator ).migrateIfNeeded( workingDirectory, upgradableDatabase, schemaIndexProvider );

        assertTrue( checkNeoStoreHasLatestVersion( check, workingDirectory ) );
        assertTrue( allStoreFilesHaveNoTrailer( fs, workingDirectory ) );

        // Since consistency checker is in read only mode we need to start/stop db to generate label scan store.
        startStopDatabase( workingDirectory );
        assertConsistentStore( workingDirectory );
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
                new UpgradableDatabase( check, new LegacyStoreVersionCheck( fs ) );
        SilentMigrationProgressMonitor progressMonitor = new SilentMigrationProgressMonitor();
        LogService logService = NullLogService.getInstance();
        final Config config = new Config();
        StoreMigrator failingStoreMigrator = new StoreMigrator( progressMonitor, fs, pageCache, config, logService )
        {
            @Override
            public void moveMigratedFiles( File migrationDir, File storeDir, String versionToUpgradeFrom ) throws IOException
            {
                super.moveMigratedFiles( migrationDir, storeDir, versionToUpgradeFrom );
                throw new RuntimeException( "This upgrade is failing" );
            }
        };

        assertTrue( allLegacyStoreFilesHaveVersion( fs, workingDirectory, version ) );

        try
        {
            newUpgrader( failingStoreMigrator )
                    .migrateIfNeeded( workingDirectory, upgradableDatabase, schemaIndexProvider );
            fail( "Should throw exception" );
        }
        catch ( RuntimeException e )
        {
            assertEquals( "This upgrade is failing", e.getMessage() );
        }

        assertTrue( checkNeoStoreHasLatestVersion( check, workingDirectory ) );
        assertTrue( allStoreFilesHaveNoTrailer( fs, workingDirectory ) );

        progressMonitor = new SilentMigrationProgressMonitor();
        StoreMigrator migrator = new StoreMigrator( progressMonitor, fs, pageCache, config, logService );
        newUpgrader( migrator ).migrateIfNeeded( workingDirectory, upgradableDatabase, schemaIndexProvider );

        assertTrue( checkNeoStoreHasLatestVersion( check, workingDirectory ) );
        assertTrue( allStoreFilesHaveNoTrailer( fs, workingDirectory ) );

        pageCache.close();

        // Since consistency checker is in read only mode we need to start/stop db to generate label scan store.
        startStopDatabase( workingDirectory );
        assertConsistentStore( workingDirectory );
    }

    private StoreUpgrader newUpgrader( StoreMigrator migrator )
    {
        StoreUpgrader upgrader =
                new StoreUpgrader( ALLOW_UPGRADE, fs, StoreUpgrader.NO_MONITOR, NullLogProvider.getInstance() );
        upgrader.addParticipant( migrator );
        return upgrader;
    }

    private void startStopDatabase( File workingDirectory )
    {
        GraphDatabaseService databaseService = new GraphDatabaseFactory().newEmbeddedDatabase( workingDirectory );
        databaseService.shutdown();
    }

    @Rule
    public final TestDirectory directory = TargetDirectory.testDirForTest( getClass() );
    @Rule
    public final PageCacheRule pageCacheRule = new PageCacheRule();
    private final FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
}
