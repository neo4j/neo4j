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
package org.neo4j.kernel.impl.storemigration;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;

import org.neo4j.function.Function;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.inmemory.InMemoryIndexProvider;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.TransactionId;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyStoreVersionCheck;
import org.neo4j.kernel.impl.storemigration.legacystore.v19.Legacy19Store;
import org.neo4j.kernel.impl.storemigration.legacystore.v20.Legacy20Store;
import org.neo4j.kernel.impl.storemigration.legacystore.v21.Legacy21Store;
import org.neo4j.kernel.impl.storemigration.legacystore.v22.Legacy22Store;
import org.neo4j.kernel.impl.storemigration.monitoring.SilentMigrationProgressMonitor;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.test.PageCacheRule;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TargetDirectory.TestDirectory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_LOG_BYTE_OFFSET;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_LOG_VERSION;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.UNKNOWN_TX_COMMIT_TIMESTAMP;

@RunWith( Parameterized.class )
public class StoreMigratorIT
{
    @Rule
    public final TestDirectory directory = TargetDirectory.testDirForTest( getClass() );
    @Rule
    public final PageCacheRule pageCacheRule = new PageCacheRule();
    public final FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
    private final SchemaIndexProvider schemaIndexProvider = new InMemoryIndexProvider();

    @Parameterized.Parameter( 0 )
    public String version;

    @Parameterized.Parameter( 1 )
    public LogPosition expectedLogPosition;

    @Parameterized.Parameter( 2 )
    public Function<TransactionId, Boolean> txIdComparator;

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<Object[]> versions()
    {
        return Arrays.asList(
                new Object[]{
                        Legacy19Store.LEGACY_VERSION,
                        new LogPosition( BASE_TX_LOG_VERSION, BASE_TX_LOG_BYTE_OFFSET ),
                        txInfoAcceptanceWithEquals( new TransactionId( 4, -163208757247L, 1368619960754L ) )
                },
                new Object[]{
                        Legacy20Store.LEGACY_VERSION,
                        new LogPosition( BASE_TX_LOG_VERSION, BASE_TX_LOG_BYTE_OFFSET ),
                        txInfoAcceptanceOnIdAndTimestamp( 1039, UNKNOWN_TX_COMMIT_TIMESTAMP )
                },
                new Object[]{
                        Legacy21Store.LEGACY_VERSION,
                        new LogPosition( BASE_TX_LOG_VERSION, BASE_TX_LOG_BYTE_OFFSET ),
                        txInfoAcceptanceOnIdAndTimestamp( 38, UNKNOWN_TX_COMMIT_TIMESTAMP)
                },
                new Object[]{
                        Legacy22Store.LEGACY_VERSION,
                        new LogPosition( 2, BASE_TX_LOG_BYTE_OFFSET ),
                        txInfoAcceptanceOnIdAndTimestamp( 38, UNKNOWN_TX_COMMIT_TIMESTAMP )
                }
        );
    }

    private static Function<TransactionId,Boolean> txInfoAcceptanceWithEquals( final TransactionId txInfo )
    {
        return new Function<TransactionId, Boolean>()
        {
            @Override
            public Boolean apply( TransactionId other )
            {
                return other.equals( txInfo );
            }
        };
    }

    static Function<TransactionId,Boolean> txInfoAcceptanceOnIdAndTimestamp( final long id, final long timestamp)
    {
        return new Function<TransactionId, Boolean>()
        {
            @Override
            public Boolean apply( TransactionId txInfo )
            {
                return txInfo.transactionId() == id &&
                       txInfo.commitTimestamp() == timestamp;
            }
        };
    }

    @Test
    public void shouldBeAbleToResumeMigrationOnMoving() throws Exception
    {
        // GIVEN a legacy database
        File storeDirectory = directory.graphDbDir();
        File prepare = directory.directory( "prepare" );
        MigrationTestUtils.prepareSampleLegacyDatabase( version, fs, storeDirectory, prepare );
        // and a state of the migration saying that it has done the actual migration
        LogService logService = NullLogService.getInstance();
        PageCache pageCache = pageCacheRule.getPageCache( fs );
        UpgradableDatabase upgradableDatabase =
                new UpgradableDatabase( new StoreVersionCheck( pageCache ), new LegacyStoreVersionCheck( fs ) );

        String versionToMigrateFrom = upgradableDatabase.checkUpgradeable( storeDirectory );
        SilentMigrationProgressMonitor progressMonitor = new SilentMigrationProgressMonitor();
        StoreMigrator migrator = new StoreMigrator( progressMonitor, fs, pageCache, new Config(), logService );
        File migrationDir = new File( storeDirectory, StoreUpgrader.MIGRATION_DIRECTORY );
        fs.mkdirs( migrationDir );
        migrator.migrate( storeDirectory, migrationDir, schemaIndexProvider, versionToMigrateFrom );

        // WHEN simulating resuming the migration
        progressMonitor = new SilentMigrationProgressMonitor();
        migrator = new StoreMigrator( progressMonitor, fs, pageCache, new Config(), logService );
        migrator.moveMigratedFiles( migrationDir, storeDirectory, versionToMigrateFrom );

        // THEN starting the new store should be successful
        StoreFactory storeFactory =
                new StoreFactory( fs, storeDirectory, pageCache, logService.getInternalLogProvider() );
        storeFactory.openAllNeoStores().close();
    }

    @Test
    public void shouldBeAbleToResumeMigrationOnRebuildingCounts() throws Exception
    {
        // GIVEN a legacy database
        File storeDirectory = directory.graphDbDir();
        File prepare = directory.directory( "prepare" );
        MigrationTestUtils.prepareSampleLegacyDatabase( version, fs, storeDirectory, prepare );
        // and a state of the migration saying that it has done the actual migration
        LogService logService = NullLogService.getInstance();
        PageCache pageCache = pageCacheRule.getPageCache( fs );
        UpgradableDatabase upgradableDatabase =
                new UpgradableDatabase( new StoreVersionCheck( pageCache ), new LegacyStoreVersionCheck( fs ) );

        String versionToMigrateFrom = upgradableDatabase.checkUpgradeable( storeDirectory );
        SilentMigrationProgressMonitor progressMonitor = new SilentMigrationProgressMonitor();
        StoreMigrator migrator = new StoreMigrator( progressMonitor, fs, pageCache, new Config(), logService );
        File migrationDir = new File( storeDirectory, StoreUpgrader.MIGRATION_DIRECTORY );
        fs.mkdirs( migrationDir );
        migrator.migrate( storeDirectory, migrationDir, schemaIndexProvider, versionToMigrateFrom );
        migrator.moveMigratedFiles( migrationDir, storeDirectory, versionToMigrateFrom );

        // WHEN simulating resuming the migration
        progressMonitor = new SilentMigrationProgressMonitor();
        migrator = new StoreMigrator( progressMonitor, fs, pageCache, new Config(), logService );
        migrator.rebuildCounts( storeDirectory, versionToMigrateFrom );

        // THEN starting the new store should be successful
        StoreFactory storeFactory =
                new StoreFactory( fs, storeDirectory, pageCache, logService.getInternalLogProvider() );
        storeFactory.openAllNeoStores().close();
    }

    @Test
    public void shouldComputeTheLastTxLogPositionCorrectly() throws Throwable
    {
        // GIVEN a legacy database
        File storeDirectory = directory.graphDbDir();
        File prepare = directory.directory( "prepare" );
        MigrationTestUtils.prepareSampleLegacyDatabase( version, fs, storeDirectory, prepare );
        // and a state of the migration saying that it has done the actual migration
        LogService logService = NullLogService.getInstance();
        PageCache pageCache = pageCacheRule.getPageCache( fs );
        UpgradableDatabase upgradableDatabase =
                new UpgradableDatabase( new StoreVersionCheck( pageCache ), new LegacyStoreVersionCheck( fs ) );

        String versionToMigrateFrom = upgradableDatabase.checkUpgradeable( storeDirectory );
        SilentMigrationProgressMonitor progressMonitor = new SilentMigrationProgressMonitor();
        StoreMigrator migrator = new StoreMigrator( progressMonitor, fs, pageCache, new Config(), logService );
        File migrationDir = new File( storeDirectory, StoreUpgrader.MIGRATION_DIRECTORY );
        fs.mkdirs( migrationDir );

        // WHEN migrating
        migrator.migrate( storeDirectory, migrationDir, schemaIndexProvider, versionToMigrateFrom );

        // THEN it should compute the correct last tx log position
        assertEquals( expectedLogPosition, migrator.readLastTxLogPosition( migrationDir ) );
    }

    @Test
    public void shouldComputeTheLastTxInfoCorrectly() throws Exception
    {
        // given
        File storeDirectory = directory.graphDbDir();
        File prepare = directory.directory( "prepare" );
        MigrationTestUtils.prepareSampleLegacyDatabase( version, fs, storeDirectory, prepare );
        // and a state of the migration saying that it has done the actual migration
        LogService logService = NullLogService.getInstance();
        PageCache pageCache = pageCacheRule.getPageCache( fs );
        UpgradableDatabase upgradableDatabase =
                new UpgradableDatabase( new StoreVersionCheck( pageCache ), new LegacyStoreVersionCheck( fs ) );

        String versionToMigrateFrom = upgradableDatabase.checkUpgradeable( storeDirectory );
        SilentMigrationProgressMonitor progressMonitor = new SilentMigrationProgressMonitor();
        StoreMigrator migrator = new StoreMigrator( progressMonitor, fs, pageCache, new Config(), logService );
        File migrationDir = new File( storeDirectory, StoreUpgrader.MIGRATION_DIRECTORY );
        fs.mkdir( migrationDir );

        // when
        migrator.migrate( storeDirectory, migrationDir, schemaIndexProvider, versionToMigrateFrom );

        // then
        assertTrue( txIdComparator.apply( migrator.readLastTxInformation( migrationDir ) ) );
    }
}
