/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.upgrade;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.Path;
import java.util.stream.Stream;

import org.neo4j.collection.Dependencies;
import org.neo4j.common.ProgressReporter;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.batchimport.BatchImporterFactory;
import org.neo4j.internal.batchimport.IndexImporterFactory;
import org.neo4j.internal.recordstorage.RecordStorageEngineFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_4;
import org.neo4j.kernel.impl.storemigration.IdGeneratorMigrator;
import org.neo4j.kernel.impl.storemigration.LegacyTransactionLogsLocator;
import org.neo4j.kernel.impl.storemigration.LogsUpgrader;
import org.neo4j.kernel.impl.storemigration.MigrationTestUtils;
import org.neo4j.kernel.impl.storemigration.RecordStorageMigrator;
import org.neo4j.kernel.impl.storemigration.RecordStoreVersionCheck;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.Monitors;
import org.neo4j.monitoring.PanicEventGenerator;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreVersionCheck;
import org.neo4j.storageengine.migration.MigrationProgressMonitor;
import org.neo4j.storageengine.migration.SchemaIndexMigrator;
import org.neo4j.storageengine.migration.StoreMigrationParticipant;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheSupportExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;
import org.neo4j.test.utils.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.allow_upgrade;
import static org.neo4j.consistency.store.StoreAssertions.assertConsistentStore;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.checkNeoStoreHasFormatVersion;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

@TestDirectoryExtension
public class StoreUpgraderInterruptionTestIT
{
    private static final Config CONFIG = Config.defaults( GraphDatabaseSettings.pagecache_memory, "8m" );
    private final BatchImporterFactory batchImporterFactory = BatchImporterFactory.withHighestPriority();

    @RegisterExtension
    static PageCacheSupportExtension pageCacheExtension = new PageCacheSupportExtension();
    @Inject
    private TestDirectory directory;
    @Inject
    private FileSystemAbstraction fs;

    private static Stream<Arguments> versions()
    {
        return Stream.of( Arguments.of( StandardV3_4.STORE_VERSION ) );
    }

    private JobScheduler jobScheduler;
    private Neo4jLayout neo4jLayout;
    private RecordDatabaseLayout workingDatabaseLayout;
    private Path prepareDirectory;
    private LegacyTransactionLogsLocator legacyTransactionLogsLocator;
    private PageCache pageCache;
    private RecordFormats baselineFormat;
    private RecordFormats successorFormat;

    public void init( String version )
    {
        jobScheduler = new ThreadPoolJobScheduler();
        neo4jLayout = Neo4jLayout.of( directory.homePath() );
        workingDatabaseLayout = RecordDatabaseLayout.of( neo4jLayout, DEFAULT_DATABASE_NAME );
        prepareDirectory = directory.directory( "prepare" );
        legacyTransactionLogsLocator = new LegacyTransactionLogsLocator( Config.defaults(), workingDatabaseLayout );
        pageCache = pageCacheExtension.getPageCache( fs );
        baselineFormat = RecordFormatSelector.selectForVersion( version );
        successorFormat = RecordFormatSelector.findLatestFormatInFamily( baselineFormat ).orElse( baselineFormat );
    }

    @AfterEach
    public void tearDown() throws Exception
    {
        jobScheduler.close();
    }

    @ParameterizedTest
    @MethodSource( "versions" )
    public void shouldSucceedWithUpgradeAfterPreviousAttemptDiedDuringMigration( String version )
            throws IOException, ConsistencyCheckIncompleteException
    {
        init( version );
        MigrationTestUtils.prepareSampleLegacyDatabase( version, fs, workingDatabaseLayout.databaseDirectory(), prepareDirectory );
        RecordStoreVersionCheck versionCheck = new RecordStoreVersionCheck( fs, pageCache, workingDatabaseLayout, NullLogProvider.getInstance(),
                Config.defaults(), NULL );
        MigrationProgressMonitor progressMonitor = MigrationProgressMonitor.SILENT;
        LogService logService = NullLogService.getInstance();
        RecordStorageMigrator failingStoreMigrator = new RecordStorageMigrator( fs, pageCache, CONFIG, logService, jobScheduler, NULL, batchImporterFactory,
                INSTANCE )
        {
            @Override
            public void migrate( DatabaseLayout directoryLayout, DatabaseLayout migrationLayout,
                    ProgressReporter progressReporter, String versionToMigrateFrom, String versionToMigrateTo,
                    IndexImporterFactory indexImporterFactory ) throws IOException, KernelException
            {
                super.migrate( directoryLayout, migrationLayout, progressReporter, versionToMigrateFrom, versionToMigrateTo, indexImporterFactory );
                throw new RuntimeException( "This upgrade is failing" );
            }
        };

        try
        {
            newUpgrader( versionCheck, progressMonitor, createIndexMigrator(), failingStoreMigrator )
                    .migrateIfNeeded( workingDatabaseLayout, false );
            fail( "Should throw exception" );
        }
        catch ( RuntimeException e )
        {
            assertEquals( "This upgrade is failing", e.getMessage() );
        }

        assertTrue( checkNeoStoreHasFormatVersion( versionCheck, baselineFormat ) );

        RecordStorageMigrator migrator = new RecordStorageMigrator( fs, pageCache, CONFIG, logService, jobScheduler, NULL, batchImporterFactory, INSTANCE );
        IdGeneratorMigrator idMigrator = new IdGeneratorMigrator( fs, pageCache, CONFIG, NULL );
        SchemaIndexMigrator indexMigrator = createIndexMigrator();
        newUpgrader( versionCheck, progressMonitor, indexMigrator, migrator, idMigrator ).migrateIfNeeded( workingDatabaseLayout, false );

        assertTrue( checkNeoStoreHasFormatVersion( versionCheck, successorFormat ) );

        // Since consistency checker is in read only mode we need to start/stop db to generate label scan store.
        startStopDatabase( neo4jLayout.homeDirectory() );
        assertConsistentStore( workingDatabaseLayout );
    }

    private SchemaIndexMigrator createIndexMigrator()
    {
        return new SchemaIndexMigrator( "upgrade test indexes", fs, pageCache, IndexProvider.EMPTY.directoryStructure(),
                StorageEngineFactory.defaultStorageEngine(), true );
    }

    @ParameterizedTest
    @MethodSource( "versions" )
    public void tracePageCacheAccessOnIdStoreUpgrade( String version ) throws IOException, ConsistencyCheckIncompleteException
    {
        init( version );
        MigrationTestUtils.prepareSampleLegacyDatabase( version, fs, workingDatabaseLayout.databaseDirectory(), prepareDirectory );
        RecordStoreVersionCheck versionCheck = new RecordStoreVersionCheck( fs, pageCache, workingDatabaseLayout, NullLogProvider.getInstance(),
                Config.defaults(), NULL );
        MigrationProgressMonitor progressMonitor = MigrationProgressMonitor.SILENT;
        LogService logService = NullLogService.getInstance();
        var idMigratorTracer = new DefaultPageCacheTracer();
        var recordMigratorTracer = new DefaultPageCacheTracer();
        IdGeneratorMigrator idMigrator = new IdGeneratorMigrator( fs, pageCache, CONFIG, idMigratorTracer );

        assertTrue( checkNeoStoreHasFormatVersion( versionCheck, baselineFormat ) );

        var migrator = new RecordStorageMigrator( fs, pageCache, CONFIG, logService, jobScheduler, recordMigratorTracer, batchImporterFactory, INSTANCE );
        newUpgrader( versionCheck, progressMonitor, createIndexMigrator(), migrator, idMigrator ).migrateIfNeeded( workingDatabaseLayout, false );

        assertTrue( checkNeoStoreHasFormatVersion( versionCheck, successorFormat ) );

        startStopDatabase( neo4jLayout.homeDirectory() );
        assertConsistentStore( workingDatabaseLayout );

        assertEquals( 126, idMigratorTracer.pins() );
        assertEquals( 126, idMigratorTracer.unpins() );

        assertEquals( 231, recordMigratorTracer.pins() );
        assertEquals( 231, recordMigratorTracer.unpins() );
    }

    @ParameterizedTest
    @MethodSource( "versions" )
    public void shouldSucceedWithUpgradeAfterPreviousAttemptDiedDuringMovingFiles( String version )
            throws IOException, ConsistencyCheckIncompleteException
    {
        init( version );
        MigrationTestUtils.prepareSampleLegacyDatabase( version, fs, workingDatabaseLayout.databaseDirectory(), prepareDirectory );
        RecordStoreVersionCheck versionCheck = new RecordStoreVersionCheck( fs, pageCache, workingDatabaseLayout, NullLogProvider.getInstance(),
                Config.defaults(), NULL );
        MigrationProgressMonitor progressMonitor = MigrationProgressMonitor.SILENT;
        LogService logService = NullLogService.getInstance();
        RecordStorageMigrator failingStoreMigrator = new RecordStorageMigrator( fs, pageCache, CONFIG, logService, jobScheduler, NULL, batchImporterFactory,
                INSTANCE )
        {
            @Override
            public void moveMigratedFiles( DatabaseLayout migrationLayout, DatabaseLayout directoryLayout, String versionToUpgradeFrom,
                    String versionToMigrateTo ) throws IOException
            {
                super.moveMigratedFiles( migrationLayout, directoryLayout, versionToUpgradeFrom, versionToMigrateTo );
                throw new RuntimeException( "This upgrade is failing" );
            }
        };
        IdGeneratorMigrator idMigrator = new IdGeneratorMigrator( fs, pageCache, CONFIG, NULL );

        assertTrue( checkNeoStoreHasFormatVersion( versionCheck, baselineFormat ) );

        try
        {
            newUpgrader( versionCheck, progressMonitor, createIndexMigrator(), failingStoreMigrator, idMigrator )
                    .migrateIfNeeded( workingDatabaseLayout, false );
            fail( "Should throw exception" );
        }
        catch ( RuntimeException e )
        {
            assertEquals( "This upgrade is failing", e.getMessage() );
        }

        RecordStorageMigrator migrator = new RecordStorageMigrator( fs, pageCache, CONFIG, logService, jobScheduler, NULL, batchImporterFactory, INSTANCE );
        newUpgrader( versionCheck, progressMonitor, createIndexMigrator(), migrator, idMigrator ).migrateIfNeeded( workingDatabaseLayout, false );

        assertTrue( checkNeoStoreHasFormatVersion( versionCheck, successorFormat ) );

        pageCache.close();

        // Since consistency checker is in read only mode we need to start/stop db to generate label scan store.
        startStopDatabase( neo4jLayout.homeDirectory() );
        assertConsistentStore( workingDatabaseLayout );
    }

    private StoreUpgrader newUpgrader( StoreVersionCheck versionCheck, MigrationProgressMonitor progressMonitor, StoreMigrationParticipant... participants )
    {
        Config config = Config.defaults( allow_upgrade, true );

        Dependencies dependencies = new Dependencies();
        dependencies.satisfyDependencies( new Monitors() );
        RecordStorageEngineFactory storageEngineFactory = new RecordStorageEngineFactory();
        var databaseHealth = new DatabaseHealth( PanicEventGenerator.NO_OP, NullLog.getInstance() );
        LogsUpgrader logsUpgrader = new LogsUpgrader( fs, storageEngineFactory, workingDatabaseLayout, pageCache, legacyTransactionLogsLocator,
                config, dependencies, NULL, INSTANCE, databaseHealth );
        StoreUpgrader upgrader =
                new StoreUpgrader( storageEngineFactory, versionCheck, progressMonitor, config, fs, NullLogProvider.getInstance(), logsUpgrader, NULL );
        for ( StoreMigrationParticipant participant : participants )
        {
            upgrader.addParticipant( participant );
        }
        return upgrader;
    }

    private static void startStopDatabase( Path storeDir )
    {
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder( storeDir ).setConfig( allow_upgrade, true ).build();
        managementService.database( DEFAULT_DATABASE_NAME );
        managementService.shutdown();
    }
}
