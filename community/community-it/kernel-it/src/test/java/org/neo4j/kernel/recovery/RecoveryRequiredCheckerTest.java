/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.recovery;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.io.IOException;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.pagecache.PageCacheSupportExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_TX_LOGS_ROOT_DIR_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_logs_root_path;

@Neo4jLayoutExtension
class RecoveryRequiredCheckerTest
{
    @RegisterExtension
    static PageCacheSupportExtension pageCacheExtension = new PageCacheSupportExtension();
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private DatabaseLayout databaseLayout;

    private File storeDir;
    private final StorageEngineFactory storageEngineFactory = StorageEngineFactory.selectStorageEngine();

    @BeforeEach
    void setup()
    {
        storeDir = testDirectory.homeDir();
    }

    @Test
    void shouldNotWantToRecoverIntactStore() throws Exception
    {
        startStopAndCreateDefaultData();

        PageCache pageCache = pageCacheExtension.getPageCache( fileSystem );
        RecoveryRequiredChecker recoverer = getRecoveryCheckerWithDefaultConfig( fileSystem, pageCache, storageEngineFactory );

        assertThat( recoverer.isRecoveryRequiredAt( databaseLayout ) ).isEqualTo( false );
    }

    @Test
    void shouldWantToRecoverBrokenStore() throws Exception
    {
        try ( EphemeralFileSystemAbstraction ephemeralFs = createAndCrashWithDefaultConfig() )
        {
            PageCache pageCache = pageCacheExtension.getPageCache( ephemeralFs );
            RecoveryRequiredChecker recoverer = getRecoveryCheckerWithDefaultConfig( ephemeralFs, pageCache, storageEngineFactory );

            assertThat( recoverer.isRecoveryRequiredAt( databaseLayout ) ).isEqualTo( true );
        }
    }

    @Test
    void shouldBeAbleToRecoverBrokenStore() throws Exception
    {
        try ( EphemeralFileSystemAbstraction ephemeralFs = createAndCrashWithDefaultConfig() )
        {
            PageCache pageCache = pageCacheExtension.getPageCache( ephemeralFs );

            RecoveryRequiredChecker recoverer = getRecoveryCheckerWithDefaultConfig( ephemeralFs, pageCache, storageEngineFactory );

            assertThat( recoverer.isRecoveryRequiredAt( databaseLayout ) ).isEqualTo( true );

            startStopDatabase( ephemeralFs, storeDir );

            assertThat( recoverer.isRecoveryRequiredAt( databaseLayout ) ).isEqualTo( false );
        }
    }

    @Test
    void shouldBeAbleToRecoverBrokenStoreWithLogsInSeparateAbsoluteLocation() throws Exception
    {
        File customTransactionLogsLocation = testDirectory.directory( DEFAULT_TX_LOGS_ROOT_DIR_NAME );
        Config config = Config.newBuilder()
                .set( GraphDatabaseSettings.neo4j_home, testDirectory.homeDir().toPath() )
                .set( transaction_logs_root_path, customTransactionLogsLocation.toPath().toAbsolutePath() )
                .build();
        recoverBrokenStoreWithConfig( config );
    }

    @Test
    void shouldNotWantToRecoverEmptyStore() throws Exception
    {
        DatabaseLayout databaseLayout = DatabaseLayout.ofFlat( testDirectory.directory( "dir-without-store" ) );

        PageCache pageCache = pageCacheExtension.getPageCache( fileSystem );
        RecoveryRequiredChecker checker = getRecoveryCheckerWithDefaultConfig( fileSystem, pageCache, storageEngineFactory );

        assertFalse( checker.isRecoveryRequiredAt( databaseLayout ) );
    }

    @Test
    void shouldWantToRecoverStoreWithoutOneIdFile() throws Exception
    {
        startStopAndCreateDefaultData();
        assertAllIdFilesExist();

        PageCache pageCache = pageCacheExtension.getPageCache( fileSystem );
        RecoveryRequiredChecker checker = getRecoveryCheckerWithDefaultConfig( fileSystem, pageCache, storageEngineFactory );
        assertFalse( checker.isRecoveryRequiredAt( databaseLayout ) );

        fileSystem.deleteFileOrThrow( databaseLayout.idNodeStore() );

        assertTrue( checker.isRecoveryRequiredAt( databaseLayout ) );
    }

    @Test
    void shouldWantToRecoverStoreWithoutAllIdFiles() throws Exception
    {
        startStopAndCreateDefaultData();
        assertAllIdFilesExist();

        PageCache pageCache = pageCacheExtension.getPageCache( fileSystem );
        RecoveryRequiredChecker checker = getRecoveryCheckerWithDefaultConfig( fileSystem, pageCache, storageEngineFactory );
        assertFalse( checker.isRecoveryRequiredAt( databaseLayout ) );

        for ( File idFile : databaseLayout.idFiles() )
        {
            fileSystem.deleteFileOrThrow( idFile );
        }

        assertTrue( checker.isRecoveryRequiredAt( databaseLayout ) );
    }

    @Test
    void recoveryRequiredWhenAnyStoreFileIsMissing() throws Exception
    {
        startStopAndCreateDefaultData();

        assertStoreFilesExist();

        PageCache pageCache = pageCacheExtension.getPageCache( fileSystem );
        RecoveryRequiredChecker checker = getRecoveryCheckerWithDefaultConfig( fileSystem, pageCache, storageEngineFactory );
        assertFalse( checker.isRecoveryRequiredAt( databaseLayout ) );

        fileSystem.deleteFileOrThrow( databaseLayout.nodeStore() );

        assertTrue( checker.isRecoveryRequiredAt( databaseLayout ) );
    }

    @Test
    void recoveryRequiredWhenSeveralStoreFileAreMissing() throws Exception
    {
        startStopAndCreateDefaultData();

        assertStoreFilesExist();

        PageCache pageCache = pageCacheExtension.getPageCache( fileSystem );
        RecoveryRequiredChecker checker = getRecoveryCheckerWithDefaultConfig( fileSystem, pageCache, storageEngineFactory );
        assertFalse( checker.isRecoveryRequiredAt( databaseLayout ) );

        fileSystem.deleteFileOrThrow( databaseLayout.relationshipStore() );
        fileSystem.deleteFileOrThrow( databaseLayout.propertyStore() );
        fileSystem.deleteFileOrThrow( databaseLayout.relationshipTypeTokenStore() );

        assertTrue( checker.isRecoveryRequiredAt( databaseLayout ) );
    }

    @Test
    void recoveryNotRequiredWhenCountStoreIsMissing() throws Exception
    {
        startStopAndCreateDefaultData();

        assertStoreFilesExist();

        PageCache pageCache = pageCacheExtension.getPageCache( fileSystem );
        RecoveryRequiredChecker checker = getRecoveryCheckerWithDefaultConfig( fileSystem, pageCache, storageEngineFactory );
        assertFalse( checker.isRecoveryRequiredAt( databaseLayout ) );

        fileSystem.deleteFileOrThrow( databaseLayout.countStore() );

        assertFalse( checker.isRecoveryRequiredAt( databaseLayout ) );
    }

    @Test
    void recoveryNotRequiredWhenIndexStatisticStoreIsMissing() throws Exception
    {
        startStopAndCreateDefaultData();

        assertStoreFilesExist();

        PageCache pageCache = pageCacheExtension.getPageCache( fileSystem );
        RecoveryRequiredChecker checker = getRecoveryCheckerWithDefaultConfig( fileSystem, pageCache, storageEngineFactory );
        assertFalse( checker.isRecoveryRequiredAt( databaseLayout ) );

        fileSystem.deleteFileOrThrow( databaseLayout.indexStatisticsStore() );

        assertFalse( checker.isRecoveryRequiredAt( databaseLayout ) );
    }

    private void recoverBrokenStoreWithConfig( Config config ) throws IOException
    {
        try ( EphemeralFileSystemAbstraction ephemeralFs = createSomeDataAndCrash( storeDir, config ) )
        {
            PageCache pageCache = pageCacheExtension.getPageCache( ephemeralFs );

            RecoveryRequiredChecker recoveryChecker = getRecoveryChecker( ephemeralFs, pageCache, storageEngineFactory, config );

            assertThat( recoveryChecker.isRecoveryRequiredAt( DatabaseLayout.of( config ) ) ).isEqualTo( true );

            DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder( storeDir )
                        .setFileSystem( ephemeralFs )
                        .setConfig( config )
                    .build();
            managementService.shutdown();

            assertThat( recoveryChecker.isRecoveryRequiredAt( databaseLayout ) ).isEqualTo( false );
        }
    }

    private EphemeralFileSystemAbstraction createAndCrashWithDefaultConfig() throws IOException
    {
        return createSomeDataAndCrash( storeDir, Config.defaults() );
    }

    private void assertAllIdFilesExist()
    {
        for ( File idFile : databaseLayout.idFiles() )
        {
            assertTrue( fileSystem.fileExists( idFile ), "ID file " + idFile + " does not exist" );
        }
    }

    private void assertStoreFilesExist()
    {
        for ( File file : databaseLayout.storeFiles() )
        {
            assertTrue( fileSystem.fileExists( file ), "Store file " + file + " does not exist" );
        }
    }

    private static RecoveryRequiredChecker getRecoveryCheckerWithDefaultConfig( FileSystemAbstraction fileSystem, PageCache pageCache,
            StorageEngineFactory storageEngineFactory )
    {
        return getRecoveryChecker( fileSystem, pageCache, storageEngineFactory, Config.defaults() );
    }

    private static RecoveryRequiredChecker getRecoveryChecker( FileSystemAbstraction fileSystem, PageCache pageCache,
            StorageEngineFactory storageEngineFactory, Config config )
    {
        return new RecoveryRequiredChecker( fileSystem, pageCache, config, storageEngineFactory );
    }

    private static EphemeralFileSystemAbstraction createSomeDataAndCrash( File store, Config config ) throws IOException
    {
        try ( EphemeralFileSystemAbstraction ephemeralFs = new EphemeralFileSystemAbstraction() )
        {
            DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder( store )
                        .setFileSystem( ephemeralFs )
                        .setConfig( config )
                    .build();
            final GraphDatabaseService db = managementService.database( DEFAULT_DATABASE_NAME );

            try ( Transaction tx = db.beginTx() )
            {
                tx.createNode();
                tx.commit();
            }

            EphemeralFileSystemAbstraction snapshot = ephemeralFs.snapshot();
            managementService.shutdown();
            return snapshot;
        }
    }

    private static DatabaseManagementService startDatabase( FileSystemAbstraction fileSystem, File storeDir )
    {
        return new TestDatabaseManagementServiceBuilder( storeDir )
                .setFileSystem( fileSystem )
                .build();
    }

    private static void startStopDatabase( FileSystemAbstraction fileSystem, File storeDir )
    {
        DatabaseManagementService managementService = startDatabase( fileSystem, storeDir );
        managementService.shutdown();
    }

    private void startStopAndCreateDefaultData()
    {
        DatabaseManagementService managementService = startDatabase( fileSystem, storeDir );
        try
        {
            GraphDatabaseService database = managementService.database( DEFAULT_DATABASE_NAME );
            try ( Transaction transaction = database.beginTx() )
            {
                transaction.createNode();
                transaction.commit();
            }
        }
        finally
        {
            managementService.shutdown();
        }
    }
}
