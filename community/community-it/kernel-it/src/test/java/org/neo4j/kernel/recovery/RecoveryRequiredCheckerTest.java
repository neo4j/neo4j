/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.extension.EphemeralFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.extension.pagecache.PageCacheSupportExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.DEFAULT_TX_LOGS_ROOT_DIR_NAME;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.transaction_logs_root_path;
import static org.neo4j.kernel.configuration.LayoutConfig.of;

@ExtendWith( {EphemeralFileSystemExtension.class, TestDirectoryExtension.class} )
class RecoveryRequiredCheckerTest
{
    @RegisterExtension
    static PageCacheSupportExtension pageCacheExtension = new PageCacheSupportExtension();
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private EphemeralFileSystemAbstraction fileSystem;

    private final Monitors monitors = new Monitors();

    private File storeDir;
    private DatabaseLayout databaseLayout;

    @BeforeEach
    void setup()
    {
        databaseLayout = testDirectory.databaseLayout();
        storeDir = databaseLayout.databaseDirectory();
        new TestGraphDatabaseFactory().setFileSystem( fileSystem ).newImpermanentDatabase( storeDir ).shutdown();
    }

    @Test
    void shouldNotWantToRecoverIntactStore() throws Exception
    {
        PageCache pageCache = pageCacheExtension.getPageCache( fileSystem );
        RecoveryRequiredChecker recoverer = getRecoveryCheckerWithDefaultConfig( fileSystem, pageCache );

        assertThat( recoverer.isRecoveryRequiredAt( databaseLayout ), is( false ) );
    }

    @Test
    void shouldWantToRecoverBrokenStore() throws Exception
    {
        try ( FileSystemAbstraction fileSystemAbstraction = createAndCrashWithDefaultConfig() )
        {

            PageCache pageCache = pageCacheExtension.getPageCache( fileSystemAbstraction );
            RecoveryRequiredChecker recoverer = getRecoveryCheckerWithDefaultConfig( fileSystemAbstraction, pageCache );

            assertThat( recoverer.isRecoveryRequiredAt( databaseLayout ), is( true ) );
        }
    }

    @Test
    void shouldBeAbleToRecoverBrokenStore() throws Exception
    {
        try ( FileSystemAbstraction fileSystemAbstraction = createAndCrashWithDefaultConfig() )
        {
            PageCache pageCache = pageCacheExtension.getPageCache( fileSystemAbstraction );

            RecoveryRequiredChecker recoverer = getRecoveryCheckerWithDefaultConfig( fileSystemAbstraction, pageCache );

            assertThat( recoverer.isRecoveryRequiredAt( databaseLayout ), is( true ) );

            new TestGraphDatabaseFactory().setFileSystem( fileSystemAbstraction ).newImpermanentDatabase( storeDir ).shutdown();

            assertThat( recoverer.isRecoveryRequiredAt( databaseLayout ), is( false ) );
        }
    }

    @Test
    void shouldBeAbleToRecoverBrokenStoreWithLogsInSeparateAbsoluteLocation() throws Exception
    {
        File customTransactionLogsLocation = testDirectory.directory( DEFAULT_TX_LOGS_ROOT_DIR_NAME );
        Config config = Config.builder().withSetting( transaction_logs_root_path,
                customTransactionLogsLocation.getAbsolutePath() ).build();
        recoverBrokenStoreWithConfig( config );
    }

    private void recoverBrokenStoreWithConfig( Config config ) throws IOException
    {
        try ( FileSystemAbstraction fileSystemAbstraction = createSomeDataAndCrash( storeDir, fileSystem, config ) )
        {
            PageCache pageCache = pageCacheExtension.getPageCache( fileSystemAbstraction );

            RecoveryRequiredChecker recoveryChecker = getRecoveryChecker( fileSystemAbstraction, pageCache, config );

            assertThat( recoveryChecker.isRecoveryRequiredAt( testDirectory.databaseLayout( of( config ) ) ), is( true ) );

            new TestGraphDatabaseFactory()
                    .setFileSystem( fileSystemAbstraction )
                    .newEmbeddedDatabaseBuilder( storeDir )
                    .setConfig( config.getRaw() )
                    .newGraphDatabase()
                    .shutdown();

            assertThat( recoveryChecker.isRecoveryRequiredAt( databaseLayout ), is( false ) );
        }
    }

    private FileSystemAbstraction createAndCrashWithDefaultConfig()
    {
        return createSomeDataAndCrash( storeDir, fileSystem, Config.defaults() );
    }

    private static RecoveryRequiredChecker getRecoveryCheckerWithDefaultConfig( FileSystemAbstraction fileSystem, PageCache pageCache )
    {
        return getRecoveryChecker( fileSystem, pageCache, Config.defaults() );
    }

    private static RecoveryRequiredChecker getRecoveryChecker( FileSystemAbstraction fileSystem, PageCache pageCache, Config config )
    {
        return new RecoveryRequiredChecker( fileSystem, pageCache, config );
    }

    private static FileSystemAbstraction createSomeDataAndCrash( File store, EphemeralFileSystemAbstraction fileSystem, Config config )
    {
        final GraphDatabaseService db = new TestGraphDatabaseFactory()
                        .setFileSystem( fileSystem )
                        .newImpermanentDatabaseBuilder( store )
                        .setConfig( config.getRaw() )
                        .newGraphDatabase();

        try ( Transaction tx = db.beginTx() )
        {
            db.createNode();
            tx.success();
        }

        EphemeralFileSystemAbstraction snapshot = fileSystem.snapshot();
        db.shutdown();
        return snapshot;
    }
}
