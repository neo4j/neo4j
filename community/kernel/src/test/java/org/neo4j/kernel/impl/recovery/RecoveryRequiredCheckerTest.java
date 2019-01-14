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
package org.neo4j.kernel.impl.recovery;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.logical_logs_location;

public class RecoveryRequiredCheckerTest
{
    private final EphemeralFileSystemRule fileSystemRule = new EphemeralFileSystemRule();
    private final PageCacheRule pageCacheRule = new PageCacheRule();
    private final TestDirectory testDirectory = TestDirectory.testDirectory( fileSystemRule.get() );

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( pageCacheRule ).around( fileSystemRule ).around( testDirectory );

    private final Monitors monitors = new Monitors();
    private EphemeralFileSystemAbstraction fileSystem;
    private File storeDir;

    @Before
    public void setup()
    {
        storeDir = testDirectory.graphDbDir();
        fileSystem = fileSystemRule.get();
        new TestGraphDatabaseFactory().setFileSystem( fileSystem ).newImpermanentDatabase( storeDir ).shutdown();
    }

    @Test
    public void shouldNotWantToRecoverIntactStore() throws Exception
    {
        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        RecoveryRequiredChecker recoverer = getRecoveryCheckerWithDefaultConfig( fileSystem, pageCache );

        assertThat( recoverer.isRecoveryRequiredAt( storeDir ), is( false ) );
    }

    @Test
    public void shouldWantToRecoverBrokenStore() throws Exception
    {
        try ( FileSystemAbstraction fileSystemAbstraction = createAndCrashWithDefaultConfig() )
        {

            PageCache pageCache = pageCacheRule.getPageCache( fileSystemAbstraction );
            RecoveryRequiredChecker recoverer = getRecoveryCheckerWithDefaultConfig( fileSystemAbstraction, pageCache );

            assertThat( recoverer.isRecoveryRequiredAt( storeDir ), is( true ) );
        }
    }

    @Test
    public void shouldBeAbleToRecoverBrokenStore() throws Exception
    {
        try ( FileSystemAbstraction fileSystemAbstraction = createAndCrashWithDefaultConfig() )
        {
            PageCache pageCache = pageCacheRule.getPageCache( fileSystemAbstraction );

            RecoveryRequiredChecker recoverer = getRecoveryCheckerWithDefaultConfig( fileSystemAbstraction, pageCache );

            assertThat( recoverer.isRecoveryRequiredAt( storeDir ), is( true ) );

            new TestGraphDatabaseFactory().setFileSystem( fileSystemAbstraction ).newImpermanentDatabase( storeDir ).shutdown();

            assertThat( recoverer.isRecoveryRequiredAt( storeDir ), is( false ) );
        }
    }

    @Test
    public void shouldBeAbleToRecoverBrokenStoreWithLogsInSeparateRelativeLocation() throws Exception
    {
        File customTransactionLogsLocation = new File( storeDir, "tx-logs" );
        Config config = Config.defaults( logical_logs_location, customTransactionLogsLocation.getName() );
        recoverBrokenStoreWithConfig( config );
    }

    @Test
    public void shouldBeAbleToRecoverBrokenStoreWithLogsInSeparateAbsoluteLocation() throws Exception
    {
        File customTransactionLogsLocation = testDirectory.directory( "tx-logs" );
        Config config = Config.defaults( logical_logs_location, customTransactionLogsLocation.getAbsolutePath() );
        recoverBrokenStoreWithConfig( config );
    }

    private void recoverBrokenStoreWithConfig( Config config ) throws IOException
    {
        try ( FileSystemAbstraction fileSystemAbstraction = createSomeDataAndCrash( storeDir, fileSystem, config ) )
        {
            PageCache pageCache = pageCacheRule.getPageCache( fileSystemAbstraction );

            RecoveryRequiredChecker recoverer = getRecoveryChecker( fileSystemAbstraction, pageCache, config );

            assertThat( recoverer.isRecoveryRequiredAt( storeDir ), is( true ) );

            new TestGraphDatabaseFactory()
                    .setFileSystem( fileSystemAbstraction )
                    .newEmbeddedDatabaseBuilder( storeDir )
                    .setConfig( config.getRaw() )
                    .newGraphDatabase()
                    .shutdown();

            assertThat( recoverer.isRecoveryRequiredAt( storeDir ), is( false ) );
        }
    }

    private FileSystemAbstraction createAndCrashWithDefaultConfig()
    {
        return createSomeDataAndCrash( storeDir, fileSystem, Config.defaults() );
    }

    private RecoveryRequiredChecker getRecoveryCheckerWithDefaultConfig( FileSystemAbstraction fileSystem, PageCache pageCache )
    {
        return getRecoveryChecker( fileSystem, pageCache, Config.defaults() );
    }

    private RecoveryRequiredChecker getRecoveryChecker( FileSystemAbstraction fileSystem, PageCache pageCache, Config config )
    {
        return new RecoveryRequiredChecker( fileSystem, pageCache, config, monitors );
    }

    private FileSystemAbstraction createSomeDataAndCrash( File store, EphemeralFileSystemAbstraction fileSystem,
            Config config )
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
