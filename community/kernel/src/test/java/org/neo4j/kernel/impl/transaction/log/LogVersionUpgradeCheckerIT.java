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
package org.neo4j.kernel.impl.transaction.log;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.storemigration.UpgradeNotAllowedByConfigurationException;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryVersion;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.kernel.recovery.LogTailScanner;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.matchers.NestedThrowableMatcher;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.neo4j.graphdb.Label.label;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryByteCodes.CHECK_POINT;

public class LogVersionUpgradeCheckerIT
{
    private final TestDirectory storeDirectory = TestDirectory.testDirectory();
    private final DefaultFileSystemRule fs = new DefaultFileSystemRule();
    private final PageCacheRule pageCacheRule = new PageCacheRule();

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( storeDirectory ).around( fs ).around( pageCacheRule );

    @Rule
    public ExpectedException expect = ExpectedException.none();

    @Test
    public void startAsNormalWhenUpgradeIsNotAllowed()
    {
        createGraphDbAndKillIt();

        // Try to start with upgrading disabled
        final GraphDatabaseService db = new TestGraphDatabaseFactory()
                .setFileSystem( fs.get() )
                .newImpermanentDatabaseBuilder( storeDirectory.graphDbDir() )
                .setConfig( GraphDatabaseSettings.allow_upgrade, "false" )
                .newGraphDatabase();
        db.shutdown();
    }

    @Test
    public void failToStartFromOlderTransactionLogsIfNotAllowed() throws Exception
    {
        createStoreWithLogEntryVersion( LogEntryVersion.V2_3 );

        expect.expect( new NestedThrowableMatcher( UpgradeNotAllowedByConfigurationException.class ) );

        // Try to start with upgrading disabled
        final GraphDatabaseService db = new TestGraphDatabaseFactory()
                .setFileSystem( fs.get() )
                .newImpermanentDatabaseBuilder( storeDirectory.graphDbDir() )
                .setConfig( GraphDatabaseSettings.allow_upgrade, "false" )
                .newGraphDatabase();
        db.shutdown();
    }

    @Test
    public void startFromOlderTransactionLogsIfAllowed() throws Exception
    {
        createStoreWithLogEntryVersion( LogEntryVersion.V2_3 );

        // Try to start with upgrading enabled
        final GraphDatabaseService db = new TestGraphDatabaseFactory()
                .setFileSystem( fs.get() )
                .newImpermanentDatabaseBuilder( storeDirectory.graphDbDir() )
                .setConfig( GraphDatabaseSettings.allow_upgrade, "true" )
                .newGraphDatabase();
        db.shutdown();
    }

    private void createGraphDbAndKillIt()
    {
        final GraphDatabaseService db = new TestGraphDatabaseFactory()
                .setFileSystem( fs )
                .newImpermanentDatabaseBuilder( storeDirectory.graphDbDir() )
                .newGraphDatabase();

        try ( Transaction tx = db.beginTx() )
        {
            db.createNode( label( "FOO" ) );
            db.createNode( label( "BAR" ) );
            tx.success();
        }

        db.shutdown();
    }

    private void createStoreWithLogEntryVersion( LogEntryVersion logEntryVersion ) throws Exception
    {
        createGraphDbAndKillIt();
        appendCheckpoint( logEntryVersion );
    }

    private void appendCheckpoint( LogEntryVersion logVersion ) throws IOException
    {
        File storeDir = storeDirectory.graphDbDir();
        PageCache pageCache = pageCacheRule.getPageCache( fs );
        VersionAwareLogEntryReader<ReadableClosablePositionAwareChannel> logEntryReader = new VersionAwareLogEntryReader<>();
        LogFiles logFiles = LogFilesBuilder.activeFilesBuilder( storeDir, fs, pageCache ).withLogEntryReader( logEntryReader ).build();
        LogTailScanner tailScanner = new LogTailScanner( logFiles, logEntryReader, new Monitors() );
        LogTailScanner.LogTailInformation tailInformation = tailScanner.getTailInformation();

        try ( Lifespan lifespan = new Lifespan( logFiles ) )
        {
            FlushablePositionAwareChannel channel = logFiles.getLogFile().getWriter();

            LogPosition logPosition = tailInformation.lastCheckPoint.getLogPosition();

            // Fake record
            channel.put( logVersion.byteCode() )
                    .put( CHECK_POINT )
                    .putLong( logPosition.getLogVersion() )
                    .putLong( logPosition.getByteOffset() );

            channel.prepareForFlush().flush();
        }
    }
}
