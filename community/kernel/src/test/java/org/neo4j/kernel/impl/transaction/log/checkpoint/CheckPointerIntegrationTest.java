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
package org.neo4j.kernel.impl.transaction.log.checkpoint;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.mockfs.UncloseableDelegatingFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.LogEntryCursor;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.CheckPoint;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.impl.transaction.log.LogVersionRepository.INITIAL_LOG_VERSION;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeader.LOG_HEADER_SIZE;

public class CheckPointerIntegrationTest
{
    @Rule
    public EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();

    private GraphDatabaseBuilder builder;
    private FileSystemAbstraction fs;
    private File storeDir;

    @Before
    public void setup()
    {
        fs = fsRule.get();
        storeDir = testDirectory.graphDbDir();
        builder = new TestGraphDatabaseFactory().setFileSystem( new UncloseableDelegatingFileSystemAbstraction( fs ) )
                .newImpermanentDatabaseBuilder( storeDir );
    }

    @Test
    public void databaseShutdownDuringConstantCheckPointing() throws
            InterruptedException
    {
        GraphDatabaseService db = builder
                .setConfig( GraphDatabaseSettings.check_point_interval_time, 0 + "ms" )
                .setConfig( GraphDatabaseSettings.check_point_interval_tx, "1" )
                .setConfig( GraphDatabaseSettings.logical_log_rotation_threshold, "1g" )
                .newGraphDatabase();
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode();
            tx.success();
        }
        Thread.sleep( 10 );
        db.shutdown();
    }

    @Test
    public void shouldCheckPointBasedOnTime() throws Throwable
    {
        // given
        long millis = 200;
        GraphDatabaseService db = builder
                .setConfig( GraphDatabaseSettings.check_point_interval_time, millis + "ms" )
                .setConfig( GraphDatabaseSettings.check_point_interval_tx, "10000" )
                .setConfig( GraphDatabaseSettings.logical_log_rotation_threshold, "1g" )
                .newGraphDatabase();

        // when
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode();
            tx.success();
        }

        // The scheduled job checking whether or not checkpoints are needed runs more frequently
        // now that we've set the time interval so low, so we can simply wait for it here
        long endTime = currentTimeMillis() + SECONDS.toMillis( 30 );
        while ( !checkPointInTxLog( db ) )
        {
            Thread.sleep( millis );
            assertTrue( "Took too long to produce a checkpoint", currentTimeMillis() < endTime );
        }

        db.shutdown();

        // then - 2 check points have been written in the log
        List<CheckPoint> checkPoints = new CheckPointCollector( storeDir, fs ).find( 0 );

        assertTrue( "Expected at least two (at least one for time interval and one for shutdown), was " +
                checkPoints.toString(), checkPoints.size() >= 2 );
    }

    private boolean checkPointInTxLog( GraphDatabaseService db ) throws IOException
    {
        LogFiles logFiles = ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency( LogFiles.class );
        LogFile logFile = logFiles.getLogFile();
        try ( ReadableLogChannel reader = logFile.getReader( new LogPosition( 0, LOG_HEADER_SIZE ) ) )
        {
            LogEntryReader<ReadableClosablePositionAwareChannel> logEntryReader = new VersionAwareLogEntryReader<>();
            LogEntry entry;
            while ( (entry = logEntryReader.readLogEntry( reader )) != null )
            {
                if ( entry instanceof CheckPoint )
                {
                    return true;
                }
            }
            return false;
        }
    }

    @Test
    public void shouldCheckPointBasedOnTxCount() throws Throwable
    {
        // given
        GraphDatabaseService db = builder
                .setConfig( GraphDatabaseSettings.check_point_interval_time, "300m" )
                .setConfig( GraphDatabaseSettings.check_point_interval_tx, "1" )
                .setConfig( GraphDatabaseSettings.logical_log_rotation_threshold, "1g" )
                .newGraphDatabase();

        // when
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode();
            tx.success();
        }

        // Instead of waiting 10s for the background job to do this check, perform the check right here
        triggerCheckPointAttempt( db );

        assertTrue( checkPointInTxLog( db ) );

        db.shutdown();

        // then - 2 check points have been written in the log
        List<CheckPoint> checkPoints = new CheckPointCollector( storeDir, fs ).find( 0 );

        assertEquals( 2, checkPoints.size() );
    }

    @Test
    public void shouldNotCheckPointWhenThereAreNoCommits() throws Throwable
    {
        // given
        GraphDatabaseService db = builder
                .setConfig( GraphDatabaseSettings.check_point_interval_time, "1s" )
                .setConfig( GraphDatabaseSettings.check_point_interval_tx, "10000" )
                .setConfig( GraphDatabaseSettings.logical_log_rotation_threshold, "1g" )
                .newGraphDatabase();

        // when

        // nothing happens

        triggerCheckPointAttempt( db );
        assertFalse( checkPointInTxLog( db ) );

        db.shutdown();

        // then - 1 check point has been written in the log
        List<CheckPoint> checkPoints = new CheckPointCollector( storeDir, fs ).find( 0 );

        assertEquals( 1, checkPoints.size() );
    }

    @Test
    public void shouldBeAbleToStartAndShutdownMultipleTimesTheDBWithoutCommittingTransactions() throws Throwable
    {
        // given
        GraphDatabaseBuilder graphDatabaseBuilder = builder.setConfig( GraphDatabaseSettings
                .check_point_interval_time, "300m" )
                .setConfig( GraphDatabaseSettings.check_point_interval_tx, "10000" )
                .setConfig( GraphDatabaseSettings.logical_log_rotation_threshold, "1g" );

        // when
        graphDatabaseBuilder.newGraphDatabase().shutdown();
        graphDatabaseBuilder.newGraphDatabase().shutdown();

        // then - 2 check points have been written in the log
        List<CheckPoint> checkPoints = new CheckPointCollector( storeDir, fs ).find( 0 );

        assertEquals( 2, checkPoints.size() );
    }

    private void triggerCheckPointAttempt( GraphDatabaseService db ) throws Exception
    {
        // Simulates triggering the checkpointer background job which runs now and then, checking whether
        // or not there's a need to perform a checkpoint.
        ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency( CheckPointer.class ).checkPointIfNeeded(
                new SimpleTriggerInfo( "Test" ) );
    }

    private static class CheckPointCollector
    {
        private final LogFiles logFiles;
        private final LogEntryReader<ReadableClosablePositionAwareChannel> logEntryReader;

        CheckPointCollector( File directory, FileSystemAbstraction fileSystem ) throws IOException
        {
            this.logEntryReader = new VersionAwareLogEntryReader<>();
            this.logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( directory, fileSystem )
                    .withLogEntryReader( logEntryReader ).build();
        }

        public List<CheckPoint> find( long version ) throws IOException
        {
            List<CheckPoint> checkPoints = new ArrayList<>();
            for (; version >= INITIAL_LOG_VERSION && logFiles.versionExists( version ); version-- )
            {
                LogVersionedStoreChannel channel = logFiles.openForVersion( version );
                ReadableClosablePositionAwareChannel recoveredDataChannel = new ReadAheadLogChannel( channel );

                try ( LogEntryCursor cursor = new LogEntryCursor( logEntryReader, recoveredDataChannel ) )
                {
                    while ( cursor.next() )
                    {
                        LogEntry entry = cursor.get();
                        if ( entry instanceof CheckPoint )
                        {
                            checkPoints.add( entry.as() );
                        }
                    }
                }
            }
            return checkPoints;
        }
    }
}
