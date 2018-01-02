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
package org.neo4j.kernel.impl.transaction.log.checkpoint;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.LogEntryCursor;
import org.neo4j.kernel.impl.transaction.log.LogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.CheckPoint;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryVersion;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.TestGraphDatabaseFactory;

import static java.lang.System.getProperty;
import static org.junit.Assert.assertEquals;
import static org.neo4j.kernel.impl.transaction.log.LogVersionBridge.NO_MORE_CHANNELS;
import static org.neo4j.kernel.impl.transaction.log.LogVersionRepository.INITIAL_LOG_VERSION;
import static org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel.DEFAULT_READ_AHEAD_SIZE;

public class CheckPointerIntegrationTest
{
    private static final File storeDir = new File( getProperty( "java.io.tmpdir" ), "graph.db" );

    @Rule
    public EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();

    private GraphDatabaseBuilder builder;
    private FileSystemAbstraction fs;

    @Before
    public void setup() throws IOException
    {
        fs = fsRule.get();
        fs.deleteRecursively( storeDir );
        builder = new TestGraphDatabaseFactory().setFileSystem( fs ).newImpermanentDatabaseBuilder( storeDir );
    }

    @Test
    public void databaseShutdownDuringConstantCheckPointing() throws
            InterruptedException, IOException
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
        long seconds = 3;
        GraphDatabaseService db = builder
                .setConfig( GraphDatabaseSettings.check_point_interval_time, seconds + "s" )
                .setConfig( GraphDatabaseSettings.check_point_interval_tx, "10000" )
                .setConfig( GraphDatabaseSettings.logical_log_rotation_threshold, "1g" )
                .newGraphDatabase();

        // when
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode();
            tx.success();
        }

        // trigger the check point
        TimeUnit.SECONDS.sleep( seconds );

        // give a chance to the check pointer to be scheduled and run...
        TimeUnit.SECONDS.sleep( 1 );

        db.shutdown();

        // then - 2 check points have been written in the log
        List<CheckPoint> checkPoints = new CheckPointCollector( storeDir, fs ).find( 0 );

        assertEquals( 2, checkPoints.size() );
    }

    @Test
    public void shouldCheckPointBasedOnTxCount() throws Throwable
    {
        // given
        GraphDatabaseService db = builder.setConfig( GraphDatabaseSettings.check_point_interval_time, "300m" )
                .setConfig( GraphDatabaseSettings.check_point_interval_tx, "1" )
                .setConfig( GraphDatabaseSettings.logical_log_rotation_threshold, "1g" )
                .newGraphDatabase();

        // when
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode();
            tx.success();
        }

        // give a chance to the check pointer to be scheduled and run...
        TimeUnit.SECONDS.sleep( 11 );

        db.shutdown();

        // then - 2 check points have been written in the log
        List<CheckPoint> checkPoints = new CheckPointCollector( storeDir, fs ).find( 0 );

        assertEquals( 2, checkPoints.size() );
    }

    @Test
    public void shouldNotCheckPointWhenThereAreNoCommits() throws Throwable
    {
        // given
        long seconds = 1;
        GraphDatabaseService db = builder
                .setConfig( GraphDatabaseSettings.check_point_interval_time, seconds + "s" )
                .setConfig( GraphDatabaseSettings.check_point_interval_tx, "10000" )
                .setConfig( GraphDatabaseSettings.logical_log_rotation_threshold, "1g" )
                .newGraphDatabase();

        // when

        // nothing happens

        // trigger the check point
        TimeUnit.SECONDS.sleep( seconds );

        // give a chance to the check pointer to be scheduled and run...
        TimeUnit.SECONDS.sleep( 1 );

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

    private static class CheckPointCollector
    {
        private final PhysicalLogFiles logFiles;
        private final FileSystemAbstraction fileSystem;
        private final LogEntryReader<ReadableLogChannel> logEntryReader;

        public CheckPointCollector( File directory, FileSystemAbstraction fileSystem )
        {
            this.fileSystem = fileSystem;
            this.logFiles = new PhysicalLogFiles( directory, fileSystem );
            this.logEntryReader = new VersionAwareLogEntryReader<>( LogEntryVersion.CURRENT.byteCode() );
        }

        public List<CheckPoint> find( long version ) throws IOException
        {
            List<CheckPoint> checkPoints = new ArrayList<>();
            for (; version >= INITIAL_LOG_VERSION; version-- )
            {
                LogVersionedStoreChannel channel = PhysicalLogFile.tryOpenForVersion( logFiles, fileSystem, version );
                if ( channel == null )
                {
                    break;
                }

                ReadableLogChannel recoveredDataChannel =
                        new ReadAheadLogChannel( channel, NO_MORE_CHANNELS, DEFAULT_READ_AHEAD_SIZE );

                try ( LogEntryCursor cursor = new LogEntryCursor( logEntryReader, recoveredDataChannel ) )
                {
                    while ( cursor.next() )
                    {
                        LogEntry entry = cursor.get();
                        if ( entry instanceof CheckPoint )
                        {
                            checkPoints.add( entry.<CheckPoint>as() );
                        }
                    }
                }
            }
            return checkPoints;
        }

    }
}
