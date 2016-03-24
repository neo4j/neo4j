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
package org.neo4j.kernel;

import org.junit.Rule;
import org.junit.Test;
import org.mockito.InOrder;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.neo4j.helpers.collection.Pair;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.DeadSimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.DeadSimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.LogHeaderCache;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.LogVersionRepository;
import org.neo4j.kernel.impl.transaction.log.LogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.PositionAwarePhysicalFlushableChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.entry.CheckPoint;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.entry.OnePhaseCommit;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.recovery.DefaultRecoverySPI;
import org.neo4j.kernel.recovery.LatestCheckPointFinder;
import org.neo4j.kernel.recovery.Recovery;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.neo4j.kernel.impl.transaction.log.LogVersionBridge.NO_MORE_CHANNELS;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderWriter.writeLogHeader;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_LOG_VERSION;

public class RecoveryTest
{
    private final FileSystemAbstraction fs = new DefaultFileSystemAbstraction();

    @Rule
    public final TargetDirectory.TestDirectory directory = TargetDirectory.testDirForTest( getClass() );
    private final LogVersionRepository logVersionRepository = new DeadSimpleLogVersionRepository( 1L );
    private final TransactionIdStore transactionIdStore = new DeadSimpleTransactionIdStore( 5L, 0, 0, 0 );
    private final int logVersion = 1;

    private LogEntry lastCommittedTxStartEntry;
    private LogEntry lastCommittedTxCommitEntry;
    private LogEntry expectedStartEntry;
    private LogEntry expectedCommitEntry;
    private LogEntry expectedCheckPointEntry;

    @Test
    public void shouldRecoverExistingData() throws Exception
    {
        final PhysicalLogFiles logFiles = new PhysicalLogFiles( directory.directory(), "log", fs );
        File file = logFiles.getLogFileForVersion( logVersion );

        writeSomeData( file, new Visitor<Pair<LogEntryWriter, Consumer<LogPositionMarker>>,IOException>()
        {
            @Override
            public boolean visit( Pair<LogEntryWriter,Consumer<LogPositionMarker>> pair ) throws IOException
            {
                LogEntryWriter writer = pair.first();
                Consumer<LogPositionMarker> consumer = pair.other();
                LogPositionMarker marker = new LogPositionMarker();

                // last committed tx
                consumer.accept( marker );
                LogPosition lastCommittedTxPosition = marker.newPosition();
                writer.writeStartEntry( 0, 1, 2l, 3l, new byte[0] );
                lastCommittedTxStartEntry = new LogEntryStart( 0, 1, 2l, 3l, new byte[0], lastCommittedTxPosition );
                writer.writeCommitEntry( 4l, 5l );
                lastCommittedTxCommitEntry = new OnePhaseCommit( 4l, 5l );

                // check point
                writer.writeCheckPointEntry( lastCommittedTxPosition );
                expectedCheckPointEntry = new CheckPoint( lastCommittedTxPosition );

                // tx committed after checkpoint
                consumer.accept( marker );
                writer.writeStartEntry( 0, 1, 6l, 4l, new byte[0] );
                expectedStartEntry = new LogEntryStart( 0, 1, 6l, 4l, new byte[0], marker.newPosition() );

                writer.writeCommitEntry( 5l, 7l );
                expectedCommitEntry = new OnePhaseCommit( 5l, 7l );

                return true;
            }
        } );

        LifeSupport life = new LifeSupport();
        Recovery.Monitor monitor = mock( Recovery.Monitor.class );
        final AtomicBoolean recoveryRequiredCalled = new AtomicBoolean();
        try
        {
            StorageEngine storageEngine = mock( StorageEngine.class );
            final LogEntryReader<ReadableClosablePositionAwareChannel> reader = new VersionAwareLogEntryReader<>();
            LatestCheckPointFinder finder = new LatestCheckPointFinder( logFiles, fs, reader );

            life.add( new Recovery( new DefaultRecoverySPI( storageEngine, null,
                    logFiles, fs, logVersionRepository, finder )
            {
                @Override
                public Visitor<LogVersionedStoreChannel,Exception> getRecoverer()
                {
                    return new Visitor<LogVersionedStoreChannel,Exception>()
                    {
                        @Override
                        public boolean visit( LogVersionedStoreChannel element ) throws IOException
                        {
                            try ( ReadableLogChannel channel = new ReadAheadLogChannel( element,
                                    NO_MORE_CHANNELS ) )
                            {
                                assertEquals( lastCommittedTxStartEntry, reader.readLogEntry( channel ) );
                                assertEquals( lastCommittedTxCommitEntry, reader.readLogEntry( channel ) );
                                assertEquals( expectedCheckPointEntry, reader.readLogEntry( channel ) );
                                assertEquals( expectedStartEntry, reader.readLogEntry( channel ) );
                                assertEquals( expectedCommitEntry, reader.readLogEntry( channel ) );

                                assertNull( reader.readLogEntry( channel ) );

                                return true;
                            }
                        }
                    };
                }

                @Override
                public void recoveryRequired()
                {
                    recoveryRequiredCalled.set( true );
                }
            }, monitor ) );

            life.add( new PhysicalLogFile( fs, logFiles, 50, transactionIdStore::getLastCommittedTransactionId,
                    logVersionRepository, mock( PhysicalLogFile.Monitor.class ), new LogHeaderCache( 10 ) ) );

            life.start();

            InOrder order = inOrder( monitor );
            order.verify( monitor, times( 1 ) ).recoveryRequired( any( LogPosition.class ) );
            order.verify( monitor, times( 1 ) ).logRecovered( any( LogPosition.class ) );
            order.verify( monitor, times( 1 ) ).recoveryCompleted();
            assertTrue( recoveryRequiredCalled.get() );
        }
        finally
        {
            life.shutdown();
        }
    }

    @Test
    public void shouldSeeThatACleanDatabaseShouldNotRequireRecovery() throws Exception
    {
        final PhysicalLogFiles logFiles = new PhysicalLogFiles( directory.directory(), "log", fs );
        File file = logFiles.getLogFileForVersion( logVersion );

        writeSomeData( file, new Visitor<Pair<LogEntryWriter, Consumer<LogPositionMarker>>,IOException>()
        {
            @Override
            public boolean visit( Pair<LogEntryWriter,Consumer<LogPositionMarker>> pair ) throws IOException
            {
                LogEntryWriter writer = pair.first();
                Consumer<LogPositionMarker> consumer = pair.other();
                LogPositionMarker marker = new LogPositionMarker();

                // last committed tx
                consumer.accept( marker );
                writer.writeStartEntry( 0, 1, 2l, 3l, new byte[0] );
                writer.writeCommitEntry( 4l, 5l );

                // check point
                consumer.accept( marker );
                writer.writeCheckPointEntry( marker.newPosition() );

                return true;
            }
        } );

        LifeSupport life = new LifeSupport();
        Recovery.Monitor monitor = mock( Recovery.Monitor.class );
        try
        {
            StorageEngine storageEngine = mock( StorageEngine.class );
            final LogEntryReader<ReadableClosablePositionAwareChannel> reader = new VersionAwareLogEntryReader<>();
            LatestCheckPointFinder finder = new LatestCheckPointFinder( logFiles, fs, reader );

            life.add( new Recovery( new DefaultRecoverySPI( storageEngine, null,
                    logFiles, fs, logVersionRepository, finder )
            {
                @Override
                public Visitor<LogVersionedStoreChannel,Exception> getRecoverer()
                {
                    throw new AssertionError( "Recovery should not be required" );
                }

                @Override
                public void recoveryRequired()
                {
                    fail( "Recovery should not be required" );
                }
            }, monitor ));

            life.add( new PhysicalLogFile( fs, logFiles, 50, transactionIdStore::getLastCommittedTransactionId,
                    logVersionRepository, mock( PhysicalLogFile.Monitor.class), new LogHeaderCache( 10 ) ) );

            life.start();

            verifyZeroInteractions( monitor );
        }
        finally
        {
            life.shutdown();
        }
    }

    private void writeSomeData( File file, Visitor<Pair<LogEntryWriter,Consumer<LogPositionMarker>>,IOException> visitor ) throws IOException
    {

        try (  LogVersionedStoreChannel versionedStoreChannel =
                       new PhysicalLogVersionedStoreChannel( fs.open( file, "rw" ), logVersion, CURRENT_LOG_VERSION );
              final PositionAwarePhysicalFlushableChannel writableLogChannel = new PositionAwarePhysicalFlushableChannel( versionedStoreChannel ) )
        {
            writeLogHeader( writableLogChannel, logVersion, 2l );

            Consumer<LogPositionMarker> consumer = marker -> {
                try
                {
                    writableLogChannel.getCurrentPosition( marker );
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }
            };
            LogEntryWriter first = new LogEntryWriter( writableLogChannel );
            visitor.visit( Pair.of( first, consumer ) );
        }
    }
}
