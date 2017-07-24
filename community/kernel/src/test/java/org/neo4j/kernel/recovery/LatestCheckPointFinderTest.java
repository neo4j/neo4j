/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.recovery;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.DeadSimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.log.FlushablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.LogHeaderCache;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.LogVersionRepository;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.entry.CheckPoint;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.recovery.LatestCheckPointFinder.LatestCheckPoint;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.neo4j.io.ByteUnit.mebiBytes;
import static org.neo4j.kernel.impl.transaction.log.PhysicalLogFile.NO_MONITOR;
import static org.neo4j.kernel.recovery.LatestCheckPointFinder.LatestCheckPoint.NO_TRANSACTION_ID;

@RunWith( Parameterized.class )
public class LatestCheckPointFinderTest
{
    @Rule
    public final EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    private final File directory = new File( "/somewhere" );
    private final LogEntryReader<ReadableClosablePositionAwareChannel> reader = new VersionAwareLogEntryReader<>();
    private LatestCheckPointFinder finder;
    private PhysicalLogFiles logFiles;
    private final int startLogVersion;
    private final int endLogVersion;

    public LatestCheckPointFinderTest( Integer startLogVersion, Integer endLogVersion )
    {
        this.startLogVersion = startLogVersion;
        this.endLogVersion = endLogVersion;
    }

    @Parameterized.Parameters( name = "{0},{1}" )
    public static Collection<Object[]> params()
    {
        return Arrays.asList( new Object[]{1, 2}, new Object[]{42, 43} );
    }

    @Before
    public void setUp()
    {
        fsRule.get().mkdirs( directory );
        logFiles = new PhysicalLogFiles( directory, fsRule.get() );
        finder = new LatestCheckPointFinder( logFiles, fsRule.get(), reader );
    }

    @Test
    public void noLogFilesFound() throws Throwable
    {
        // given no files
        setupLogFiles();
        int logVersion = startLogVersion;
        LatestCheckPointFinder finder = new LatestCheckPointFinder( logFiles, fsRule.get(), reader );

        // when
        LatestCheckPoint latestCheckPoint = finder.find( logVersion );

        // then
        assertLatestCheckPoint( false, false, NO_TRANSACTION_ID, -1, latestCheckPoint );
    }

    @Test
    public void oneLogFileNoCheckPoints() throws Throwable
    {
        // given
        int logVersion = endLogVersion;
        setupLogFiles( logFile() );

        // when
        LatestCheckPoint latestCheckPoint = finder.find( logVersion );

        // then
        assertLatestCheckPoint( false, false, NO_TRANSACTION_ID, logVersion, latestCheckPoint );
    }

    @Test
    public void oneLogFileNoCheckPointsOneStart() throws Throwable
    {
        // given
        int logVersion = endLogVersion;
        long txId = 10;
        setupLogFiles( logFile( start(), commit( txId ) ) );

        // when
        LatestCheckPoint latestCheckPoint = finder.find( logVersion );

        // then
        assertLatestCheckPoint( false, true, txId, logVersion, latestCheckPoint );
    }

    @Test
    public void twoLogFilesNoCheckPoints() throws Throwable
    {
        // given
        setupLogFiles( logFile(), logFile() );

        // when
        LatestCheckPoint latestCheckPoint = finder.find( endLogVersion );

        // then
        assertLatestCheckPoint( false, false, NO_TRANSACTION_ID, startLogVersion, latestCheckPoint );
    }

    @Test
    public void twoLogFilesNoCheckPointsOneStart() throws Throwable
    {
        // given
        long txId = 21;
        setupLogFiles( logFile(), logFile( start(), commit( txId ) ) );

        // when
        LatestCheckPoint latestCheckPoint = finder.find( endLogVersion );

        // then
        assertLatestCheckPoint( false, true, txId, startLogVersion, latestCheckPoint );
    }

    @Test
    public void twoLogFilesNoCheckPointsOneStartWithoutCommit() throws Throwable
    {
        // given
        setupLogFiles( logFile(), logFile( start() ) );

        // when
        LatestCheckPoint latestCheckPoint = finder.find( endLogVersion );

        // then
        assertLatestCheckPoint( false, true, NO_TRANSACTION_ID, startLogVersion, latestCheckPoint );
    }

    @Test
    public void twoLogFilesNoCheckPointsTwoCommits() throws Throwable
    {
        // given
        long txId = 21;
        setupLogFiles( logFile(), logFile( start(), commit( txId ), start(), commit( txId + 1 ) ) );

        // when
        LatestCheckPoint latestCheckPoint = finder.find( endLogVersion );

        // then
        assertLatestCheckPoint( false, true, txId, startLogVersion, latestCheckPoint );
    }

    @Test
    public void twoLogFilesCheckPointTargetsPrevious() throws Exception
    {
        // given
        long txId = 6;
        PositionEntry position = position();
        setupLogFiles(
                logFile( start(), commit( txId - 1 ), position ),
                logFile( start(), commit( txId ) ),
                logFile( checkPoint( position ) ) );

        // when
        LatestCheckPoint latestCheckPoint = finder.find( endLogVersion );

        // then
        assertLatestCheckPoint( true, true, txId, endLogVersion, latestCheckPoint );
    }

    @Test
    public void latestLogFileContainingACheckPointOnly() throws Throwable
    {
        // given
        setupLogFiles( logFile( checkPoint() ) );

        // when
        LatestCheckPoint latestCheckPoint = finder.find( endLogVersion );

        // then
        assertLatestCheckPoint( true, false, NO_TRANSACTION_ID, endLogVersion, latestCheckPoint );
    }

    @Test
    public void latestLogFileContainingACheckPointAndAStartBefore() throws Throwable
    {
        // given
        setupLogFiles( logFile( start(), checkPoint() ) );

        // when
        LatestCheckPoint latestCheckPoint = finder.find( endLogVersion );

        // then
        assertLatestCheckPoint( true, false, NO_TRANSACTION_ID, endLogVersion, latestCheckPoint );
    }

    @Test
    public void bigFileLatestCheckpointFindsStartAfter() throws Throwable
    {
        long firstTxAfterCheckpoint = Integer.MAX_VALUE + 4L;

        LatestCheckPointFinder checkPointFinder =
                new FirstTxIdConfigurableCheckpointFinder( firstTxAfterCheckpoint, logFiles, fsRule.get(), reader );
        LogEntryStart startEntry = new LogEntryStart( 1, 2, 3L, 4L, new byte[]{5, 6},
                new LogPosition( endLogVersion, Integer.MAX_VALUE + 17L ) );
        CheckPoint checkPoint = new CheckPoint( new LogPosition( endLogVersion, 16L ) );
        LatestCheckPoint latestCheckPoint = checkPointFinder.latestCheckPoint( endLogVersion, endLogVersion, startEntry,
                endLogVersion, checkPoint );

        assertLatestCheckPoint( true, true, firstTxAfterCheckpoint, endLogVersion,
            latestCheckPoint );
    }

    @Test
    public void latestLogFileContainingACheckPointAndAStartAfter() throws Throwable
    {
        // given
        long txId = 35;
        StartEntry start = start();
        setupLogFiles( logFile( start, commit( txId ), checkPoint( start ) ) );

        // when
        LatestCheckPoint latestCheckPoint = finder.find( endLogVersion );

        // then
        assertLatestCheckPoint( true, true, txId, endLogVersion, latestCheckPoint );
    }

    @Test
    public void latestLogFileContainingACheckPointAndAStartWithoutCommitAfter() throws Throwable
    {
        // given
        StartEntry start = start();
        setupLogFiles( logFile( start, checkPoint( start ) ) );

        // when
        LatestCheckPoint latestCheckPoint = finder.find( endLogVersion );

        // then
        assertLatestCheckPoint( true, true, NO_TRANSACTION_ID, endLogVersion, latestCheckPoint );
    }

    @Test
    public void latestLogFileContainingMultipleCheckPointsOneStartInBetween() throws Throwable
    {
        // given
        setupLogFiles( logFile( checkPoint(), start(), checkPoint() ) );

        // when
        LatestCheckPoint latestCheckPoint = finder.find( endLogVersion );

        // then
        assertLatestCheckPoint( true, false, NO_TRANSACTION_ID, endLogVersion, latestCheckPoint );
    }

    @Test
    public void latestLogFileContainingMultipleCheckPointsOneStartAfterBoth() throws Throwable
    {
        // given
        long txId = 11;
        setupLogFiles( logFile( checkPoint(), checkPoint(), start(), commit( txId ) ) );

        // when
        LatestCheckPoint latestCheckPoint = finder.find( endLogVersion );

        // then
        assertLatestCheckPoint( true, true, txId, endLogVersion, latestCheckPoint );
    }

    @Test
    public void olderLogFileContainingACheckPointAndNewerFileContainingAStart() throws Throwable
    {
        // given
        long txId = 11;
        StartEntry start = start();
        setupLogFiles( logFile( checkPoint() ), logFile( start, commit( txId ) ) );

        // when
        LatestCheckPoint latestCheckPoint = finder.find( endLogVersion );

        // then
        assertLatestCheckPoint( true, true, txId, startLogVersion, latestCheckPoint );
    }

    @Test
    public void olderLogFileContainingACheckPointAndNewerFileIsEmpty() throws Throwable
    {
        // given
        StartEntry start = start();
        setupLogFiles( logFile( start, checkPoint() ), logFile() );

        // when
        LatestCheckPoint latestCheckPoint = finder.find( endLogVersion );

        // then
        assertLatestCheckPoint( true, false, NO_TRANSACTION_ID, startLogVersion, latestCheckPoint );
    }

    @Test
    public void olderLogFileContainingAStartAndNewerFileContainingACheckPointPointingToAPreviousPositionThanStart() throws Throwable
    {
        // given
        long txId = 123;
        StartEntry start = start();
        setupLogFiles( logFile( start, commit( txId ) ), logFile( checkPoint( start ) ) );

        // when
        LatestCheckPoint latestCheckPoint = finder.find( endLogVersion );

        // then
        assertLatestCheckPoint( true, true, txId, endLogVersion, latestCheckPoint );
    }

    @Test
    public void olderLogFileContainingAStartAndNewerFileContainingACheckPointPointingToAPreviousPositionThanStartWithoutCommit()
            throws Throwable
    {
        // given
        StartEntry start = start();
        setupLogFiles( logFile( start ), logFile( checkPoint( start ) ) );

        // when
        LatestCheckPoint latestCheckPoint = finder.find( endLogVersion );

        // then
        assertLatestCheckPoint( true, false, NO_TRANSACTION_ID, endLogVersion, latestCheckPoint );
    }

    @Test
    public void olderLogFileContainingAStartAndNewerFileContainingACheckPointPointingToALaterPositionThanStart() throws Throwable
    {
        // given
        PositionEntry position = position();
        setupLogFiles( logFile( start(), commit( 3 ), position ), logFile( checkPoint( position ) ) );

        // when
        LatestCheckPoint latestCheckPoint = finder.find( endLogVersion );

        // then
        assertLatestCheckPoint( true, false, NO_TRANSACTION_ID, endLogVersion, latestCheckPoint );
    }

    @Test
    public void latestLogEmptyStartEntryBeforeAndAfterCheckPointInTheLastButOneLog() throws Throwable
    {
        // given
        long txId = 432;
        setupLogFiles( logFile( start(), checkPoint(), start(), commit( txId ) ), logFile() );

        // when
        LatestCheckPoint latestCheckPoint = finder.find( endLogVersion );

        // then
        assertLatestCheckPoint( true, true, txId, startLogVersion, latestCheckPoint );
    }

    // === Below is code for helping the tests above ===

    @SafeVarargs
    private final void setupLogFiles( LogCreator... logFiles ) throws IOException
    {
        Map<Entry,LogPosition> positions = new HashMap<>();
        long version = endLogVersion - logFiles.length;
        for ( LogCreator logFile : logFiles )
        {
            logFile.create( ++version, positions );
        }
    }

    private LogCreator logFile( Entry... entries )
    {
        return ( logVersion, positions ) ->
        {
            try
            {
                AtomicLong lastTxId = new AtomicLong();
                Supplier<Long> lastTxIdSupplier = lastTxId::get;
                LogVersionRepository logVersionRepository = new DeadSimpleLogVersionRepository( logVersion );
                LifeSupport life = new LifeSupport();
                life.start();
                PhysicalLogFile logFile = life.add( new PhysicalLogFile( fsRule.get(), logFiles, mebiBytes( 1 ),
                        lastTxIdSupplier, logVersionRepository, NO_MONITOR, new LogHeaderCache( 10 ) ) );
                try
                {
                    FlushablePositionAwareChannel writeChannel = logFile.getWriter();
                    LogPositionMarker positionMarker = new LogPositionMarker();
                    LogEntryWriter writer = new LogEntryWriter( writeChannel );
                    for ( Entry entry : entries )
                    {
                        LogPosition currentPosition = writeChannel.getCurrentPosition( positionMarker ).newPosition();
                        positions.put( entry, currentPosition );
                        if ( entry instanceof StartEntry )
                        {
                            writer.writeStartEntry( 0, 0, 0, 0, new byte[0] );
                        }
                        else if ( entry instanceof CommitEntry )
                        {
                            CommitEntry commitEntry = (CommitEntry) entry;
                            writer.writeCommitEntry( commitEntry.txId, 0 );
                            lastTxId.set( commitEntry.txId );
                        }
                        else if ( entry instanceof CheckPointEntry )
                        {
                            CheckPointEntry checkPointEntry = (CheckPointEntry) entry;
                            Entry target = checkPointEntry.withPositionOfEntry;
                            LogPosition logPosition = target != null ? positions.get( target ) : currentPosition;
                            assert logPosition != null : "No registered log position for " + target;
                            writer.writeCheckPointEntry( logPosition );
                        }
                        else if ( entry instanceof PositionEntry )
                        {
                            // Don't write anything, this entry is just for registering a position so that
                            // another CheckPointEntry can refer to it
                        }
                        else
                        {
                            throw new IllegalArgumentException( "Unknown entry " + entry );
                        }

                    }
                }
                finally
                {
                    life.shutdown();
                }
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
        };
    }

    interface LogCreator
    {
        void create( long version, Map<Entry,LogPosition> positions ) throws IOException;
    }

    // Marker interface, helping compilation/test creation
    interface Entry
    {
    }

    private static StartEntry start()
    {
        return new StartEntry();
    }

    private static CommitEntry commit( long txId )
    {
        return new CommitEntry( txId );
    }

    private static CheckPointEntry checkPoint()
    {
        return checkPoint( null/*means self-position*/ );
    }

    private static CheckPointEntry checkPoint( Entry forEntry )
    {
        return new CheckPointEntry( forEntry );
    }

    private static PositionEntry position()
    {
        return new PositionEntry();
    }

    private static class StartEntry implements Entry
    {
    }

    private static class CommitEntry implements Entry
    {
        final long txId;

        CommitEntry( long txId )
        {
            this.txId = txId;
        }
    }

    private static class CheckPointEntry implements Entry
    {
        final Entry withPositionOfEntry;

        CheckPointEntry( Entry withPositionOfEntry )
        {
            this.withPositionOfEntry = withPositionOfEntry;
        }
    }

    private static class PositionEntry implements Entry
    {
    }

    private void assertLatestCheckPoint( boolean hasCheckPointEntry, boolean commitsAfterLastCheckPoint,
            long firstTxIdAfterLastCheckPoint, long logVersion, LatestCheckPoint latestCheckPoint )
    {
        assertEquals( hasCheckPointEntry, latestCheckPoint.checkPoint != null );
        assertEquals( commitsAfterLastCheckPoint, latestCheckPoint.commitsAfterCheckPoint );
        if ( commitsAfterLastCheckPoint )
        {
            assertEquals( firstTxIdAfterLastCheckPoint, latestCheckPoint.firstTxIdAfterLastCheckPoint );
        }
        assertEquals( logVersion, latestCheckPoint.oldestLogVersionFound );
    }

    private static class FirstTxIdConfigurableCheckpointFinder extends LatestCheckPointFinder
    {

        private final long txId;

        FirstTxIdConfigurableCheckpointFinder( long txId, PhysicalLogFiles logFiles, FileSystemAbstraction fileSystem,
                LogEntryReader<ReadableClosablePositionAwareChannel> logEntryReader )
        {
            super( logFiles, fileSystem, logEntryReader );
            this.txId = txId;
        }

        @Override
        public LatestCheckPoint latestCheckPoint( long fromVersionBackwards, long version,
                LogEntryStart latestStartEntry, long oldestVersionFound, CheckPoint latestCheckPoint )
                throws IOException
        {
            return super.latestCheckPoint( fromVersionBackwards, version, latestStartEntry, oldestVersionFound,
                    latestCheckPoint );
        }

        @Override
        protected long extractFirstTxIdAfterPosition( LogPosition initialPosition, long maxLogVersion )
                throws IOException
        {
            return txId;
        }
    }
}
