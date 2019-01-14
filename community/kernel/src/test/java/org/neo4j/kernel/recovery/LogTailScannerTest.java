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

import org.neo4j.kernel.impl.transaction.SimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.log.FlushablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.LogVersionRepository;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.entry.CheckPoint;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryVersion;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.kernel.recovery.LogTailScanner.LogTailInformation;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.neo4j.kernel.recovery.LogTailScanner.NO_TRANSACTION_ID;

@RunWith( Parameterized.class )
public class LogTailScannerTest
{
    @Rule
    public final EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    @Rule
    public final PageCacheRule pageCacheRule = new PageCacheRule();
    private final File directory = new File( "/somewhere" );
    private final LogEntryReader<ReadableClosablePositionAwareChannel> reader = new VersionAwareLogEntryReader<>();
    private LogTailScanner tailScanner;

    private final Monitors monitors = new Monitors();
    private LogFiles logFiles;
    private final int startLogVersion;
    private final int endLogVersion;
    private final LogEntryVersion latestLogEntryVersion = LogEntryVersion.CURRENT;
    private LogVersionRepository logVersionRepository;

    public LogTailScannerTest( Integer startLogVersion, Integer endLogVersion )
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
    public void setUp() throws IOException
    {
        fsRule.get().mkdirs( directory );
        logVersionRepository = new SimpleLogVersionRepository();
        logFiles = LogFilesBuilder
                .activeFilesBuilder( directory, fsRule, pageCacheRule.getPageCache( fsRule ) )
                .withLogVersionRepository( logVersionRepository )
                .build();
        tailScanner = new LogTailScanner( logFiles, reader, monitors );
    }

    @Test
    public void noLogFilesFound()
    {
        // given no files
        setupLogFiles();

        // when
        LogTailInformation logTailInformation = tailScanner.getTailInformation();

        // then
        assertLatestCheckPoint( false, false, NO_TRANSACTION_ID, -1, logTailInformation );
    }

    @Test
    public void oneLogFileNoCheckPoints()
    {
        // given
        setupLogFiles( logFile() );

        // when
        LogTailInformation logTailInformation = tailScanner.getTailInformation();

        // then
        assertLatestCheckPoint( false, true, NO_TRANSACTION_ID, endLogVersion, logTailInformation );
    }

    @Test
    public void oneLogFileNoCheckPointsOneStart()
    {
        // given
        long txId = 10;
        setupLogFiles( logFile( start(), commit( txId ) ) );

        // when
        LogTailInformation logTailInformation = tailScanner.getTailInformation();

        // then
        assertLatestCheckPoint( false, true, txId, endLogVersion, logTailInformation );
    }

    @Test
    public void twoLogFilesNoCheckPoints()
    {
        // given
        setupLogFiles( logFile(), logFile() );

        // when
        LogTailInformation logTailInformation = tailScanner.getTailInformation();

        // then
        assertLatestCheckPoint( false, true, NO_TRANSACTION_ID, startLogVersion, logTailInformation );
    }

    @Test
    public void twoLogFilesNoCheckPointsOneStart()
    {
        // given
        long txId = 21;
        setupLogFiles( logFile(), logFile( start(), commit( txId ) ) );

        // when
        LogTailInformation logTailInformation = tailScanner.getTailInformation();

        // then
        assertLatestCheckPoint( false, true, txId, startLogVersion, logTailInformation );
    }

    @Test
    public void twoLogFilesNoCheckPointsOneStartWithoutCommit()
    {
        // given
        setupLogFiles( logFile(), logFile( start() ) );

        // when
        LogTailInformation logTailInformation = tailScanner.getTailInformation();

        // then
        assertLatestCheckPoint( false, true, NO_TRANSACTION_ID, startLogVersion, logTailInformation );
    }

    @Test
    public void twoLogFilesNoCheckPointsTwoCommits()
    {
        // given
        long txId = 21;
        setupLogFiles( logFile(), logFile( start(), commit( txId ), start(), commit( txId + 1 ) ) );

        // when
        LogTailInformation logTailInformation = tailScanner.getTailInformation();

        // then
        assertLatestCheckPoint( false, true, txId, startLogVersion, logTailInformation );
    }

    @Test
    public void twoLogFilesCheckPointTargetsPrevious()
    {
        // given
        long txId = 6;
        PositionEntry position = position();
        setupLogFiles(
                logFile( start(), commit( txId - 1 ), position ),
                logFile( start(), commit( txId ) ),
                logFile( checkPoint( position ) ) );

        // when
        LogTailInformation logTailInformation = tailScanner.getTailInformation();

        // then
        assertLatestCheckPoint( true, true, txId, endLogVersion, logTailInformation );
    }

    @Test
    public void twoLogFilesStartAndCommitInDifferentFiles()
    {
        // given
        long txId = 6;
        setupLogFiles(
                logFile( start() ),
                logFile( commit( txId ) ) );

        // when
        LogTailInformation logTailInformation = tailScanner.getTailInformation();

        // then
        assertLatestCheckPoint( false, true, 6, startLogVersion, logTailInformation );
    }

    @Test
    public void latestLogFileContainingACheckPointOnly()
    {
        // given
        setupLogFiles( logFile( checkPoint() ) );

        // when
        LogTailInformation logTailInformation = tailScanner.getTailInformation();

        // then
        assertLatestCheckPoint( true, false, NO_TRANSACTION_ID, endLogVersion, logTailInformation );
    }

    @Test
    public void latestLogFileContainingACheckPointAndAStartBefore()
    {
        // given
        setupLogFiles( logFile( start(), checkPoint() ) );

        // when
        LogTailInformation logTailInformation = tailScanner.getTailInformation();

        // then
        assertLatestCheckPoint( true, false, NO_TRANSACTION_ID, endLogVersion, logTailInformation );
    }

    @Test
    public void bigFileLatestCheckpointFindsStartAfter() throws Throwable
    {
        long firstTxAfterCheckpoint = Integer.MAX_VALUE + 4L;

        LogTailScanner tailScanner =
                new FirstTxIdConfigurableTailScanner( firstTxAfterCheckpoint, logFiles, reader, monitors );
        LogEntryStart startEntry = new LogEntryStart( 1, 2, 3L, 4L, new byte[]{5, 6},
                new LogPosition( endLogVersion, Integer.MAX_VALUE + 17L ) );
        CheckPoint checkPoint = new CheckPoint( new LogPosition( endLogVersion, 16L ) );
        LogTailInformation logTailInformation = tailScanner.checkpointTailInformation( endLogVersion, startEntry,
                endLogVersion, latestLogEntryVersion, checkPoint, false);

        assertLatestCheckPoint( true, true, firstTxAfterCheckpoint, endLogVersion, logTailInformation );
    }

    @Test
    public void twoLogFilesSecondIsCorruptedBeforeCommit() throws IOException
    {
        setupLogFiles( logFile( checkPoint() ), logFile( start(), commit( 2 ) ) );

        File highestLogFile = logFiles.getHighestLogFile();
        fsRule.truncate( highestLogFile, fsRule.getFileSize( highestLogFile ) - 3 );

        // when
        LogTailInformation logTailInformation = tailScanner.getTailInformation();

        // then
        assertLatestCheckPoint( true, true, NO_TRANSACTION_ID, startLogVersion, logTailInformation );
    }

    @Test
    public void twoLogFilesSecondIsCorruptedBeforeAfterCommit() throws IOException
    {
        int firstTxId = 2;
        setupLogFiles( logFile( checkPoint() ), logFile( start(), commit( firstTxId ), start(), commit( 3 ) ) );

        File highestLogFile = logFiles.getHighestLogFile();
        fsRule.truncate( highestLogFile, fsRule.getFileSize( highestLogFile ) - 3 );

        // when
        LogTailInformation logTailInformation = tailScanner.getTailInformation();

        // then
        assertLatestCheckPoint( true, true, firstTxId, startLogVersion, logTailInformation );
    }

    @Test
    public void latestLogFileContainingACheckPointAndAStartAfter()
    {
        // given
        long txId = 35;
        StartEntry start = start();
        setupLogFiles( logFile( start, commit( txId ), checkPoint( start ) ) );

        // when
        LogTailInformation logTailInformation = tailScanner.getTailInformation();

        // then
        assertLatestCheckPoint( true, true, txId, endLogVersion, logTailInformation );
    }

    @Test
    public void latestLogFileContainingACheckPointAndAStartWithoutCommitAfter()
    {
        // given
        StartEntry start = start();
        setupLogFiles( logFile( start, checkPoint( start ) ) );

        // when
        LogTailInformation logTailInformation = tailScanner.getTailInformation();

        // then
        assertLatestCheckPoint( true, true, NO_TRANSACTION_ID, endLogVersion, logTailInformation );
    }

    @Test
    public void latestLogFileContainingMultipleCheckPointsOneStartInBetween()
    {
        // given
        setupLogFiles( logFile( checkPoint(), start(), checkPoint() ) );

        // when
        LogTailInformation logTailInformation = tailScanner.getTailInformation();

        // then
        assertLatestCheckPoint( true, false, NO_TRANSACTION_ID, endLogVersion, logTailInformation );
    }

    @Test
    public void latestLogFileContainingMultipleCheckPointsOneStartAfterBoth()
    {
        // given
        long txId = 11;
        setupLogFiles( logFile( checkPoint(), checkPoint(), start(), commit( txId ) ) );

        // when
        LogTailInformation logTailInformation = tailScanner.getTailInformation();

        // then
        assertLatestCheckPoint( true, true, txId, endLogVersion, logTailInformation );
    }

    @Test
    public void olderLogFileContainingACheckPointAndNewerFileContainingAStart()
    {
        // given
        long txId = 11;
        StartEntry start = start();
        setupLogFiles( logFile( checkPoint() ), logFile( start, commit( txId ) ) );

        // when
        LogTailInformation logTailInformation = tailScanner.getTailInformation();

        // then
        assertLatestCheckPoint( true, true, txId, startLogVersion, logTailInformation );
    }

    @Test
    public void olderLogFileContainingACheckPointAndNewerFileIsEmpty()
    {
        // given
        StartEntry start = start();
        setupLogFiles( logFile( start, checkPoint() ), logFile() );

        // when
        LogTailInformation logTailInformation = tailScanner.getTailInformation();

        // then
        assertLatestCheckPoint( true, true, NO_TRANSACTION_ID, startLogVersion, logTailInformation );
    }

    @Test
    public void olderLogFileContainingAStartAndNewerFileContainingACheckPointPointingToAPreviousPositionThanStart()
    {
        // given
        long txId = 123;
        StartEntry start = start();
        setupLogFiles( logFile( start, commit( txId ) ), logFile( checkPoint( start ) ) );

        // when
        LogTailInformation logTailInformation = tailScanner.getTailInformation();

        // then
        assertLatestCheckPoint( true, true, txId, endLogVersion, logTailInformation );
    }

    @Test
    public void olderLogFileContainingAStartAndNewerFileContainingACheckPointPointingToAPreviousPositionThanStartWithoutCommit()
    {
        // given
        StartEntry start = start();
        setupLogFiles( logFile( start ), logFile( checkPoint( start ) ) );

        // when
        LogTailInformation logTailInformation = tailScanner.getTailInformation();

        // then
        assertLatestCheckPoint( true, false, NO_TRANSACTION_ID, endLogVersion, logTailInformation );
    }

    @Test
    public void olderLogFileContainingAStartAndNewerFileContainingACheckPointPointingToALaterPositionThanStart()
    {
        // given
        PositionEntry position = position();
        setupLogFiles( logFile( start(), commit( 3 ), position ), logFile( checkPoint( position ) ) );

        // when
        LogTailInformation logTailInformation = tailScanner.getTailInformation();

        // then
        assertLatestCheckPoint( true, false, NO_TRANSACTION_ID, endLogVersion, logTailInformation );
    }

    @Test
    public void latestLogEmptyStartEntryBeforeAndAfterCheckPointInTheLastButOneLog()
    {
        // given
        long txId = 432;
        setupLogFiles( logFile( start(), checkPoint(), start(), commit( txId ) ), logFile() );

        // when
        LogTailInformation logTailInformation = tailScanner.getTailInformation();

        // then
        assertLatestCheckPoint( true, true, txId, startLogVersion, logTailInformation );
    }

    // === Below is code for helping the tests above ===

    private void setupLogFiles( LogCreator... logFiles )
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
                logVersionRepository.setCurrentLogVersion( logVersion );
                LifeSupport logFileLife = new LifeSupport();
                logFileLife.start();
                logFileLife.add( logFiles );
                LogFile logFile = logFiles.getLogFile();
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
                    logFileLife.shutdown();
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
        void create( long version, Map<Entry,LogPosition> positions );
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
            long firstTxIdAfterLastCheckPoint, long logVersion, LogTailInformation logTailInformation )
    {
        assertEquals( hasCheckPointEntry, logTailInformation.lastCheckPoint != null );
        assertEquals( commitsAfterLastCheckPoint, logTailInformation.commitsAfterLastCheckpoint() );
        if ( commitsAfterLastCheckPoint )
        {
            assertEquals( firstTxIdAfterLastCheckPoint, logTailInformation.firstTxIdAfterLastCheckPoint );
        }
        assertEquals( logVersion, logTailInformation.oldestLogVersionFound );
    }

    private static class FirstTxIdConfigurableTailScanner extends LogTailScanner
    {

        private final long txId;

        FirstTxIdConfigurableTailScanner( long txId, LogFiles logFiles,
                LogEntryReader<ReadableClosablePositionAwareChannel> logEntryReader, Monitors monitors )
        {
            super( logFiles, logEntryReader, monitors );
            this.txId = txId;
        }

        @Override
        protected ExtractedTransactionRecord extractFirstTxIdAfterPosition( LogPosition initialPosition, long maxLogVersion )
        {
            return new ExtractedTransactionRecord( txId );
        }
    }
}
