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
package org.neo4j.kernel.recovery;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableVersionableLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.CheckPoint;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.recovery.LatestCheckPointFinder.LatestCheckPoint;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeader.LOG_HEADER_SIZE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderWriter.encodeLogVersion;

@RunWith( Parameterized.class )
public class LatestCheckPointFinderTest
{
    private final PhysicalLogFiles logFiles = mock( PhysicalLogFiles.class );
    private final FileSystemAbstraction fs = mock( FileSystemAbstraction.class );
    @SuppressWarnings( "unchecked" )
    private final LogEntryReader<ReadableLogChannel> reader = mock( LogEntryReader.class );

    private final int startLogVersion;
    private final int endLogVersion;

    public LatestCheckPointFinderTest( Integer startLogVersion, Integer endLogVersion )
    {
        this.startLogVersion = startLogVersion;
        this.endLogVersion = endLogVersion;
    }

    @Parameterized.Parameters( name="{0},{1}")
    public static Collection<Object[]> params()
    {
        return Arrays.asList( new Object[]{0, 1}, new Object[]{42, 43}  );
    }

    @Test
    public void noLogFilesFound() throws Throwable
    {
        // given no files
        int logVersion = startLogVersion;
        when( logFiles.getLogFileForVersion( logVersion ) ).thenReturn( mock( File.class ) );
        when( fs.fileExists( any( File.class ) ) ).thenReturn( false );

        LatestCheckPointFinder finder = new LatestCheckPointFinder( logFiles, fs, reader );

        // when
        LatestCheckPoint latestCheckPoint = finder.find( logVersion );

        // then
        assertEquals( new LatestCheckPoint( null, false, -1 ), latestCheckPoint );
    }

    @Test
    public void oneLogFileNoCheckPoints() throws Throwable
    {
        // given
        int logVersion = startLogVersion;
        setupLogFiles( logVersion, logVersion );
        LatestCheckPointFinder finder = new LatestCheckPointFinder( logFiles, fs, reader );

        // when
        LatestCheckPoint latestCheckPoint = finder.find( logVersion );

        // then
        assertEquals( new LatestCheckPoint( null, false, logVersion ), latestCheckPoint );
    }

    @Test
    public void oneLogFileNoCheckPointsOneStart() throws Throwable
    {
        // given
        int logVersion = startLogVersion;
        setupLogFiles( logVersion, logVersion );
        LatestCheckPointFinder finder = new LatestCheckPointFinder( logFiles, fs, reader );
        LogEntryStart start = new LogEntryStart( 0, 0, 0, 0, new byte[0], new LogPosition( logVersion, 16 ) );
        when( reader.readLogEntry( any( ReadableLogChannel.class ) ) ).thenReturn( start, (LogEntry) null );

        // when
        LatestCheckPoint latestCheckPoint = finder.find( logVersion );

        // then
        assertEquals( new LatestCheckPoint( null, true, logVersion ), latestCheckPoint );
    }

    @Test
    public void twoLogFilesNoCheckPoints() throws Throwable
    {
        // given
        setupLogFiles( startLogVersion, endLogVersion );
        LatestCheckPointFinder finder = new LatestCheckPointFinder( logFiles, fs, reader );

        // when
        LatestCheckPoint latestCheckPoint = finder.find( endLogVersion );

        // then
        assertEquals( new LatestCheckPoint( null, false, startLogVersion ), latestCheckPoint );
    }

    @Test
    public void twoLogFilesNoCheckPointsOneStart() throws Throwable
    {
        // given
        setupLogFiles( startLogVersion, endLogVersion );
        LatestCheckPointFinder finder = new LatestCheckPointFinder( logFiles, fs, reader );
        LogEntryStart start = new LogEntryStart( 0, 0, 0, 0, new byte[0], new LogPosition( endLogVersion, 16 ) );
        when( reader.readLogEntry( any( ReadableLogChannel.class ) ) ).thenReturn( start, null );

        // when
        LatestCheckPoint latestCheckPoint = finder.find( endLogVersion );

        // then
        assertEquals( new LatestCheckPoint( null, true, startLogVersion ), latestCheckPoint );
    }

    @Test
    public void latestLogFileContainingACheckPointOnly() throws Throwable
    {
        // given
        setupLogFiles( startLogVersion, endLogVersion );
        LatestCheckPointFinder finder = new LatestCheckPointFinder( logFiles, fs, reader );
        CheckPoint checkPoint = new CheckPoint( new LogPosition( endLogVersion, 33 ) );

        when( reader.readLogEntry( any( ReadableVersionableLogChannel.class ) ) ).thenReturn( checkPoint, null );

        // when
        LatestCheckPoint latestCheckPoint = finder.find( endLogVersion );

        // then
        assertEquals( new LatestCheckPoint( checkPoint, false, endLogVersion ), latestCheckPoint );
    }

    @Test
    public void latestLogFileContainingACheckPointAndAStartBefore() throws Throwable
    {
        // given
        setupLogFiles( startLogVersion, endLogVersion );
        LatestCheckPointFinder finder = new LatestCheckPointFinder( logFiles, fs, reader );
        LogEntryStart start = new LogEntryStart( 0, 0, 0, 0, new byte[0], new LogPosition( endLogVersion, 16 ) );
        CheckPoint checkPoint = new CheckPoint( new LogPosition( endLogVersion, 33 ) );
        when( reader.readLogEntry( any( ReadableLogChannel.class ) ) ).thenReturn( start, checkPoint, null );

        // when
        LatestCheckPoint latestCheckPoint = finder.find( endLogVersion );

        // then
        assertEquals( new LatestCheckPoint( checkPoint, false, endLogVersion ), latestCheckPoint );
    }

    @Test
    public void latestLogFileContainingACheckPointAndAStartAfter() throws Throwable
    {
        // given
        setupLogFiles( startLogVersion, endLogVersion );
        LatestCheckPointFinder finder = new LatestCheckPointFinder( logFiles, fs, reader );
        CheckPoint checkPoint = new CheckPoint( new LogPosition( endLogVersion, 16 ) );
        LogEntryStart start = new LogEntryStart( 0, 0, 0, 0, new byte[0], new LogPosition( endLogVersion, 33 ) );
        when( reader.readLogEntry( any( ReadableLogChannel.class ) ) ).thenReturn( start, checkPoint, null );

        // when
        LatestCheckPoint latestCheckPoint = finder.find( endLogVersion );

        // then
        assertEquals( new LatestCheckPoint( checkPoint, true, endLogVersion ), latestCheckPoint );
    }

    @Test
    public void latestLogFileContainingACheckPointAndAStartAtSamePosition() throws Throwable
    {
        // given
        setupLogFiles( startLogVersion, endLogVersion );
        LatestCheckPointFinder finder = new LatestCheckPointFinder( logFiles, fs, reader );
        CheckPoint checkPoint = new CheckPoint( new LogPosition( endLogVersion, 16 ) );
        LogEntryStart start = new LogEntryStart( 0, 0, 0, 0, new byte[0], new LogPosition( endLogVersion, 16 ) );
        when( reader.readLogEntry( any( ReadableLogChannel.class ) ) ).thenReturn( start, checkPoint, null );

        // when
        LatestCheckPoint latestCheckPoint = finder.find( endLogVersion );

        // then
        assertEquals( new LatestCheckPoint( checkPoint, true, endLogVersion ), latestCheckPoint );
    }

    @Test
    public void latestLogFileContainingMultipleCheckPointsOneStartInBetween() throws Throwable
    {
        // given
        setupLogFiles( startLogVersion, endLogVersion );
        LatestCheckPointFinder finder = new LatestCheckPointFinder( logFiles, fs, reader );
        LogEntryStart start = new LogEntryStart( 0, 0, 0, 0, new byte[0], new LogPosition( endLogVersion, 22 ) );
        CheckPoint checkPoint = new CheckPoint( new LogPosition( endLogVersion, 33 ) );

        when( reader.readLogEntry( any( ReadableVersionableLogChannel.class ) ) ).thenReturn(
                mock( CheckPoint.class ), start, checkPoint, null );

        // when
        LatestCheckPoint latestCheckPoint = finder.find( endLogVersion );

        // then
        assertEquals( new LatestCheckPoint( checkPoint, false, endLogVersion ), latestCheckPoint );
    }

    @Test
    public void latestLogFileContainingMultipleCheckPointsOneStartAfterBoth() throws Throwable
    {
        // given
        setupLogFiles( startLogVersion, endLogVersion );
        LatestCheckPointFinder finder = new LatestCheckPointFinder( logFiles, fs, reader );
        CheckPoint checkPoint = new CheckPoint( new LogPosition( endLogVersion, 22 ) );
        LogEntryStart start = new LogEntryStart( 0, 0, 0, 0, new byte[0], new LogPosition( endLogVersion, 33 ) );

        when( reader.readLogEntry( any( ReadableVersionableLogChannel.class ) ) ).thenReturn(
                mock( CheckPoint.class ), checkPoint, start, null );

        // when
        LatestCheckPoint latestCheckPoint = finder.find( endLogVersion );

        // then
        assertEquals( new LatestCheckPoint( checkPoint, true, endLogVersion ), latestCheckPoint );
    }

    @Test
    public void olderLogFileContainingACheckPointAndNewerFileContainingAStart() throws Throwable
    {
        // given
        setupLogFiles( startLogVersion, endLogVersion );
        LatestCheckPointFinder finder = new LatestCheckPointFinder( logFiles, fs, reader );
        LogEntryStart start1 = new LogEntryStart( 0, 0, 0, 0, new byte[0], new LogPosition( endLogVersion, 22 ) );
        LogEntryStart start2 = new LogEntryStart( 0, 0, 0, 0, new byte[0], new LogPosition( startLogVersion, 16 ) );
        CheckPoint checkPoint = new CheckPoint( new LogPosition( startLogVersion, 33 ) );

        when( reader.readLogEntry( any( ReadableVersionableLogChannel.class ) ) ).thenReturn(
                start1, null, // first file
                start2,  checkPoint, null // second file
        );

        // when
        LatestCheckPoint latestCheckPoint = finder.find( endLogVersion );

        // then
        assertEquals( new LatestCheckPoint( checkPoint, true, startLogVersion ), latestCheckPoint );
    }

    @Test
    public void olderLogFileContainingACheckPointAndNewerFileIsEmpty() throws Throwable
    {
        // given
        setupLogFiles( startLogVersion, endLogVersion );
        LatestCheckPointFinder finder = new LatestCheckPointFinder( logFiles, fs, reader );
        LogEntryStart start = new LogEntryStart( 0, 0, 0, 0, new byte[0], new LogPosition( startLogVersion, 16 ) );
        CheckPoint checkPoint = new CheckPoint( new LogPosition( startLogVersion, 33 ) );

        when( reader.readLogEntry( any( ReadableVersionableLogChannel.class ) ) ).thenReturn(
                null, // first file
                start,  checkPoint, null // second file
        );

        // when
        LatestCheckPoint latestCheckPoint = finder.find( endLogVersion );

        // then
        assertEquals( new LatestCheckPoint( checkPoint, false, startLogVersion ), latestCheckPoint );
    }

    @Test
    public void olderLogFileContainingAStartAndNewerFileContainingACheckPointPointingToAPreviousPositionThanStart() throws Throwable
    {
        // given
        setupLogFiles( startLogVersion, endLogVersion );
        LatestCheckPointFinder finder = new LatestCheckPointFinder( logFiles, fs, reader );
        LogEntryStart start = new LogEntryStart( 0, 0, 0, 0, new byte[0], new LogPosition( startLogVersion, 22 ) );
        CheckPoint checkPoint = new CheckPoint( new LogPosition( startLogVersion, 16 ) );

        when( reader.readLogEntry( any( ReadableVersionableLogChannel.class ) ) ).thenReturn(
                checkPoint, // first file
                start, null // second file
        );

        // when
        LatestCheckPoint latestCheckPoint = finder.find( endLogVersion );

        // then
        assertEquals( new LatestCheckPoint( checkPoint, true, endLogVersion ), latestCheckPoint );
    }

    @Test
    public void olderLogFileContainingAStartAndNewerFileContainingACheckPointPointingToALaterPositionThanStart() throws Throwable
    {
        // given
        setupLogFiles( startLogVersion, endLogVersion );
        LatestCheckPointFinder finder = new LatestCheckPointFinder( logFiles, fs, reader );
        LogEntryStart start = new LogEntryStart( 0, 0, 0, 0, new byte[0], new LogPosition( startLogVersion, 22 ) );
        CheckPoint checkPoint = new CheckPoint( new LogPosition( startLogVersion, 25 ) );

        when( reader.readLogEntry( any( ReadableVersionableLogChannel.class ) ) ).thenReturn(
                checkPoint, // first file
                start, null // second file
        );

        // when
        LatestCheckPoint latestCheckPoint = finder.find( endLogVersion );

        // then
        assertEquals( new LatestCheckPoint( checkPoint, false, endLogVersion ), latestCheckPoint );
    }

    @Test
    public void latestLogEmptyStartEntryBeforeAndAfterCheckPointInTheLastButOneLog() throws Throwable
    {
        // given
        setupLogFiles( startLogVersion, endLogVersion );
        LatestCheckPointFinder finder = new LatestCheckPointFinder( logFiles, fs, reader );
        LogEntry firstStart = new LogEntryStart( 0, 0, 0, 0, new byte[0], new LogPosition( startLogVersion, 20 ) );
        LogEntry secondStart = new LogEntryStart( 0, 0, 0, 0, new byte[0], new LogPosition( startLogVersion, 27 ) );
        CheckPoint checkPoint = new CheckPoint( new LogPosition( startLogVersion, 25 ) );

        when( reader.readLogEntry( any( ReadableLogChannel.class ) ) ).thenReturn(
                null, // first file
                firstStart, checkPoint, secondStart, null // second file
        );

        // when
        LatestCheckPoint latestCheckPoint = finder.find( endLogVersion );

        // then
        assertEquals( new LatestCheckPoint( checkPoint, true, startLogVersion ), latestCheckPoint );
    }

    private void setupLogFiles( int startLogVersion, int endLogVersion ) throws IOException
    {
        when( fs.fileExists( any( File.class ) ) ).thenReturn( false );
        for ( int i = startLogVersion; i <= endLogVersion; i++ )
        {
            File file = mock( File.class );
            when( fs.fileExists( file ) ).thenReturn( true );
            when( logFiles.getLogFileForVersion( i ) ).thenReturn( file );
            StoreChannel channel = mock( StoreChannel.class );
            when( fs.open( eq( file ), anyString() ) ).thenReturn( channel );
            final int version = i;
            when( channel.read( any( ByteBuffer.class ) ) ).thenAnswer( new Answer<Object>()
            {
                @Override
                public Object answer( InvocationOnMock invocationOnMock ) throws Throwable
                {
                    ByteBuffer buffer = (ByteBuffer) invocationOnMock.getArguments()[0];
                    buffer.putLong( encodeLogVersion( version ) );
                    buffer.putLong( 33 );
                    return LOG_HEADER_SIZE;
                }
            } );
        }

        // to make sure we have a mocked file for the file before startLogVersion to avoid NPE...
        if ( startLogVersion > 0 )
        {
            File file = mock( File.class );
            when( logFiles.getLogFileForVersion( startLogVersion - 1 ) ).thenReturn( file );
        }
    }
}
