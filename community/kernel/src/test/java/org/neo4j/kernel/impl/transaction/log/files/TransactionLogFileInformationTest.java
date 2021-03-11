/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.impl.transaction.log.files;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import org.neo4j.kernel.impl.transaction.log.LogHeaderCache;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_FORMAT_LOG_HEADER_SIZE;

class TransactionLogFileInformationTest
{
    private final LogFiles logFiles = mock( TransactionLogFiles.class );
    private final LogFile logFile = mock( TransactionLogFile.class );
    private final LogHeaderCache logHeaderCache = mock( LogHeaderCache.class );
    private final TransactionLogFilesContext context = mock( TransactionLogFilesContext.class );

    @BeforeEach
    void setUp()
    {
        when( logFiles.getLogFile() ).thenReturn( logFile );
    }

    @Test
    void shouldReadAndCacheFirstCommittedTransactionIdForAGivenVersionWhenNotCached() throws Exception
    {
        TransactionLogFileInformation info = new TransactionLogFileInformation( logFiles, logHeaderCache, context );
        long expected = 5;

        long version = 10L;
        when( logHeaderCache.getLogHeader( version ) ).thenReturn( null );
        when( logFiles.getLogFile().versionExists( version ) ).thenReturn( true );
        LogHeader expectedHeader = new LogHeader( (byte) -1/*ignored*/, -1L/*ignored*/, expected - 1L, CURRENT_FORMAT_LOG_HEADER_SIZE );
        when( logFiles.getLogFile().extractHeader( version ) ).thenReturn( expectedHeader );

        long firstCommittedTxId = info.getFirstEntryId( version );
        assertEquals( expected, firstCommittedTxId );
        verify( logHeaderCache ).putHeader( version, expectedHeader );
    }

    @Test
    void shouldReadFirstCommittedTransactionIdForAGivenVersionWhenCached() throws Exception
    {
        TransactionLogFileInformation info = new TransactionLogFileInformation( logFiles, logHeaderCache, context );
        long expected = 5;

        long version = 10L;
        LogHeader expectedHeader = new LogHeader( (byte) -1/*ignored*/, -1L/*ignored*/, expected - 1L, CURRENT_FORMAT_LOG_HEADER_SIZE );
        when( logHeaderCache.getLogHeader( version ) ).thenReturn( expectedHeader );

        long firstCommittedTxId = info.getFirstEntryId( version );
        assertEquals( expected, firstCommittedTxId );
    }

    @Test
    void shouldReadAndCacheFirstCommittedTransactionIdWhenNotCached() throws Exception
    {
        TransactionLogFileInformation info = new TransactionLogFileInformation( logFiles, logHeaderCache, context );
        long expected = 5;

        long version = 10L;
        when( logFile.getHighestLogVersion() ).thenReturn( version );
        when( logHeaderCache.getLogHeader( version ) ).thenReturn( null );
        when( logFile.versionExists( version ) ).thenReturn( true );
        LogHeader expectedHeader = new LogHeader( (byte) -1/*ignored*/, -1L/*ignored*/, expected - 1L, CURRENT_FORMAT_LOG_HEADER_SIZE );
        when( logFile.extractHeader( version ) ).thenReturn( expectedHeader );
        when( logFile.hasAnyEntries( version ) ).thenReturn( true );

        long firstCommittedTxId = info.getFirstExistingEntryId();
        assertEquals( expected, firstCommittedTxId );
        verify( logHeaderCache ).putHeader( version, expectedHeader );
    }

    @Test
    void shouldReadFirstCommittedTransactionIdWhenCached() throws Exception
    {
        TransactionLogFileInformation info = new TransactionLogFileInformation( logFiles, logHeaderCache, context );
        long expected = 5;

        long version = 10L;
        when( logFile.getHighestLogVersion() ).thenReturn( version );
        when( logFile.versionExists( version ) ).thenReturn( true );

        LogHeader expectedHeader = new LogHeader( (byte) -1/*ignored*/, -1L/*ignored*/, expected - 1L, CURRENT_FORMAT_LOG_HEADER_SIZE );
        when( logHeaderCache.getLogHeader( version ) ).thenReturn( expectedHeader );
        when( logFile.hasAnyEntries( version ) ).thenReturn( true );

        long firstCommittedTxId = info.getFirstExistingEntryId();
        assertEquals( expected, firstCommittedTxId );
    }

    @Test
    void shouldReturnNothingWhenThereAreNoTransactions() throws Exception
    {
        TransactionLogFileInformation info = new TransactionLogFileInformation( logFiles, logHeaderCache, context );

        long version = 10L;
        when( logFile.getHighestLogVersion() ).thenReturn( version );
        when( logFile.hasAnyEntries( version ) ).thenReturn( false );

        long firstCommittedTxId = info.getFirstExistingEntryId();
        assertEquals( -1, firstCommittedTxId );
    }

    @Test
    void doNotReadAgainPreviouslyObserverLogTransactionTime() throws IOException
    {
        var logEntryReader = mock( LogEntryReader.class );
        var readableLogChannel = mock( ReadableLogChannel.class );
        when( logEntryReader.readLogEntry( readableLogChannel ) ).thenReturn( new LogEntryStart( 1, 1, 1, new byte[]{}, LogPosition.UNSPECIFIED ) );
        when( context.getLogEntryReader() ).thenReturn( logEntryReader );
        var fileInfo = new TransactionLogFileInformation( logFiles, logHeaderCache, context );

        var expectedHeader = new LogHeader( (byte) 1, 2, 3, 4 );
        when( logFile.extractHeader( anyLong() ) ).thenReturn( expectedHeader );
        when( logFile.getReader( any() ) ).thenReturn( readableLogChannel );

        fileInfo.getFirstStartRecordTimestamp( 1 );
        fileInfo.getFirstStartRecordTimestamp( 1 );
        fileInfo.getFirstStartRecordTimestamp( 1 );
        fileInfo.getFirstStartRecordTimestamp( 1 );
        fileInfo.getFirstStartRecordTimestamp( 1 );

        verify( logFile, times( 1 ) ).getReader( any() );
    }
}
