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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.ReadableClosableChannel;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.transaction.SimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.SimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.entry.IncompleteLogHeaderException;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.storageengine.api.LogVersionRepository;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.transaction.log.TestLogEntryReader.logEntryReader;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader.readLogHeader;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_COMMIT_TIMESTAMP;

@TestDirectoryExtension
@ExtendWith( LifeExtension.class )
class TransactionLogFileTest
{
    @Inject
    private TestDirectory directory;
    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private LifeSupport life;

    private final LogVersionRepository logVersionRepository = new SimpleLogVersionRepository( 1L );
    private final TransactionIdStore transactionIdStore =
            new SimpleTransactionIdStore( 2L, 0, BASE_TX_COMMIT_TIMESTAMP, 0, 0 );

    @Test
    void skipLogFileWithoutHeader() throws IOException
    {
        LogFiles logFiles = LogFilesBuilder.builder( directory.databaseLayout(), fileSystem )
                                           .withTransactionIdStore( transactionIdStore )
                                           .withLogVersionRepository( logVersionRepository )
                                           .withLogEntryReader( logEntryReader() )
                                           .build();
        life.add( logFiles );
        life.start();

        // simulate new file without header presence
        logVersionRepository.incrementAndGetVersion();
        fileSystem.write( logFiles.getLogFileForVersion( logVersionRepository.getCurrentLogVersion() ) ).close();
        transactionIdStore.transactionCommitted( 5L, 5L, 5L );

        PhysicalLogicalTransactionStore.LogVersionLocator versionLocator = new PhysicalLogicalTransactionStore.LogVersionLocator( 4L );
        logFiles.accept( versionLocator );

        LogPosition logPosition = versionLocator.getLogPosition();
        assertEquals( 1, logPosition.getLogVersion() );
    }

    @Test
    void shouldOpenInFreshDirectoryAndFinallyAddHeader() throws Exception
    {
        // GIVEN
        String name = "log";
        LogFiles logFiles = LogFilesBuilder.builder( directory.databaseLayout(), fileSystem )
                .withTransactionIdStore( transactionIdStore )
                .withLogVersionRepository( logVersionRepository )
                .withLogEntryReader( logEntryReader() )
                .build();

        // WHEN
        life.start();
        life.add( logFiles );
        life.shutdown();

        // THEN
        File file =  LogFilesBuilder.logFilesBasedOnlyBuilder( directory.databaseLayout().getTransactionLogsDirectory(), fileSystem )
                .withLogEntryReader( logEntryReader() )
                .build().getLogFileForVersion( 1L );
        LogHeader header = readLogHeader( fileSystem, file );
        assertEquals( 1L, header.logVersion );
        assertEquals( 2L, header.lastCommittedTxId );
    }

    @Test
    void shouldWriteSomeDataIntoTheLog() throws Exception
    {
        // GIVEN
        String name = "log";
        LogFiles logFiles = LogFilesBuilder.builder( directory.databaseLayout(), fileSystem )
                .withTransactionIdStore( transactionIdStore )
                .withLogVersionRepository( logVersionRepository )
                .withLogEntryReader( logEntryReader() )
                .build();
        life.start();
        life.add( logFiles );

        // WHEN
        FlushablePositionAwareChannel writer = logFiles.getLogFile().getWriter();
        LogPositionMarker positionMarker = new LogPositionMarker();
        writer.getCurrentPosition( positionMarker );
        int intValue = 45;
        long longValue = 4854587;
        writer.putInt( intValue );
        writer.putLong( longValue );
        writer.prepareForFlush().flush();

        // THEN
        try ( ReadableClosableChannel reader = logFiles.getLogFile().getReader( positionMarker.newPosition() ) )
        {
            assertEquals( intValue, reader.getInt() );
            assertEquals( longValue, reader.getLong() );
        }
    }

    @Test
    void shouldReadOlderLogs() throws Exception
    {
        // GIVEN
        LogFiles logFiles = LogFilesBuilder.builder( directory.databaseLayout(), fileSystem )
                .withTransactionIdStore( transactionIdStore )
                .withLogVersionRepository( logVersionRepository )
                .withLogEntryReader( logEntryReader() )
                .build();
        life.start();
        life.add( logFiles );

        // WHEN
        LogFile logFile = logFiles.getLogFile();
        FlushablePositionAwareChannel writer = logFile.getWriter();
        LogPositionMarker positionMarker = new LogPositionMarker();
        writer.getCurrentPosition( positionMarker );
        LogPosition position1 = positionMarker.newPosition();
        int intValue = 45;
        long longValue = 4854587;
        byte[] someBytes = someBytes( 40 );
        writer.putInt( intValue );
        writer.putLong( longValue );
        writer.put( someBytes, someBytes.length );
        writer.prepareForFlush().flush();
        writer.getCurrentPosition( positionMarker );
        LogPosition position2 = positionMarker.newPosition();
        long longValue2 = 123456789L;
        writer.putLong( longValue2 );
        writer.put( someBytes, someBytes.length );
        writer.prepareForFlush().flush();

        // THEN
        try ( ReadableClosableChannel reader = logFile.getReader( position1 ) )
        {
            assertEquals( intValue, reader.getInt() );
            assertEquals( longValue, reader.getLong() );
            assertArrayEquals( someBytes, readBytes( reader, 40 ) );
        }
        try ( ReadableClosableChannel reader = logFile.getReader( position2 ) )
        {
            assertEquals( longValue2, reader.getLong() );
            assertArrayEquals( someBytes, readBytes( reader, 40 ) );
        }
    }

    @Test
    void shouldVisitLogFile() throws Exception
    {
        // GIVEN
        String name = "log";
        LogFiles logFiles = LogFilesBuilder.builder( directory.databaseLayout(), fileSystem )
                .withTransactionIdStore( transactionIdStore )
                .withLogVersionRepository( logVersionRepository )
                .withLogEntryReader( logEntryReader() )
                .build();
        life.start();
        life.add( logFiles );

        LogFile logFile = logFiles.getLogFile();
        FlushablePositionAwareChannel writer = logFile.getWriter();
        LogPositionMarker mark = new LogPositionMarker();
        writer.getCurrentPosition( mark );
        for ( int i = 0; i < 5; i++ )
        {
            writer.put( (byte)i );
        }
        writer.prepareForFlush();

        // WHEN/THEN
        final AtomicBoolean called = new AtomicBoolean();
        logFile.accept( channel ->
        {
            for ( int i = 0; i < 5; i++ )
            {
                assertEquals( (byte)i, channel.get() );
            }
            called.set( true );
            return true;
        }, mark.newPosition() );
        assertTrue( called.get() );
    }

    @Test
    void shouldCloseChannelInFailedAttemptToReadHeaderAfterOpen() throws Exception
    {
        // GIVEN a file which returns 1/2 log header size worth of bytes
        FileSystemAbstraction fs = mock( FileSystemAbstraction.class );
        LogFiles logFiles = LogFilesBuilder.builder( directory.databaseLayout(), fs )
                .withTransactionIdStore( transactionIdStore )
                .withLogVersionRepository( logVersionRepository )
                .withLogEntryReader( logEntryReader() )
                .build();
        int logVersion = 0;
        File logFile = logFiles.getLogFileForVersion( logVersion );
        StoreChannel channel = mock( StoreChannel.class );
        when( channel.read( any( ByteBuffer.class ) ) ).thenReturn( LogHeader.LOG_HEADER_SIZE / 2 );
        when( fs.fileExists( logFile ) ).thenReturn( true );
        when( fs.read( eq( logFile ) ) ).thenReturn( channel );

        // WHEN
        assertThrows( IncompleteLogHeaderException.class, () -> logFiles.openForVersion( logVersion ) );
        verify( channel ).close();
    }

    @Test
    void shouldSuppressFailureToCloseChannelInFailedAttemptToReadHeaderAfterOpen() throws Exception
    {
        // GIVEN a file which returns 1/2 log header size worth of bytes
        FileSystemAbstraction fs = mock( FileSystemAbstraction.class );
        LogFiles logFiles = LogFilesBuilder.builder( directory.databaseLayout(), fs )
                .withTransactionIdStore( transactionIdStore )
                .withLogVersionRepository( logVersionRepository )
                .withLogEntryReader( logEntryReader() )
                .build();
        int logVersion = 0;
        File logFile = logFiles.getLogFileForVersion( logVersion );
        StoreChannel channel = mock( StoreChannel.class );
        when( channel.read( any( ByteBuffer.class ) ) ).thenReturn( LogHeader.LOG_HEADER_SIZE / 2 );
        when( fs.fileExists( logFile ) ).thenReturn( true );
        when( fs.read( eq( logFile ) ) ).thenReturn( channel );
        doThrow( IOException.class ).when( channel ).close();

        // WHEN
        IncompleteLogHeaderException exception =
                assertThrows( IncompleteLogHeaderException.class, () -> logFiles.openForVersion( logVersion ) );
        verify( channel ).close();
        assertEquals( 1, exception.getSuppressed().length );
        assertTrue( exception.getSuppressed()[0] instanceof IOException );
    }

    @Test
    void closeChannelThrowExceptionOnAttemptToAppendTransactionLogRecords() throws IOException
    {
        LogFiles logFiles = LogFilesBuilder.builder( directory.databaseLayout(), fileSystem )
                .withTransactionIdStore( transactionIdStore )
                .withLogVersionRepository( logVersionRepository )
                .withLogEntryReader( logEntryReader() )
                .build();
        life.start();
        life.add( logFiles );

        LogFile logFile = logFiles.getLogFile();
        FlushablePositionAwareChannel writer = logFile.getWriter();

        life.shutdown();

        assertThrows( Throwable.class, () -> writer.put( (byte) 7 ) );
        assertThrows( Throwable.class, () -> writer.putInt( 7 ) );
        assertThrows( Throwable.class, () -> writer.putLong( 7 ) );
        assertThrows( Throwable.class, () -> writer.putDouble( 7 ) );
        assertThrows( Throwable.class, () -> writer.putFloat( 7 ) );
        assertThrows( Throwable.class, () -> writer.putShort( (short) 7 ) );
        assertThrows( Throwable.class, () -> writer.put( new byte[]{1, 2, 3}, 3 ) );
        assertThrows( IllegalStateException.class, () -> writer.prepareForFlush().flush() );
    }

    private static byte[] readBytes( ReadableClosableChannel reader, int length ) throws IOException
    {
        byte[] result = new byte[length];
        reader.get( result, length );
        return result;
    }

    private static byte[] someBytes( int length )
    {
        byte[] result = new byte[length];
        for ( int i = 0; i < length; i++ )
        {
            result[i] = (byte) (i % 5);
        }
        return result;
    }
}
