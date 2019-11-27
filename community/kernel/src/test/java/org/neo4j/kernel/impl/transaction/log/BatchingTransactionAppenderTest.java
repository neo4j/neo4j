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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;

import java.io.Flushable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import org.neo4j.io.memory.ByteBuffers;
import org.neo4j.kernel.impl.api.TestCommand;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFiles;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogCheckPointEvent;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.Health;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.transaction.log.TestLogEntryReader.logEntryReader;
import static org.neo4j.kernel.impl.transaction.log.rotation.LogRotation.NO_ROTATION;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_COMMIT_TIMESTAMP;

@ExtendWith( LifeExtension.class )
class BatchingTransactionAppenderTest
{
    @Inject
    private LifeSupport life;

    private final InMemoryVersionableReadableClosablePositionAwareChannel channel =
            new InMemoryVersionableReadableClosablePositionAwareChannel();
    private final LogAppendEvent logAppendEvent = LogAppendEvent.NULL;
    private final Health databaseHealth = mock( DatabaseHealth.class );
    private final LogFile logFile = mock( LogFile.class );
    private final LogFiles logFiles = mock( TransactionLogFiles.class );
    private final TransactionIdStore transactionIdStore = mock( TransactionIdStore.class );
    private final TransactionMetadataCache positionCache = new TransactionMetadataCache();

    @BeforeEach
    void setUp()
    {
        when( logFiles.getLogFile() ).thenReturn( logFile );
    }

    @Test
    void shouldAppendSingleTransaction() throws Exception
    {
        // GIVEN
        when( logFile.getWriter() ).thenReturn( channel );
        long txId = 15;
        when( transactionIdStore.nextCommittingTransactionId() ).thenReturn( txId );
        when( transactionIdStore.getLastCommittedTransaction() ).thenReturn( new TransactionId( txId, BASE_TX_CHECKSUM, BASE_TX_COMMIT_TIMESTAMP ) );
        TransactionAppender appender = life.add( createTransactionAppender() );

        // WHEN
        TransactionRepresentation transaction = transaction( singleTestCommand(), new byte[]{1, 2, 5}, 12345, 4545, 12345 + 10 );

        appender.append( new TransactionToApply( transaction ), logAppendEvent );

        // THEN
        final LogEntryReader logEntryReader = logEntryReader();
        try ( PhysicalTransactionCursor reader = new PhysicalTransactionCursor( channel, logEntryReader ) )
        {
            reader.next();
            TransactionRepresentation tx = reader.get().getTransactionRepresentation();
            assertArrayEquals( transaction.additionalHeader(), tx.additionalHeader() );
            assertEquals( transaction.getTimeStarted(), tx.getTimeStarted() );
            assertEquals( transaction.getTimeCommitted(), tx.getTimeCommitted() );
            assertEquals( transaction.getLatestCommittedTxWhenStarted(), tx.getLatestCommittedTxWhenStarted() );
        }
    }

    @Test
    void shouldAppendBatchOfTransactions() throws Exception
    {
        // GIVEN
        when( logFile.getWriter() ).thenReturn( channel );
        TransactionAppender appender = life.add( createTransactionAppender() );
        when( transactionIdStore.nextCommittingTransactionId() ).thenReturn( 2L, 3L, 4L );
        TransactionToApply batch = batchOf(
                transaction( singleTestCommand(), new byte[0], 0, 1, 0 ),
                transaction( singleTestCommand(), new byte[0], 0, 1, 0 ),
                transaction( singleTestCommand(), new byte[0], 0, 1, 0 ) );

        // WHEN
        appender.append( batch, logAppendEvent );

        // THEN
        TransactionToApply tx = batch;
        assertEquals( 2L, tx.transactionId() );
        tx = tx.next();
        assertEquals( 3L, tx.transactionId() );
        tx = tx.next();
        assertEquals( 4L, tx.transactionId() );
        assertNull( tx.next() );
    }

    @Test
    void shouldAppendCommittedTransactions() throws Exception
    {
        // GIVEN
        when( logFile.getWriter() ).thenReturn( channel );
        long nextTxId = 15;
        when( transactionIdStore.nextCommittingTransactionId() ).thenReturn( nextTxId );
        when( transactionIdStore.getLastCommittedTransaction() ).thenReturn( new TransactionId( nextTxId, BASE_TX_CHECKSUM, BASE_TX_COMMIT_TIMESTAMP ) );
        TransactionAppender appender = life.add( new BatchingTransactionAppender( logFiles, NO_ROTATION, positionCache,
                transactionIdStore, databaseHealth ) );

        // WHEN
        final byte[] additionalHeader = new byte[]{1, 2, 5};
        final long timeStarted = 12345;
        long latestCommittedTxWhenStarted = nextTxId - 5;
        long timeCommitted = timeStarted + 10;
        PhysicalTransactionRepresentation transactionRepresentation = new PhysicalTransactionRepresentation( singleTestCommand() );
        transactionRepresentation.setHeader( additionalHeader, timeStarted, latestCommittedTxWhenStarted, timeCommitted, -1 );

        LogEntryStart start = new LogEntryStart( 0L, latestCommittedTxWhenStarted, 0, null, LogPosition.UNSPECIFIED );
        LogEntryCommit commit = new LogEntryCommit( nextTxId, 0L, BASE_TX_CHECKSUM );
        CommittedTransactionRepresentation transaction = new CommittedTransactionRepresentation( start, transactionRepresentation, commit );

        appender.append( new TransactionToApply( transactionRepresentation, transaction.getCommitEntry().getTxId() ), logAppendEvent );

        // THEN
        LogEntryReader logEntryReader = logEntryReader();
        try ( PhysicalTransactionCursor reader = new PhysicalTransactionCursor( channel, logEntryReader ) )
        {
            reader.next();
            TransactionRepresentation result = reader.get().getTransactionRepresentation();
            assertArrayEquals( additionalHeader, result.additionalHeader() );
            assertEquals( timeStarted, result.getTimeStarted() );
            assertEquals( timeCommitted, result.getTimeCommitted() );
            assertEquals( latestCommittedTxWhenStarted, result.getLatestCommittedTxWhenStarted() );
        }
    }

    @Test
    void shouldNotAppendCommittedTransactionsWhenTooFarAhead()
    {
        // GIVEN
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        when( logFile.getWriter() ).thenReturn( channel );
        TransactionAppender appender = life.add( createTransactionAppender() );

        // WHEN
        final byte[] additionalHeader = new byte[]{1, 2, 5};
        final long timeStarted = 12345;
        long latestCommittedTxWhenStarted = 4545;
        long timeCommitted = timeStarted + 10;
        PhysicalTransactionRepresentation transactionRepresentation = new PhysicalTransactionRepresentation(
                singleTestCommand() );
        transactionRepresentation.setHeader( additionalHeader, timeStarted,
                latestCommittedTxWhenStarted, timeCommitted, -1 );

        when( transactionIdStore.getLastCommittedTransactionId() ).thenReturn( latestCommittedTxWhenStarted );

        LogEntryStart start = new LogEntryStart(  0L, latestCommittedTxWhenStarted, 0, null, LogPosition.UNSPECIFIED );
        LogEntryCommit commit = new LogEntryCommit( latestCommittedTxWhenStarted + 2, 0L, BASE_TX_CHECKSUM );
        CommittedTransactionRepresentation transaction = new CommittedTransactionRepresentation( start, transactionRepresentation, commit );

        var e = assertThrows( Exception.class, () -> appender.append( new TransactionToApply( transaction.getTransactionRepresentation(),
            transaction.getCommitEntry().getTxId() ), logAppendEvent ) );
        assertThat( e.getMessage() ).contains( "to be applied, but appending it ended up generating an" );
    }

    @Test
    void shouldNotCallTransactionClosedOnFailedAppendedTransaction() throws Exception
    {
        // GIVEN
        long txId = 3;
        String failureMessage = "Forces a failure";
        FlushablePositionAwareChecksumChannel channel =
                spy( new PositionAwarePhysicalFlushableChecksumChannel( mock( PhysicalLogVersionedStoreChannel.class ),
                        ByteBuffers.allocate( Long.BYTES * 2 ) ) );
        IOException failure = new IOException( failureMessage );
        when( channel.putLong( anyLong() ) ).thenThrow( failure );
        when( logFile.getWriter() ).thenReturn( channel );
        when( transactionIdStore.nextCommittingTransactionId() ).thenReturn( txId );
        when( transactionIdStore.getLastCommittedTransaction() ).thenReturn( new TransactionId( txId, BASE_TX_CHECKSUM, BASE_TX_COMMIT_TIMESTAMP ) );
        Mockito.reset( databaseHealth );
        TransactionAppender appender = life.add( createTransactionAppender() );

        // WHEN
        TransactionRepresentation transaction = mock( TransactionRepresentation.class );
        when( transaction.additionalHeader() ).thenReturn( new byte[0] );

        var e = assertThrows( IOException.class, () -> appender.append( new TransactionToApply( transaction ), logAppendEvent ) );
        assertSame( failure, e );
        verify( transactionIdStore ).nextCommittingTransactionId();
        verify( transactionIdStore, never() ).transactionClosed( eq( txId ), anyLong(), anyLong() );
        verify( databaseHealth ).panic( failure );
    }

    @Test
    void shouldNotCallTransactionClosedOnFailedForceLogToDisk() throws Exception
    {
        // GIVEN
        long txId = 3;
        String failureMessage = "Forces a failure";
        FlushablePositionAwareChecksumChannel channel = spy( new InMemoryClosableChannel() );
        IOException failure = new IOException( failureMessage );
        final Flushable flushable = mock( Flushable.class );
        doAnswer( invocation ->
        {
            invocation.callRealMethod();
            return flushable;
        } ).when( channel ).prepareForFlush();
        doThrow( failure ).when( flushable ).flush();
        when( logFile.getWriter() ).thenReturn( channel );
        TransactionMetadataCache metadataCache = new TransactionMetadataCache();
        TransactionIdStore transactionIdStore = mock( TransactionIdStore.class );
        when( transactionIdStore.nextCommittingTransactionId() ).thenReturn( txId );
        when( transactionIdStore.getLastCommittedTransaction() ).thenReturn( new TransactionId( txId, BASE_TX_CHECKSUM, BASE_TX_COMMIT_TIMESTAMP ) );
        Mockito.reset( databaseHealth );
        TransactionAppender appender = life.add( new BatchingTransactionAppender( logFiles, NO_ROTATION,
                metadataCache, transactionIdStore, databaseHealth ) );

        // WHEN
        TransactionRepresentation transaction = mock( TransactionRepresentation.class );
        when( transaction.additionalHeader() ).thenReturn( new byte[0] );

        var e = assertThrows( IOException.class, () -> appender.append( new TransactionToApply( transaction ), logAppendEvent ) );
        assertSame( failure, e );
        verify( transactionIdStore ).nextCommittingTransactionId();
        verify( transactionIdStore, never() ).transactionClosed( eq( txId ), anyLong(), anyLong() );
        verify( databaseHealth ).panic( failure );
    }

    @Test
    void shouldBeAbleToWriteACheckPoint() throws Throwable
    {
        // Given
        FlushablePositionAwareChecksumChannel channel = mock( FlushablePositionAwareChecksumChannel.class, RETURNS_MOCKS );
        Flushable flushable = mock( Flushable.class );
        when( channel.prepareForFlush() ).thenReturn( flushable );
        when( channel.putLong( anyLong() ) ).thenReturn( channel );
        when( logFile.getWriter() ).thenReturn( channel );
        BatchingTransactionAppender appender = life.add( createTransactionAppender() );

        // When
        appender.checkPoint( new LogPosition( 1L, 2L ), LogCheckPointEvent.NULL );

        // Then
        verify( channel ).putLong( 1L );
        verify( channel ).putLong( 2L );
        verify( channel ).prepareForFlush();
        verify( flushable ).flush();
        verify( databaseHealth, never() ).panic( any() );
    }

    @Test
    void shouldKernelPanicIfNotAbleToWriteACheckPoint() throws Throwable
    {
        // Given
        IOException ioex = new IOException( "boom!" );
        FlushablePositionAwareChecksumChannel channel = mock( FlushablePositionAwareChecksumChannel.class, RETURNS_MOCKS );
        when( channel.put( anyByte() ) ).thenReturn( channel );
        when( channel.putLong( anyLong() ) ).thenThrow( ioex );
        when( channel.put( anyByte() ) ).thenThrow( ioex );
        when( logFile.getWriter() ).thenReturn( channel );
        BatchingTransactionAppender appender = life.add( createTransactionAppender() );

        // When
        var e = assertThrows( IOException.class, () -> appender.checkPoint( new LogPosition( 0L, 0L ), LogCheckPointEvent.NULL ) );
        assertEquals( ioex, e );

        // Then
        verify( databaseHealth ).panic( ioex );
    }

    @Test
    void shouldKernelPanicIfTransactionIdsMismatch()
    {
        // Given
        BatchingTransactionAppender appender = life.add( createTransactionAppender() );
        when( transactionIdStore.nextCommittingTransactionId() ).thenReturn( 42L );
        TransactionToApply batch = new TransactionToApply( mock( TransactionRepresentation.class ), 43L );
        // When
        var e = assertThrows( IllegalStateException.class, () -> appender.append( batch, LogAppendEvent.NULL ) );
        // Then
        verify( databaseHealth ).panic( e );
    }

    private BatchingTransactionAppender createTransactionAppender()
    {
        return new BatchingTransactionAppender( logFiles, NO_ROTATION, positionCache, transactionIdStore, databaseHealth, BASE_TX_CHECKSUM );
    }

    private TransactionRepresentation transaction( Collection<StorageCommand> commands, byte[] additionalHeader, long timeStarted,
            long latestCommittedTxWhenStarted, long timeCommitted )
    {
        PhysicalTransactionRepresentation tx = new PhysicalTransactionRepresentation( commands );
        tx.setHeader( additionalHeader, timeStarted, latestCommittedTxWhenStarted, timeCommitted, -1 );
        return tx;
    }

    private Collection<StorageCommand> singleTestCommand()
    {
        return Collections.singletonList( new TestCommand() );
    }

    private TransactionToApply batchOf( TransactionRepresentation... transactions )
    {
        TransactionToApply first = null;
        TransactionToApply last = null;
        for ( TransactionRepresentation transaction : transactions )
        {
            TransactionToApply tx = new TransactionToApply( transaction );
            if ( first == null )
            {
                first = last = tx;
            }
            else
            {
                last.next( tx );
                last = tx;
            }
        }
        return first;
    }
}
