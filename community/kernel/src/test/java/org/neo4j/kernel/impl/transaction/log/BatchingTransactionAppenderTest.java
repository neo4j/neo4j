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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.Flushable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.command.Command.NodeCommand;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFiles;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogCheckPointEvent;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.lifecycle.LifeRule;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.test.rule.CleanupRule;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.transaction.log.rotation.LogRotation.NO_ROTATION;
import static org.neo4j.kernel.impl.util.IdOrderingQueue.BYPASS;

public class BatchingTransactionAppenderTest
{
    @Rule
    public final LifeRule life = new LifeRule( true );
    @Rule
    public final CleanupRule cleanup = new CleanupRule();

    private final InMemoryVersionableReadableClosablePositionAwareChannel channel =
            new InMemoryVersionableReadableClosablePositionAwareChannel();
    private final LogAppendEvent logAppendEvent = LogAppendEvent.NULL;
    private final DatabaseHealth databaseHealth = mock( DatabaseHealth.class );
    private final LogFile logFile = mock( LogFile.class );
    private final LogFiles logFiles = mock( TransactionLogFiles.class );
    private final TransactionIdStore transactionIdStore = mock( TransactionIdStore.class );
    private final TransactionMetadataCache positionCache = new TransactionMetadataCache( 10 );

    @Before
    public void setUp()
    {
        when( logFiles.getLogFile() ).thenReturn( logFile );
    }

    @Test
    public void shouldAppendSingleTransaction() throws Exception
    {
        // GIVEN
        when( logFile.getWriter() ).thenReturn( channel );
        long txId = 15;
        when( transactionIdStore.nextCommittingTransactionId() ).thenReturn( txId );
        TransactionAppender appender = life.add( createTransactionAppender() );

        // WHEN
        TransactionRepresentation transaction = transaction( singleCreateNodeCommand( 0 ),
                new byte[]{1, 2, 5}, 2, 1, 12345, 4545, 12345 + 10 );

        appender.append( new TransactionToApply( transaction ), logAppendEvent );

        // THEN
        final LogEntryReader<ReadableLogChannel> logEntryReader = new VersionAwareLogEntryReader<>();
        try ( PhysicalTransactionCursor<ReadableLogChannel> reader =
                      new PhysicalTransactionCursor<>( channel, logEntryReader ) )
        {
            reader.next();
            TransactionRepresentation tx = reader.get().getTransactionRepresentation();
            assertArrayEquals( transaction.additionalHeader(), tx.additionalHeader() );
            assertEquals( transaction.getMasterId(), tx.getMasterId() );
            assertEquals( transaction.getAuthorId(), tx.getAuthorId() );
            assertEquals( transaction.getTimeStarted(), tx.getTimeStarted() );
            assertEquals( transaction.getTimeCommitted(), tx.getTimeCommitted() );
            assertEquals( transaction.getLatestCommittedTxWhenStarted(), tx.getLatestCommittedTxWhenStarted() );
        }
    }

    @Test
    public void shouldAppendBatchOfTransactions() throws Exception
    {
        // GIVEN
        when( logFile.getWriter() ).thenReturn( channel );
        TransactionAppender appender = life.add( createTransactionAppender() );
        when( transactionIdStore.nextCommittingTransactionId() ).thenReturn( 2L, 3L, 4L );
        TransactionToApply batch = batchOf(
                transaction( singleCreateNodeCommand( 0 ), new byte[0], 0, 0, 0, 1, 0 ),
                transaction( singleCreateNodeCommand( 1 ), new byte[0], 0, 0, 0, 1, 0 ),
                transaction( singleCreateNodeCommand( 2 ), new byte[0], 0, 0, 0, 1, 0 ) );

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
    public void shouldAppendCommittedTransactions() throws Exception
    {
        // GIVEN
        when( logFile.getWriter() ).thenReturn( channel );
        long nextTxId = 15;
        when( transactionIdStore.nextCommittingTransactionId() ).thenReturn( nextTxId );
        TransactionAppender appender = life.add( new BatchingTransactionAppender( logFiles, NO_ROTATION, positionCache,
                transactionIdStore, BYPASS, databaseHealth ) );

        // WHEN
        final byte[] additionalHeader = new byte[]{1, 2, 5};
        final int masterId = 2;
        int authorId = 1;
        final long timeStarted = 12345;
        long latestCommittedTxWhenStarted = nextTxId - 5;
        long timeCommitted = timeStarted + 10;
        PhysicalTransactionRepresentation transactionRepresentation = new PhysicalTransactionRepresentation(
                singleCreateNodeCommand( 0 ) );
        transactionRepresentation.setHeader( additionalHeader, masterId, authorId, timeStarted,
                latestCommittedTxWhenStarted, timeCommitted, -1 );

        LogEntryStart start = new LogEntryStart( 0, 0, 0L, latestCommittedTxWhenStarted, null,
                LogPosition.UNSPECIFIED );
        LogEntryCommit commit = new LogEntryCommit( nextTxId, 0L );
        CommittedTransactionRepresentation transaction =
                new CommittedTransactionRepresentation( start, transactionRepresentation, commit );

        appender.append( new TransactionToApply( transactionRepresentation, transaction.getCommitEntry().getTxId() ),
                logAppendEvent );

        // THEN
        LogEntryReader<ReadableLogChannel> logEntryReader = new VersionAwareLogEntryReader<>();
        try ( PhysicalTransactionCursor<ReadableLogChannel> reader =
                      new PhysicalTransactionCursor<>( channel, logEntryReader ) )
        {
            reader.next();
            TransactionRepresentation result = reader.get().getTransactionRepresentation();
            assertArrayEquals( additionalHeader, result.additionalHeader() );
            assertEquals( masterId, result.getMasterId() );
            assertEquals( authorId, result.getAuthorId() );
            assertEquals( timeStarted, result.getTimeStarted() );
            assertEquals( timeCommitted, result.getTimeCommitted() );
            assertEquals( latestCommittedTxWhenStarted, result.getLatestCommittedTxWhenStarted() );
        }
    }

    @Test
    public void shouldNotAppendCommittedTransactionsWhenTooFarAhead()
    {
        // GIVEN
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        when( logFile.getWriter() ).thenReturn( channel );
        TransactionAppender appender = life.add( createTransactionAppender() );

        // WHEN
        final byte[] additionalHeader = new byte[]{1, 2, 5};
        final int masterId = 2;
        int authorId = 1;
        final long timeStarted = 12345;
        long latestCommittedTxWhenStarted = 4545;
        long timeCommitted = timeStarted + 10;
        PhysicalTransactionRepresentation transactionRepresentation = new PhysicalTransactionRepresentation(
                singleCreateNodeCommand( 0 ) );
        transactionRepresentation.setHeader( additionalHeader, masterId, authorId, timeStarted,
                latestCommittedTxWhenStarted, timeCommitted, -1 );

        when( transactionIdStore.getLastCommittedTransactionId() ).thenReturn( latestCommittedTxWhenStarted );

        LogEntryStart start = new LogEntryStart( 0, 0, 0L, latestCommittedTxWhenStarted, null,
                LogPosition.UNSPECIFIED );
        LogEntryCommit commit = new LogEntryCommit( latestCommittedTxWhenStarted + 2, 0L );
        CommittedTransactionRepresentation transaction =
                new CommittedTransactionRepresentation( start, transactionRepresentation, commit );

        try
        {
            appender.append( new TransactionToApply( transaction.getTransactionRepresentation(),
                    transaction.getCommitEntry().getTxId() ), logAppendEvent );
            fail( "should have thrown" );
        }
        catch ( Throwable e )
        {
            assertThat( e.getMessage(), containsString( "to be applied, but appending it ended up generating an" ) );
        }
    }

    @Test
    public void shouldNotCallTransactionClosedOnFailedAppendedTransaction() throws Exception
    {
        // GIVEN
        long txId = 3;
        String failureMessage = "Forces a failure";
        FlushablePositionAwareChannel channel =
                spy( new PositionAwarePhysicalFlushableChannel( mock( PhysicalLogVersionedStoreChannel.class ) ) );
        IOException failure = new IOException( failureMessage );
        when( channel.putInt( anyInt() ) ).thenThrow( failure );
        when( logFile.getWriter() ).thenReturn( channel );
        when( transactionIdStore.nextCommittingTransactionId() ).thenReturn( txId );
        Mockito.reset( databaseHealth );
        TransactionAppender appender = life.add( createTransactionAppender() );

        // WHEN
        TransactionRepresentation transaction = mock( TransactionRepresentation.class );
        when( transaction.additionalHeader() ).thenReturn( new byte[0] );
        try
        {
            appender.append( new TransactionToApply( transaction ), logAppendEvent );
            fail( "Expected append to fail. Something is wrong with the test itself" );
        }
        catch ( IOException e )
        {
            // THEN
            assertSame( failure, e );
            verify( transactionIdStore, times( 1 ) ).nextCommittingTransactionId();
            verify( transactionIdStore, never() ).transactionClosed( eq( txId ), anyLong(), anyLong() );
            verify( databaseHealth ).panic( failure );
        }
    }

    @Test
    public void shouldNotCallTransactionClosedOnFailedForceLogToDisk() throws Exception
    {
        // GIVEN
        long txId = 3;
        String failureMessage = "Forces a failure";
        FlushablePositionAwareChannel channel = spy( new InMemoryClosableChannel() );
        IOException failure = new IOException( failureMessage );
        final Flushable flushable = mock( Flushable.class );
        doAnswer( invocation ->
        {
            invocation.callRealMethod();
            return flushable;
        } ).when( channel ).prepareForFlush();
        doThrow( failure ).when( flushable ).flush();
        when( logFile.getWriter() ).thenReturn( channel );
        TransactionMetadataCache metadataCache = new TransactionMetadataCache( 10 );
        TransactionIdStore transactionIdStore = mock( TransactionIdStore.class );
        when( transactionIdStore.nextCommittingTransactionId() ).thenReturn( txId );
        Mockito.reset( databaseHealth );
        TransactionAppender appender = life.add( new BatchingTransactionAppender( logFiles, NO_ROTATION,
                metadataCache, transactionIdStore, BYPASS, databaseHealth ) );

        // WHEN
        TransactionRepresentation transaction = mock( TransactionRepresentation.class );
        when( transaction.additionalHeader() ).thenReturn( new byte[0] );
        try
        {
            appender.append( new TransactionToApply( transaction ), logAppendEvent );
            fail( "Expected append to fail. Something is wrong with the test itself" );
        }
        catch ( IOException e )
        {
            // THEN
            assertSame( failure, e );
            verify( transactionIdStore, times( 1 ) ).nextCommittingTransactionId();
            verify( transactionIdStore, never() ).transactionClosed( eq( txId ), anyLong(), anyLong() );
            verify( databaseHealth ).panic( failure );
        }
    }

    @Test
    public void shouldBeAbleToWriteACheckPoint() throws Throwable
    {
        // Given
        FlushablePositionAwareChannel channel = mock( FlushablePositionAwareChannel.class, RETURNS_MOCKS );
        Flushable flushable = mock( Flushable.class );
        when( channel.prepareForFlush() ).thenReturn( flushable );
        when( channel.putLong( anyLong() ) ).thenReturn( channel );
        when( logFile.getWriter() ).thenReturn( channel );
        BatchingTransactionAppender appender = life.add( createTransactionAppender() );

        // When
        appender.checkPoint( new LogPosition( 1L, 2L ), LogCheckPointEvent.NULL );

        // Then
        verify( channel, times( 1 ) ).putLong( 1L );
        verify( channel, times( 1 ) ).putLong( 2L );
        verify( channel, times( 1 ) ).prepareForFlush();
        verify( flushable, times( 1 ) ).flush();
        verifyZeroInteractions( databaseHealth );
    }

    @Test
    public void shouldKernelPanicIfNotAbleToWriteACheckPoint() throws Throwable
    {
        // Given
        IOException ioex = new IOException( "boom!" );
        FlushablePositionAwareChannel channel = mock( FlushablePositionAwareChannel.class, RETURNS_MOCKS );
        when( channel.put( anyByte() ) ).thenReturn( channel );
        when( channel.putLong( anyLong() ) ).thenThrow( ioex );
        when( channel.put( anyByte() ) ).thenThrow( ioex );
        when( logFile.getWriter() ).thenReturn( channel );
        BatchingTransactionAppender appender = life.add( createTransactionAppender() );

        // When
        try
        {
            appender.checkPoint( new LogPosition( 0L, 0L ), LogCheckPointEvent.NULL );
            fail( "should have thrown " );
        }
        catch ( IOException ex )
        {
            assertEquals( ioex, ex );
        }

        // Then
        verify( databaseHealth, times( 1 ) ).panic( ioex );
    }

    @Test
    public void shouldKernelPanicIfTransactionIdsMismatch() throws Throwable
    {
        // Given
        BatchingTransactionAppender appender = life.add( createTransactionAppender() );
        when( transactionIdStore.nextCommittingTransactionId() ).thenReturn( 42L );
        TransactionToApply batch = new TransactionToApply( mock( TransactionRepresentation.class ), 43L );

        // When
        try
        {
            appender.append( batch, LogAppendEvent.NULL );
            fail( "should have thrown " );
        }
        catch ( IllegalStateException ex )
        {
            // Then
            verify( databaseHealth, times( 1 ) ).panic( ex );
        }

    }

    private BatchingTransactionAppender createTransactionAppender()
    {
        return new BatchingTransactionAppender( logFiles, NO_ROTATION, positionCache, transactionIdStore, BYPASS, databaseHealth );
    }

    private TransactionRepresentation transaction( Collection<StorageCommand> commands, byte[] additionalHeader,
            int masterId, int authorId, long timeStarted, long latestCommittedTxWhenStarted, long timeCommitted )
    {
        PhysicalTransactionRepresentation tx = new PhysicalTransactionRepresentation( commands );
        tx.setHeader( additionalHeader, masterId, authorId, timeStarted, latestCommittedTxWhenStarted,
                timeCommitted, -1 );
        return tx;
    }

    private Collection<StorageCommand> singleCreateNodeCommand( long id )
    {
        Collection<StorageCommand> commands = new ArrayList<>();
        NodeRecord before = new NodeRecord( id );
        NodeRecord after = new NodeRecord( id );
        after.setInUse( true );
        commands.add( new NodeCommand( before, after ) );
        return commands;
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
