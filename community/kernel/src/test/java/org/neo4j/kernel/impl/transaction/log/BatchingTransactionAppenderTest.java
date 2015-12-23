/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.log;

import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.Flushable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.index.IndexDefineCommand;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageCommandReaderFactory;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.command.Command.NodeCommand;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.OnePhaseCommit;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogCheckPointEvent;
import org.neo4j.kernel.lifecycle.LifeRule;
import org.neo4j.test.CleanupRule;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyByte;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import static org.neo4j.kernel.impl.transaction.log.rotation.LogRotation.NO_ROTATION;
import static org.neo4j.kernel.impl.util.IdOrderingQueue.BYPASS;

public class BatchingTransactionAppenderTest
{
    @Rule
    public final LifeRule life = new LifeRule( true );

    private final InMemoryVersionableLogChannel channel = new InMemoryVersionableLogChannel();
    private final LogAppendEvent logAppendEvent = LogAppendEvent.NULL;
    private final DatabaseHealth databaseHealth = mock( DatabaseHealth.class );
    private final LogFile logFile = mock( LogFile.class );
    private final TransactionIdStore transactionIdStore = mock( TransactionIdStore.class );
    private final TransactionMetadataCache positionCache = new TransactionMetadataCache( 10, 10 );

    @Test
    public void shouldAppendSingleTransaction() throws Exception
    {
        // GIVEN
        when( logFile.getWriter() ).thenReturn( channel );
        long txId = 15;
        when( transactionIdStore.nextCommittingTransactionId() ).thenReturn( txId );
        TransactionAppender appender = life.add( new BatchingTransactionAppender( logFile, NO_ROTATION, positionCache,
                transactionIdStore, BYPASS, databaseHealth ) );

        // WHEN
        TransactionRepresentation transaction = transaction( singleCreateNodeCommand( 0 ),
                new byte[]{1, 2, 5}, 2, 1, 12345, 4545, 12345 + 10 );

        appender.append( new TransactionToApply( transaction ), logAppendEvent );

        // THEN
        final LogEntryReader<ReadableVersionableLogChannel> logEntryReader = new VersionAwareLogEntryReader<>(
                new RecordStorageCommandReaderFactory() );
        try ( PhysicalTransactionCursor<ReadableVersionableLogChannel> reader =
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
        TransactionAppender appender = life.add( new BatchingTransactionAppender( logFile, NO_ROTATION, positionCache,
                transactionIdStore, BYPASS, databaseHealth ) );
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
        TransactionAppender appender = life.add( new BatchingTransactionAppender( logFile, NO_ROTATION, positionCache,
                transactionIdStore, BYPASS, databaseHealth ) );

        // WHEN
        final byte[] additionalHeader = new byte[]{1, 2, 5};
        final int masterId = 2, authorId = 1;
        final long timeStarted = 12345, latestCommittedTxWhenStarted = nextTxId - 5, timeCommitted = timeStarted + 10;
        PhysicalTransactionRepresentation transactionRepresentation = new PhysicalTransactionRepresentation(
                singleCreateNodeCommand( 0 ) );
        transactionRepresentation.setHeader( additionalHeader, masterId, authorId, timeStarted,
                latestCommittedTxWhenStarted, timeCommitted, -1 );

        LogEntryStart start = new LogEntryStart( 0, 0, 0l, latestCommittedTxWhenStarted, null,
                LogPosition.UNSPECIFIED );
        LogEntryCommit commit = new OnePhaseCommit( nextTxId, 0l );
        CommittedTransactionRepresentation transaction =
                new CommittedTransactionRepresentation( start, transactionRepresentation, commit );

        appender.append( new TransactionToApply( transactionRepresentation, transaction.getCommitEntry().getTxId() ),
                logAppendEvent );

        // THEN
        LogEntryReader<ReadableVersionableLogChannel> logEntryReader = new VersionAwareLogEntryReader<>(
                new RecordStorageCommandReaderFactory() );
        try ( PhysicalTransactionCursor<ReadableVersionableLogChannel> reader =
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
    public void shouldNotAppendCommittedTransactionsWhenTooFarAhead() throws Exception
    {
        // GIVEN
        InMemoryLogChannel channel = new InMemoryLogChannel();
        when( logFile.getWriter() ).thenReturn( channel );
        TransactionAppender appender = life.add( new BatchingTransactionAppender( logFile, NO_ROTATION, positionCache,
                transactionIdStore, BYPASS, databaseHealth ) );

        // WHEN
        final byte[] additionalHeader = new byte[]{1, 2, 5};
        final int masterId = 2, authorId = 1;
        final long timeStarted = 12345, latestCommittedTxWhenStarted = 4545, timeCommitted = timeStarted + 10;
        PhysicalTransactionRepresentation transactionRepresentation = new PhysicalTransactionRepresentation(
                singleCreateNodeCommand( 0 ) );
        transactionRepresentation.setHeader( additionalHeader, masterId, authorId, timeStarted,
                latestCommittedTxWhenStarted, timeCommitted, -1 );

        when( transactionIdStore.getLastCommittedTransactionId() ).thenReturn( latestCommittedTxWhenStarted );

        LogEntryStart start = new LogEntryStart( 0, 0, 0l, latestCommittedTxWhenStarted, null,
                LogPosition.UNSPECIFIED );
        LogEntryCommit commit = new OnePhaseCommit( latestCommittedTxWhenStarted + 2, 0l );
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
        WritableLogChannel channel = spy( new InMemoryLogChannel() );
        IOException failure = new IOException( failureMessage );
        when( channel.putInt( anyInt() ) ).thenThrow( failure );
        when( logFile.getWriter() ).thenReturn( channel );
        when( transactionIdStore.nextCommittingTransactionId() ).thenReturn( txId );
        Mockito.reset( databaseHealth );
        TransactionAppender appender = life.add( new BatchingTransactionAppender( logFile, NO_ROTATION, positionCache,
                transactionIdStore, BYPASS, databaseHealth ) );

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
            verify( transactionIdStore, times( 0 ) ).transactionClosed( eq( txId ), anyLong(), anyLong() );
            verify( databaseHealth ).panic( failure );
        }
    }

    @Test
    public void shouldNotCallTransactionClosedOnFailedForceLogToDisk() throws Exception
    {
        // GIVEN
        long txId = 3;
        String failureMessage = "Forces a failure";
        WritableLogChannel channel = spy( new InMemoryLogChannel() );
        IOException failure = new IOException( failureMessage );
        final Flushable flushable = mock( Flushable.class );
        doAnswer( new Answer<Flushable>()
        {
            @Override
            public Flushable answer( InvocationOnMock invocation ) throws Throwable
            {
                invocation.callRealMethod();
                return flushable;
            }
        } ).when( channel ).emptyBufferIntoChannelAndClearIt();
        doThrow( failure ).when( flushable ).flush();
        LogFile logFile = mock( LogFile.class );
        when( logFile.getWriter() ).thenReturn( channel );
        TransactionMetadataCache metadataCache = new TransactionMetadataCache( 10, 10 );
        TransactionIdStore transactionIdStore = mock( TransactionIdStore.class );
        when( transactionIdStore.nextCommittingTransactionId() ).thenReturn( txId );
        Mockito.reset( databaseHealth );
        TransactionAppender appender = life.add( new BatchingTransactionAppender( logFile, NO_ROTATION,
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
            verify( transactionIdStore, times( 0 ) ).transactionClosed( eq( txId ), anyLong(), anyLong() );
            verify( databaseHealth ).panic( failure );
        }
    }

    @Test
    public void shouldBeAbleToWriteACheckPoint() throws Throwable
    {
        // Given
        WritableLogChannel channel = mock( WritableLogChannel.class, RETURNS_MOCKS );
        Flushable flushable = mock( Flushable.class );
        when( channel.emptyBufferIntoChannelAndClearIt() ).thenReturn( flushable );
        when( channel.putLong( anyLong() ) ).thenReturn( channel );
        when( logFile.getWriter() ).thenReturn( channel );
        BatchingTransactionAppender appender = life.add( new BatchingTransactionAppender( logFile, NO_ROTATION,
                positionCache, transactionIdStore, BYPASS, databaseHealth ) );

        // When
        appender.checkPoint( new LogPosition( 1l, 2l ), LogCheckPointEvent.NULL );

        // Then
        verify( channel, times( 1 ) ).putLong( 1l );
        verify( channel, times( 1 ) ).putLong( 2l );
        verify( channel, times( 1 ) ).emptyBufferIntoChannelAndClearIt();
        verify( flushable, times( 1 ) ).flush();
        verifyZeroInteractions( databaseHealth );
    }

    @Test
    public void shouldKernelPanicIfNotAbleToWriteACheckPoint() throws Throwable
    {
        // Given
        IOException ioex = new IOException( "boom!" );
        WritableLogChannel channel = mock( WritableLogChannel.class, RETURNS_MOCKS );
        when (channel.put( anyByte() ) ).thenReturn( channel );
        when( channel.putLong( anyLong() ) ).thenThrow( ioex );
        when( channel.put( anyByte() ) ).thenThrow( ioex );
        when( logFile.getWriter() ).thenReturn( channel );
        BatchingTransactionAppender appender = life.add( new BatchingTransactionAppender(
                logFile, NO_ROTATION, positionCache, transactionIdStore, BYPASS, databaseHealth ) );

        // When
        try
        {
            appender.checkPoint( new LogPosition( 0l, 0l ), LogCheckPointEvent.NULL );
            fail( "should have thrown " );
        }
        catch ( IOException ex )
        {
            assertEquals( ioex, ex );
        }

        // Then
        verify( databaseHealth, times( 1 ) ).panic( ioex );
    }

    private TransactionRepresentation transactionWithLegacyIndexCommand()
    {
        Collection<Command> commands = new ArrayList<>();
        IndexDefineCommand command = new IndexDefineCommand();
        command.init( new HashMap<String,Integer>(), new HashMap<String,Integer>() );
        commands.add( command );
        PhysicalTransactionRepresentation transaction = new PhysicalTransactionRepresentation( commands );
        transaction.setHeader( new byte[0], 0, 0, 0, 0, 0, 0 );
        return transaction;
    }

    private TransactionToApply tryComplete( Future<?> future, int millis )
    {
        try
        {
            // Let's wait a full second here since in the green case it will return super quickly
            return (TransactionToApply) future.get( millis, MILLISECONDS );
        }
        catch ( InterruptedException | ExecutionException e )
        {
            throw new RuntimeException( "A committer that was expected to be done wasn't", e );
        }
        catch ( TimeoutException e )
        {
            return null;
        }
    }

    private boolean anyBoolean( boolean[] array, boolean lookFor )
    {
        for ( boolean item : array )
        {
            if ( item == lookFor )
            {
                return true;
            }
        }
        return false;
    }

    public final @Rule CleanupRule cleanup = new CleanupRule();

    private TransactionRepresentation transaction( Collection<Command> commands, byte[] additionalHeader,
            int masterId, int authorId, long timeStarted, long latestCommittedTxWhenStarted, long timeCommitted )
    {
        PhysicalTransactionRepresentation tx = new PhysicalTransactionRepresentation( commands );
        tx.setHeader( additionalHeader, masterId, authorId, timeStarted, latestCommittedTxWhenStarted,
                timeCommitted, -1 );
        return tx;
    }

    /**
     * @param transactions a {@code true} "transaction" means it should issue legacy index changes.
     */
    @SuppressWarnings( "rawtypes" )
    private Future[] committersStartYourEngines( final TransactionAppender appender, boolean... transactions )
    {
        ExecutorService executor = cleanup.add( Executors.newCachedThreadPool() );
        Future[] futures = new Future[transactions.length];
        for ( int i = 0; i < transactions.length; i++ )
        {
            final TransactionRepresentation transaction = createTransaction( transactions[i], i );
            futures[i] = executor.submit( new Callable<TransactionToApply>()
            {
                @Override
                public TransactionToApply call() throws IOException
                {
                    TransactionToApply tx = new TransactionToApply( transaction );
                    appender.append( tx, logAppendEvent );
                    return tx;
                }
            } );
        }
        return futures;
    }

    private TransactionRepresentation createTransaction( boolean includeLegacyIndexCommands, int i )
    {
        Collection<Command> commands = new ArrayList<>();
        if ( includeLegacyIndexCommands )
        {
            IndexDefineCommand defineCommand = new IndexDefineCommand();
            defineCommand.init(
                    MapUtil.<String,Integer>genericMap( "one", 1 ),
                    MapUtil.<String,Integer>genericMap( "two", 2 ) );
            commands.add( defineCommand );
        }
        else
        {
            NodeRecord record = new NodeRecord( 1 + i );
            record.setInUse( true );
            commands.add( new NodeCommand( new NodeRecord( record.getId() ), record ) );
        }
        PhysicalTransactionRepresentation transaction = new PhysicalTransactionRepresentation( commands );
        transaction.setHeader( new byte[0], 0, 0, 0, 0, 0, -1 );
        return transaction;
    }

    private Collection<Command> singleCreateNodeCommand( long id )
    {
        Collection<Command> commands = new ArrayList<>();
        NodeRecord before = new NodeRecord( id );
        NodeRecord after = new NodeRecord( id );
        after.setInUse( true );
        commands.add( new NodeCommand( before, after ) );
        return commands;
    }

    private TransactionToApply batchOf( TransactionRepresentation... transactions )
    {
        TransactionToApply first = null, last = null;
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
