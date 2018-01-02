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
import org.neo4j.kernel.KernelHealth;
import org.neo4j.kernel.impl.index.IndexDefineCommand;
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
import org.neo4j.kernel.impl.util.IdOrderingQueue;
import org.neo4j.kernel.impl.util.SynchronizedArrayIdOrderingQueue;
import org.neo4j.kernel.lifecycle.LifeRule;
import org.neo4j.test.CleanupRule;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
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
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.Exceptions.contains;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_COMMIT_TIMESTAMP;
import static org.neo4j.kernel.impl.transaction.log.rotation.LogRotation.NO_ROTATION;
import static org.neo4j.kernel.impl.util.IdOrderingQueue.BYPASS;

public class BatchingTransactionAppenderTest
{
    @Rule
    public final LifeRule life = new LifeRule();

    private final InMemoryVersionableLogChannel channel = new InMemoryVersionableLogChannel();
    private final LogAppendEvent logAppendEvent = LogAppendEvent.NULL;
    private final KernelHealth kernelHealth = mock( KernelHealth.class );
    private final LogFile logFile = mock( LogFile.class );
    private final TransactionIdStore transactionIdStore = mock( TransactionIdStore.class );
    private final TransactionMetadataCache positionCache = new TransactionMetadataCache( 10, 10 );

    @Test
    public void shouldAppendTransactions() throws Exception
    {
        // GIVEN
        when( logFile.getWriter() ).thenReturn( channel );
        long txId = 15;
        when( transactionIdStore.nextCommittingTransactionId() ).thenReturn( txId );
        TransactionAppender appender = life.add( new BatchingTransactionAppender( logFile, NO_ROTATION, positionCache,
                transactionIdStore, BYPASS, kernelHealth ) );

        life.start();

        // WHEN
        PhysicalTransactionRepresentation transaction = new PhysicalTransactionRepresentation(
                singleCreateNodeCommand() );
        final byte[] additionalHeader = new byte[]{1, 2, 5};
        final int masterId = 2, authorId = 1;
        final long timeStarted = 12345, latestCommittedTxWhenStarted = 4545, timeCommitted = timeStarted + 10;
        transaction.setHeader( additionalHeader, masterId, authorId, timeStarted, latestCommittedTxWhenStarted,
                timeCommitted, -1 );

        appender.append( transaction, logAppendEvent );

        // THEN
        final LogEntryReader<ReadableVersionableLogChannel> logEntryReader = new VersionAwareLogEntryReader<>();
        try ( PhysicalTransactionCursor<ReadableVersionableLogChannel> reader =
                      new PhysicalTransactionCursor<>( channel, logEntryReader ) )
        {
            reader.next();
            TransactionRepresentation tx = reader.get().getTransactionRepresentation();
            assertArrayEquals( additionalHeader, tx.additionalHeader() );
            assertEquals( masterId, tx.getMasterId() );
            assertEquals( authorId, tx.getAuthorId() );
            assertEquals( timeStarted, tx.getTimeStarted() );
            assertEquals( timeCommitted, tx.getTimeCommitted() );
            assertEquals( latestCommittedTxWhenStarted, tx.getLatestCommittedTxWhenStarted() );
        }
    }

    @Test
    public void shouldAppendCommittedTransactions() throws Exception
    {
        // GIVEN
        when( logFile.getWriter() ).thenReturn( channel );
        long nextTxId = 15;
        when( transactionIdStore.nextCommittingTransactionId() ).thenReturn( nextTxId );
        TransactionAppender appender = life.add( new BatchingTransactionAppender( logFile, NO_ROTATION, positionCache,
                transactionIdStore, BYPASS, kernelHealth ) );
        life.start();

        // WHEN
        final byte[] additionalHeader = new byte[]{1, 2, 5};
        final int masterId = 2, authorId = 1;
        final long timeStarted = 12345, latestCommittedTxWhenStarted = nextTxId - 5, timeCommitted = timeStarted + 10;
        PhysicalTransactionRepresentation transactionRepresentation = new PhysicalTransactionRepresentation(
                singleCreateNodeCommand() );
        transactionRepresentation.setHeader( additionalHeader, masterId, authorId, timeStarted,
                latestCommittedTxWhenStarted, timeCommitted, -1 );

        LogEntryStart start = new LogEntryStart( 0, 0, 0l, latestCommittedTxWhenStarted, null,
                LogPosition.UNSPECIFIED );
        LogEntryCommit commit = new OnePhaseCommit( nextTxId, 0l );
        CommittedTransactionRepresentation transaction =
                new CommittedTransactionRepresentation( start, transactionRepresentation, commit );

        appender.append( transaction.getTransactionRepresentation(), transaction.getCommitEntry().getTxId() );

        // THEN
        LogEntryReader<ReadableVersionableLogChannel> logEntryReader = new VersionAwareLogEntryReader<>();
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
                transactionIdStore, BYPASS, kernelHealth ) );

        life.start();

        // WHEN
        final byte[] additionalHeader = new byte[]{1, 2, 5};
        final int masterId = 2, authorId = 1;
        final long timeStarted = 12345, latestCommittedTxWhenStarted = 4545, timeCommitted = timeStarted + 10;
        PhysicalTransactionRepresentation transactionRepresentation = new PhysicalTransactionRepresentation(
                singleCreateNodeCommand() );
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
            appender.append( transaction.getTransactionRepresentation(), transaction.getCommitEntry().getTxId() );
            fail( "should have thrown" );
        }
        catch ( Throwable e )
        {
            assertThat( e.getMessage(), containsString( "to be applied, but appending it ended up generating an" ) );
        }
    }

    @Test
    public void shouldNotCallTransactionCommittedOnFailedAppendedTransaction() throws Exception
    {
        // GIVEN
        long txId = 3;
        String failureMessage = "Forces a failure";
        WritableLogChannel channel = spy( new InMemoryLogChannel() );
        IOException failure = new IOException( failureMessage );
        when( channel.putInt( anyInt() ) ).thenThrow( failure );
        when( logFile.getWriter() ).thenReturn( channel );
        when( transactionIdStore.nextCommittingTransactionId() ).thenReturn( txId );
        Mockito.reset( kernelHealth );
        TransactionAppender appender = life.add( new BatchingTransactionAppender( logFile, NO_ROTATION, positionCache,
                transactionIdStore, BYPASS, kernelHealth ) );

        life.start();

        // WHEN
        TransactionRepresentation transaction = mock( TransactionRepresentation.class );
        when( transaction.additionalHeader() ).thenReturn( new byte[0] );
        try
        {
            appender.append( transaction, logAppendEvent );
            fail( "Expected append to fail. Something is wrong with the test itself" );
        }
        catch ( IOException e )
        {
            // THEN
            assertSame( failure, e );
            verify( transactionIdStore, times( 1 ) ).nextCommittingTransactionId();
            verify( transactionIdStore, times( 1 ) ).transactionClosed( eq( txId ), anyLong(), anyLong() );
            verify( kernelHealth ).panic( failure );
        }
    }

    @Test
    public void shouldNotCallTransactionCommittedOnFailedForceLogToDisk() throws Exception
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
        Mockito.reset( kernelHealth );
        TransactionAppender appender = life.add( new BatchingTransactionAppender( logFile, NO_ROTATION,
                metadataCache, transactionIdStore, BYPASS, kernelHealth ) );

        life.start();

        // WHEN
        TransactionRepresentation transaction = mock( TransactionRepresentation.class );
        when( transaction.additionalHeader() ).thenReturn( new byte[0] );
        try
        {
            appender.append( transaction, logAppendEvent );
            fail( "Expected append to fail. Something is wrong with the test itself" );
        }
        catch ( IOException e )
        {
            // THEN
            assertSame( failure, e );
            verify( transactionIdStore, times( 1 ) ).nextCommittingTransactionId();
            verify( transactionIdStore, times( 1 ) ).transactionClosed( eq( txId ), anyLong(), anyLong() );
            verify( kernelHealth ).panic( failure );
        }
    }

    @SuppressWarnings( "rawtypes" )
    @Test
    public void shouldOrderTransactionsMakingLegacyIndexChanges() throws Exception
    {
        // GIVEN
        WritableLogChannel channel = new InMemoryLogChannel();
        when( logFile.getWriter() ).thenReturn( channel );
        when( transactionIdStore.nextCommittingTransactionId() ).thenReturn( 1L, 2L, 3L, 4L, 5L );
        IdOrderingQueue legacyIndexOrdering = new SynchronizedArrayIdOrderingQueue( 5 );
        TransactionAppender appender = life.add( new BatchingTransactionAppender( logFile, NO_ROTATION, positionCache,
                transactionIdStore, legacyIndexOrdering, kernelHealth ) );

        life.start();

        // WHEN appending 5 simultaneous transaction, of which 3 has legacy index changes [1*,2,3*,4,5*]
        // LEGEND: * = has legacy index changes
        boolean[] transactions = {true, false, true, false, true};
        Future[] committers = committersStartYourEngines( appender, transactions );

        // THEN the ones w/o legacy index changes should just have fallen right through
        // and the ones w/ such changes should be ordered and wait for each other

        // ... so make sure to let the non-legacy-index transactions through, just because we can
        boolean[] completed = new boolean[transactions.length];
        for ( int i = 0; i < transactions.length; i++ )
        {
            if ( !transactions[i] )
            {   // Here's a non-legacy-index transaction
                assertNotNull( tryComplete( committers[i], 1000 ) );
                completed[i] = true;
            }
        }

        // ... and wait for the legacy index transactions to be completed in order
        while ( anyBoolean( completed, false ) )
        {
            // Look for incomplete transactions (i.e. the legacy index transactions), and among
            // those there should be one that is completed, whereas the other should not be.
            Long doneTx = null;
            for ( int attempt = 0; attempt < 5 && doneTx == null; attempt++ )
            {
                for ( int i = 0; i < completed.length; i++ )
                {
                    if ( !completed[i] )
                    {
                        Commitment commitment = tryComplete( committers[i], 100 );
                        if ( commitment != null )
                        {
                            assertNull( "Multiple legacy index transactions seems to have " +
                                        "moved on from append at the same time", doneTx );
                            doneTx = commitment.transactionId();
                            completed[i] = true;
                        }
                    }
                }
            }
            assertNotNull( "None done this round", doneTx );
            legacyIndexOrdering.removeChecked( doneTx );
        }
    }

    @Test
    public void shouldCloseTransactionThatWasAppendedAndMarkedAsCommittedButFailedAfterThat() throws Exception
    {
        // GIVEN
        long txId = 3;
        String failureMessage = "Forces a failure";
        WritableLogChannel channel = new InMemoryLogChannel();
        when( logFile.getWriter() ).thenReturn( channel );
        when( transactionIdStore.nextCommittingTransactionId() ).thenReturn( txId );
        IdOrderingQueue idOrderingQueue = mock( IdOrderingQueue.class );
        doThrow( new RuntimeException( failureMessage ) ).when( idOrderingQueue ).waitFor( anyLong() );
        TransactionAppender appender = life.add( new BatchingTransactionAppender( logFile, NO_ROTATION, positionCache,
                transactionIdStore, idOrderingQueue, kernelHealth ) );

        life.start();

        // WHEN
        TransactionRepresentation transaction = transactionWithLegacyIndexCommand();
        try
        {
            appender.append( transaction, logAppendEvent );
            fail( "Expected append to fail. Something is wrong with the test itself" );
        }
        catch ( Exception e )
        {
            // THEN
            assertTrue( contains( e, failureMessage, RuntimeException.class ) );
            verify( transactionIdStore, times( 1 ) ).nextCommittingTransactionId();
            verify( transactionIdStore, times( 1 ) ).transactionCommitted( eq( txId ), anyLong(),
                    eq( BASE_TX_COMMIT_TIMESTAMP ) );
            verify( transactionIdStore, times( 1 ) ).transactionClosed( eq( txId ), anyLong(), anyLong() );
            verifyNoMoreInteractions( transactionIdStore );
        }
    }

    @Test
    public void shouldBeAbleToWriteACheckPoint() throws Throwable
    {
        // Given
        BatchingTransactionAppender appender = new BatchingTransactionAppender( logFile, NO_ROTATION, positionCache,
                transactionIdStore, BYPASS, kernelHealth );

        WritableLogChannel channel = mock( WritableLogChannel.class, RETURNS_MOCKS );
        Flushable flushable = mock( Flushable.class );
        when( channel.emptyBufferIntoChannelAndClearIt() ).thenReturn( flushable );
        when( channel.putLong( anyLong() ) ).thenReturn( channel );
        when( logFile.getWriter() ).thenReturn( channel );

        appender.start();

        // When
        appender.checkPoint( new LogPosition( 1l, 2l ), LogCheckPointEvent.NULL );

        // Then
        verify( channel, times( 1 ) ).putLong( 1l );
        verify( channel, times( 1 ) ).putLong( 2l );
        verify( channel, times( 1 ) ).emptyBufferIntoChannelAndClearIt();
        verify( flushable, times( 1 ) ).flush();
        verifyZeroInteractions( kernelHealth );
    }

    @Test
    public void shouldKernelPanicIfNotAbleToWriteACheckPoint() throws Throwable
    {
        // Given
        BatchingTransactionAppender appender = new BatchingTransactionAppender( logFile, NO_ROTATION, positionCache,
                transactionIdStore, BYPASS, kernelHealth );

        IOException ioex = new IOException( "boom!" );
        WritableLogChannel channel = mock( WritableLogChannel.class, RETURNS_MOCKS );
        when( channel.putLong( anyLong() ) ).thenThrow( ioex );
        when( logFile.getWriter() ).thenReturn( channel );

        appender.start();

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
        verify( kernelHealth, times( 1 ) ).panic( ioex );
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

    private Commitment tryComplete( Future<?> future, int millis )
    {
        try
        {
            // Let's wait a full second here since in the green case it will return super quickly
            return (Commitment) future.get( millis, MILLISECONDS );
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
            futures[i] = executor.submit( new Callable<Commitment>()
            {
                @Override
                public Commitment call() throws IOException
                {
                    return appender.append( transaction, logAppendEvent );
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
            NodeCommand nodeCommand = new NodeCommand();
            NodeRecord record = new NodeRecord( 1 + i );
            record.setInUse( true );
            nodeCommand.init( new NodeRecord( record.getId() ), record );
            commands.add( nodeCommand );
        }
        PhysicalTransactionRepresentation transaction = new PhysicalTransactionRepresentation( commands );
        transaction.setHeader( new byte[0], 0, 0, 0, 0, 0, -1 );
        return transaction;
    }

    private Collection<Command> singleCreateNodeCommand()
    {
        Collection<Command> commands = new ArrayList<>();
        Command.NodeCommand command = new Command.NodeCommand();

        long id = 0;
        NodeRecord before = new NodeRecord( id );
        NodeRecord after = new NodeRecord( id );
        after.setInUse( true );
        command.init( before, after );

        commands.add( command );
        return commands;
    }
}
