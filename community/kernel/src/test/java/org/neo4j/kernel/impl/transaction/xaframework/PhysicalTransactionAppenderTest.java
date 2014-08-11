/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.xaframework;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.junit.Test;

import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.TransactionIdStore;
import org.neo4j.kernel.impl.nioneo.xa.command.Command;
import org.neo4j.kernel.impl.transaction.xaframework.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.xaframework.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.xaframework.log.entry.OnePhaseCommit;
import org.neo4j.kernel.impl.transaction.xaframework.log.entry.VersionAwareLogEntryReader;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PhysicalTransactionAppenderTest
{
    @Test
    public void shouldAppendTransactions() throws Exception
    {
        // GIVEN
        LogFile logFile = mock( LogFile.class );
        InMemoryLogChannel channel = new InMemoryLogChannel();
        when( logFile.getWriter() ).thenReturn( channel );
        TxIdGenerator txIdGenerator = mock( TxIdGenerator.class );
        TransactionMetadataCache positionCache = new TransactionMetadataCache( 10, 100 );
        TransactionIdStore transactionIdStore = mock( TransactionIdStore.class );
        TransactionAppender appender = new PhysicalTransactionAppender(
                logFile, txIdGenerator, positionCache, transactionIdStore );

        // WHEN
        PhysicalTransactionRepresentation transaction = new PhysicalTransactionRepresentation(
                singleCreateNodeCommand() );
        final byte[] additionalHeader = new byte[] {1, 2, 5};
        final int masterId = 2, authorId = 1;
        final long timeStarted = 12345, latestCommittedTxWhenStarted = 4545, timeCommitted = timeStarted+10;
        transaction.setHeader( additionalHeader, masterId, authorId, timeStarted, latestCommittedTxWhenStarted,
                timeCommitted );

        appender.append( transaction );

        // THEN
        try(PhysicalTransactionCursor reader = new PhysicalTransactionCursor( channel, new VersionAwareLogEntryReader()))
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
        LogFile logFile = mock( LogFile.class );
        InMemoryLogChannel channel = new InMemoryLogChannel();
        when( logFile.getWriter() ).thenReturn( channel );
        TxIdGenerator txIdGenerator = mock( TxIdGenerator.class );
        TransactionMetadataCache positionCache = new TransactionMetadataCache( 10, 100 );
        TransactionIdStore transactionIdStore = mock( TransactionIdStore.class );
        TransactionAppender appender = new PhysicalTransactionAppender(
                logFile, txIdGenerator, positionCache, transactionIdStore );


        // WHEN
        final byte[] additionalHeader = new byte[]{1, 2, 5};
        final int masterId = 2, authorId = 1;
        final long timeStarted = 12345, latestCommittedTxWhenStarted = 4545, timeCommitted = timeStarted+10;
        PhysicalTransactionRepresentation transactionRepresentation = new PhysicalTransactionRepresentation(
                singleCreateNodeCommand() );
        transactionRepresentation.setHeader( additionalHeader, masterId, authorId, timeStarted,
                latestCommittedTxWhenStarted, timeCommitted );

        when( transactionIdStore.getLastCommittingTransactionId() ).thenReturn( latestCommittedTxWhenStarted );

        LogEntryStart start = new LogEntryStart( 0, 0, 0l, latestCommittedTxWhenStarted, null,
                LogPosition.UNSPECIFIED );
        LogEntryCommit commit = new OnePhaseCommit( latestCommittedTxWhenStarted + 1, 0l );
        CommittedTransactionRepresentation transaction =
                new CommittedTransactionRepresentation( start, transactionRepresentation, commit );

        appender.append( transaction );

        // THEN
        PhysicalTransactionCursor reader = new PhysicalTransactionCursor( channel, new VersionAwareLogEntryReader() );
        reader.next();
        TransactionRepresentation result = reader.get().getTransactionRepresentation();
        assertArrayEquals( additionalHeader, result.additionalHeader() );
        assertEquals( masterId, result.getMasterId() );
        assertEquals( authorId, result.getAuthorId() );
        assertEquals( timeStarted, result.getTimeStarted() );
        assertEquals( timeCommitted, result.getTimeCommitted() );
        assertEquals( latestCommittedTxWhenStarted, result.getLatestCommittedTxWhenStarted() );
    }

    @Test
    public void shouldNotAppendCommittedTransactionsWhenTooFarAhead() throws Exception
    {
        // GIVEN
        LogFile logFile = mock( LogFile.class );
        InMemoryLogChannel channel = new InMemoryLogChannel();
        when( logFile.getWriter() ).thenReturn( channel );
        TxIdGenerator txIdGenerator = mock( TxIdGenerator.class );
        TransactionMetadataCache positionCache = new TransactionMetadataCache( 10, 100 );
        TransactionIdStore transactionIdStore = mock( TransactionIdStore.class );
        TransactionAppender appender = new PhysicalTransactionAppender(
                logFile, txIdGenerator, positionCache, transactionIdStore );

        // WHEN
        final byte[] additionalHeader = new byte[]{1, 2, 5};
        final int masterId = 2, authorId = 1;
        final long timeStarted = 12345, latestCommittedTxWhenStarted = 4545, timeCommitted = timeStarted+10;
        PhysicalTransactionRepresentation transactionRepresentation = new PhysicalTransactionRepresentation(
                singleCreateNodeCommand() );
        transactionRepresentation.setHeader( additionalHeader, masterId, authorId, timeStarted,
                latestCommittedTxWhenStarted, timeCommitted );

        when( transactionIdStore.getLastCommittingTransactionId() ).thenReturn( latestCommittedTxWhenStarted );

        LogEntryStart start = new LogEntryStart( 0, 0, 0l, latestCommittedTxWhenStarted, null,
                LogPosition.UNSPECIFIED );
        LogEntryCommit commit = new OnePhaseCommit( latestCommittedTxWhenStarted + 2, 0l );
        CommittedTransactionRepresentation transaction =
                new CommittedTransactionRepresentation( start, transactionRepresentation, commit );

        try
        {
            appender.append( transaction );
            fail( "should have thrown" );
        }
        catch ( IOException e )
        {
            assertEquals( "Tried to apply transaction with txId=" + (latestCommittedTxWhenStarted + 2) +
                    " but last committed txId=" + latestCommittedTxWhenStarted, e.getMessage() );
        }

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
