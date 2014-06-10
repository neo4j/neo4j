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
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.xa.CommandReaderFactory;
import org.neo4j.kernel.impl.nioneo.xa.command.Command;
import org.neo4j.kernel.impl.util.Consumer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class PhysicalTransactionAppenderTest
{
    @Test
    public void shouldAppendTransactions() throws Exception
    {
        // GIVEN
        InMemoryLogChannel channel = new InMemoryLogChannel();
        TxIdGenerator txIdGenerator = mock( TxIdGenerator.class );
        LogPositionCache positionCache = new LogPositionCache( 10, 100 );
        TransactionAppender appender = new PhysicalTransactionAppender( channel, txIdGenerator, positionCache );

        // WHEN
        PhysicalTransactionRepresentation transaction = new PhysicalTransactionRepresentation(
                singleCreateNodeCommand(), false );
        final byte[] additionalHeader = new byte[] {1, 2, 5};
        final int masterId = 2, authorId = 1;
        final long timeWritten = 12345, latestCommittedTxWhenStarted = 4545;
        transaction.setHeader( additionalHeader, masterId, authorId, timeWritten, latestCommittedTxWhenStarted );
        appender.append( transaction );

        // THEN
        TransactionCursor reader = new PhysicalTransactionCursor( channel, new VersionAwareLogEntryReader(
                CommandReaderFactory.DEFAULT ) );
        final AtomicInteger visited = new AtomicInteger();
        reader.next( new Consumer<TransactionRepresentation, IOException>()
        {
            @Override
            public boolean accept( TransactionRepresentation transaction ) throws IOException
            {
                assertArrayEquals( additionalHeader, transaction.additionalHeader() );
                assertEquals( masterId, transaction.getMasterId() );
                assertEquals( authorId, transaction.getAuthorId() );
                assertEquals( timeWritten, transaction.getTimeWritten() );
                assertEquals( latestCommittedTxWhenStarted, transaction.getLatestCommittedTxWhenStarted() );
                visited.incrementAndGet();
                return true;
            }
        } );
        assertEquals( 1, visited.get() );
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
