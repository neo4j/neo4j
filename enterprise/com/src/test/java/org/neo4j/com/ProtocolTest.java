/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.com;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;

import org.jboss.netty.buffer.ChannelBuffer;
import org.junit.Test;

import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.command.Command.NodeCommand;
import org.neo4j.kernel.impl.transaction.log.InMemoryLogChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class ProtocolTest
{
    @Test
    public void shouldSerializeAndDeserializeTransactionRepresentation() throws Exception
    {
        // GIVEN
        PhysicalTransactionRepresentation transaction = new PhysicalTransactionRepresentation( justOneNode() );
        byte[] additionalHeader = "extra".getBytes();
        int masterId = 1, authorId = 2;
        long timeStarted = 12345, lastTxWhenStarted = 12, timeCommitted = timeStarted+10;
        transaction.setHeader( additionalHeader, masterId, authorId, timeStarted, lastTxWhenStarted, timeCommitted, -1 );
        Protocol.TransactionSerializer serializer = new Protocol.TransactionSerializer( transaction );
        ChannelBuffer buffer = new ChannelBufferWrapper( new InMemoryLogChannel() );

        // WHEN serializing the transaction
        serializer.write( buffer );

        // THEN deserializing the same transaction should yield the same data.
        // ... remember that this deserializer doesn't read the data source name string. Read it manually here
        assertEquals( NeoStoreDataSource.DEFAULT_DATA_SOURCE_NAME, Protocol.readString( buffer ) );
        TransactionRepresentation readTransaction = Protocol.TRANSACTION_REPRESENTATION_DESERIALIZER.read(
                buffer, ByteBuffer.allocate( 1000 ) );
        assertArrayEquals( additionalHeader, readTransaction.additionalHeader() );
        assertEquals( masterId, readTransaction.getMasterId() );
        assertEquals( authorId, readTransaction.getAuthorId() );
        assertEquals( timeStarted, readTransaction.getTimeStarted() );
        assertEquals( lastTxWhenStarted, readTransaction.getLatestCommittedTxWhenStarted() );
        assertEquals( timeCommitted, readTransaction.getTimeCommitted() );
    }

    private Collection<Command> justOneNode()
    {
        NodeCommand nodeCommand = new NodeCommand();
        NodeRecord node = new NodeRecord( 0 );
        node.setInUse( true );
        nodeCommand.init( new NodeRecord( node.getId() ), node );
        return Arrays.<Command>asList( nodeCommand );
    }
}
