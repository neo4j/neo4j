/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.coreedge.raft.replication.tx;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.neo4j.coreedge.raft.net.NetworkReadableLogChannelNetty4;
import org.neo4j.coreedge.raft.net.NetworkWritableLogChannelNetty4;
import org.neo4j.coreedge.raft.replication.session.GlobalSession;
import org.neo4j.coreedge.raft.replication.session.LocalOperationId;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.CommandWriter;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommand;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;

public class ReplicatedTransactionFactory
{
    public static ReplicatedTransaction createImmutableReplicatedTransaction(
            TransactionRepresentation tx, GlobalSession globalSession, LocalOperationId localOperationId ) throws IOException
    {
        ByteBuf transactionBuffer = Unpooled.buffer();

        NetworkWritableLogChannelNetty4 channel = new NetworkWritableLogChannelNetty4( transactionBuffer );
        ReplicatedTransactionFactory.TransactionSerializer.write( tx, channel );

        byte[] txBytes = transactionBuffer.array().clone();
        transactionBuffer.release();

        return new ReplicatedTransaction( txBytes, globalSession, localOperationId );
    }

    public static TransactionRepresentation extractTransactionRepresentation( ReplicatedTransaction replicatedTransaction ) throws IOException
    {
        ByteBuf txBuffer = Unpooled.wrappedBuffer( replicatedTransaction.getTxBytes() );
        NetworkReadableLogChannelNetty4 channel = new NetworkReadableLogChannelNetty4( txBuffer );

        return TransactionDeserializer.read( channel );
    }

    public static class TransactionSerializer
    {
        public static void write( TransactionRepresentation tx, NetworkWritableLogChannelNetty4 channel ) throws
                IOException
        {
            channel.putInt( tx.getAuthorId() );
            channel.putInt( tx.getMasterId() );
            channel.putLong( tx.getLatestCommittedTxWhenStarted() );
            channel.putLong( tx.getTimeStarted() );
            channel.putLong( tx.getTimeCommitted() );
            channel.putInt( tx.getLockSessionId() );

            byte[] additionalHeader = tx.additionalHeader();
            if ( additionalHeader != null )
            {
                channel.putInt( additionalHeader.length );
                channel.put( additionalHeader, additionalHeader.length );
            }
            else
            {
                channel.putInt( 0 );
            }

            new LogEntryWriter( channel, new CommandWriter( channel ) ).serialize( tx );
        }
    }

    public static class TransactionDeserializer
    {
        public static TransactionRepresentation read( NetworkReadableLogChannelNetty4 channel ) throws IOException
        {
            LogEntryReader<ReadableLogChannel> reader = new VersionAwareLogEntryReader<>();

            int authorId = channel.getInt();
            int masterId = channel.getInt();
            long latestCommittedTxWhenStarted = channel.getLong();
            long timeStarted = channel.getLong();
            long timeCommitted = channel.getLong();
            int lockSessionId = channel.getInt();

            int headerLength = channel.getInt();
            byte[] header = new byte[headerLength];

            channel.get( header, headerLength );

            if ( headerLength == 0 )
            {
                header = null;
            }

            LogEntryCommand entryRead;
            List<Command> commands = new LinkedList<>();

            while ( (entryRead = (LogEntryCommand) reader.readLogEntry( channel )) != null )
            {
                commands.add( entryRead.getXaCommand() );
            }

            PhysicalTransactionRepresentation tx = new PhysicalTransactionRepresentation( commands );
            tx.setHeader( header, masterId, authorId, timeStarted, latestCommittedTxWhenStarted, timeCommitted, lockSessionId );

            return tx;
        }
    }

}
