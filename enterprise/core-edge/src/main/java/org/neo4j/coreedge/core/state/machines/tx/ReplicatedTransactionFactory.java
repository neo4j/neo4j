/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.core.state.machines.tx;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import org.neo4j.coreedge.messaging.NetworkFlushableChannelNetty4;
import org.neo4j.coreedge.messaging.NetworkReadableClosableChannelNetty4;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageCommandReaderFactory;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommand;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.storageengine.api.StorageCommand;

public class ReplicatedTransactionFactory
{
    public static ReplicatedTransaction createImmutableReplicatedTransaction( TransactionRepresentation tx  )
    {
        ByteBuf transactionBuffer = Unpooled.buffer();

        NetworkFlushableChannelNetty4 channel = new NetworkFlushableChannelNetty4( transactionBuffer );
        try
        {
            TransactionSerializer.write( tx, channel );
        }
        catch ( IOException e )
        {
            // TODO: This should not happen. Not even the IOException, fix it.
            throw new RuntimeException( e );
        }

        /*
         * This trims down the array to send up to the actual index it was written. While sending additional zeroes
         * is safe, since LogEntryReader stops reading once it sees a zero entry, it is wasteful.
         */
        byte[] txBytes = Arrays.copyOf( transactionBuffer.array(), transactionBuffer.writerIndex() );
        transactionBuffer.release();

        return new ReplicatedTransaction( txBytes );
    }

    public static TransactionRepresentation extractTransactionRepresentation( ReplicatedTransaction transactionCommand, byte[] extraHeader )
    {
        ByteBuf txBuffer = Unpooled.wrappedBuffer( transactionCommand.getTxBytes() );
        NetworkReadableClosableChannelNetty4 channel = new NetworkReadableClosableChannelNetty4( txBuffer );

        try
        {
            return read( channel, extraHeader );
        }
        catch ( IOException e )
        {
            // TODO: This should not happen. Not even the IOException, fix it.
            throw new RuntimeException( e );
        }
    }

    private static class TransactionSerializer
    {
        public static void write( TransactionRepresentation tx, NetworkFlushableChannelNetty4 channel ) throws
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

            new LogEntryWriter( channel ).serialize( tx );
        }
    }

    public static TransactionRepresentation read( NetworkReadableClosableChannelNetty4 channel, byte[] extraHeader ) throws IOException
    {
        LogEntryReader<ReadableClosablePositionAwareChannel> reader = new VersionAwareLogEntryReader<>(
                new RecordStorageCommandReaderFactory() );

        int authorId = channel.getInt();
        int masterId = channel.getInt();
        long latestCommittedTxWhenStarted = channel.getLong();
        long timeStarted = channel.getLong();
        long timeCommitted = channel.getLong();
        int lockSessionId = channel.getInt();

        int headerLength = channel.getInt();
        byte[] header;
        if ( headerLength == 0 )
        {
            header = extraHeader;
        }
        else
        {
            header = new byte[headerLength];
        }

        channel.get( header, headerLength );

        LogEntryCommand entryRead;
        List<StorageCommand> commands = new LinkedList<>();

        while ( (entryRead = (LogEntryCommand) reader.readLogEntry( channel )) != null )
        {
            commands.add( entryRead.getXaCommand() );
        }

        PhysicalTransactionRepresentation tx = new PhysicalTransactionRepresentation( commands );
        tx.setHeader( header, masterId, authorId, timeStarted, latestCommittedTxWhenStarted, timeCommitted, lockSessionId );

        return tx;
    }
}
