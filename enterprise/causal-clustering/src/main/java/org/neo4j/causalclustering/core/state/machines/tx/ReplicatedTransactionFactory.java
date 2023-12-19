/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.core.state.machines.tx;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.neo4j.causalclustering.messaging.MessageTooBigException;
import org.neo4j.causalclustering.messaging.NetworkFlushableChannelNetty4;
import org.neo4j.causalclustering.messaging.NetworkReadableClosableChannelNetty4;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageCommandReaderFactory;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.entry.InvalidLogEntryHandler;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommand;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.storageengine.api.StorageCommand;

import static org.neo4j.io.ByteUnit.gibiBytes;

public class ReplicatedTransactionFactory
{
    private static final long MAX_SERIALIZED_TX_SIZE = gibiBytes( 1 );

    private ReplicatedTransactionFactory()
    {
        throw new AssertionError( "Should not be instantiated" );
    }

    public static ReplicatedTransaction createImmutableReplicatedTransaction( TransactionRepresentation tx  )
    {
        ByteBuf transactionBuffer = Unpooled.buffer();

        NetworkFlushableChannelNetty4 channel = new NetworkFlushableChannelNetty4( transactionBuffer, MAX_SERIALIZED_TX_SIZE );
        try
        {
            TransactionSerializer.write( tx, channel );
        }
        catch ( MessageTooBigException e )
        {
            throw new IllegalStateException( "Transaction size was too large to replicate across the cluster.", e );
        }
        catch ( IOException e )
        {
            // TODO: This should not happen. All operations are in memory, no IOException should be thrown
            // Easier said than done though, we use the LogEntry handling routines which throw IOException
            throw new RuntimeException( e );
        }

        /*
         * This trims down the array to send up to the actual index it was written. Not doing this would send additional
         * zeroes which not only wasteful, but also not handled by the LogEntryReader receiving this.
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
            // TODO: This should not happen. All operations are in memory, no IOException should be thrown
            // Easier said than done though, we use the LogEntry handling routines which throw IOException
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
                new RecordStorageCommandReaderFactory(), InvalidLogEntryHandler.STRICT );

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
            commands.add( entryRead.getCommand() );
        }

        PhysicalTransactionRepresentation tx = new PhysicalTransactionRepresentation( commands );
        tx.setHeader( header, masterId, authorId, timeStarted, latestCommittedTxWhenStarted, timeCommitted, lockSessionId );

        return tx;
    }
}
