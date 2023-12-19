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
package org.neo4j.causalclustering.core.state.machines.token;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.neo4j.causalclustering.messaging.NetworkFlushableChannelNetty4;
import org.neo4j.causalclustering.messaging.NetworkReadableClosableChannelNetty4;
import org.neo4j.causalclustering.messaging.marshalling.StringMarshal;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageCommandReaderFactory;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.entry.InvalidLogEntryHandler;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommand;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.WritableChannel;

public class ReplicatedTokenRequestSerializer
{
    private ReplicatedTokenRequestSerializer()
    {
        throw new AssertionError( "Should not be instantiated" );
    }

    public static void marshal( ReplicatedTokenRequest content, WritableChannel channel ) throws IOException
    {
        channel.putInt( content.type().ordinal() );
        StringMarshal.marshal( channel, content.tokenName() );

        channel.putInt( content.commandBytes().length );
        channel.put( content.commandBytes(), content.commandBytes().length );
    }

    public static ReplicatedTokenRequest unmarshal( ReadableChannel channel ) throws IOException
    {
        TokenType type = TokenType.values()[ channel.getInt() ];
        String tokenName = StringMarshal.unmarshal( channel );

        int commandBytesLength = channel.getInt();
        byte[] commandBytes = new byte[ commandBytesLength ];
        channel.get( commandBytes, commandBytesLength );

        return new ReplicatedTokenRequest( type, tokenName, commandBytes );
    }

    public static void marshal( ReplicatedTokenRequest content, ByteBuf buffer )
    {
        buffer.writeInt( content.type().ordinal() );
        StringMarshal.marshal( buffer, content.tokenName() );

        buffer.writeInt( content.commandBytes().length );
        buffer.writeBytes( content.commandBytes() );
    }

    public static ReplicatedTokenRequest unmarshal( ByteBuf buffer )
    {
        TokenType type = TokenType.values()[ buffer.readInt() ];
        String tokenName = StringMarshal.unmarshal( buffer );

        int commandBytesLength = buffer.readInt();
        byte[] commandBytes = new byte[ commandBytesLength ];
        buffer.readBytes( commandBytes );

        return new ReplicatedTokenRequest( type, tokenName, commandBytes );
    }

    public static byte[] commandBytes( Collection<StorageCommand> commands )
    {
        ByteBuf commandBuffer = Unpooled.buffer();
        NetworkFlushableChannelNetty4 channel = new NetworkFlushableChannelNetty4( commandBuffer );

        try
        {
            new LogEntryWriter( channel ).serialize( commands );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "This should never happen since the channel is backed by an in-memory buffer.", e );
        }

        /*
         * This trims down the array to send up to the actual index it was written. Not doing this would send additional
         * zeroes which not only wasteful, but also not handled by the LogEntryReader receiving this.
         */
        byte[] commandsBytes = Arrays.copyOf( commandBuffer.array(), commandBuffer.writerIndex() );
        commandBuffer.release();

        return commandsBytes;
    }

    static Collection<StorageCommand> extractCommands( byte[] commandBytes )
    {
        ByteBuf txBuffer = Unpooled.wrappedBuffer( commandBytes );
        NetworkReadableClosableChannelNetty4 channel = new NetworkReadableClosableChannelNetty4( txBuffer );

        LogEntryReader<ReadableClosablePositionAwareChannel> reader = new VersionAwareLogEntryReader<>(
                new RecordStorageCommandReaderFactory(), InvalidLogEntryHandler.STRICT );

        LogEntryCommand entryRead;
        List<StorageCommand> commands = new LinkedList<>();

        try
        {
            while ( (entryRead = (LogEntryCommand) reader.readLogEntry( channel )) != null )
            {
                commands.add( entryRead.getCommand() );
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "This should never happen since the channel is backed by an in-memory buffer.", e );
        }

        return commands;
    }
}
