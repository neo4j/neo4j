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
package org.neo4j.coreedge.raft.log;

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import org.neo4j.coreedge.raft.ReplicatedInteger;
import org.neo4j.coreedge.raft.ReplicatedString;
import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.raft.state.ChannelMarshal;
import org.neo4j.coreedge.server.ByteBufMarshal;
import org.neo4j.storageengine.api.ReadPastEndException;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

public class DummyRaftableContentSerializer implements ChannelMarshal<ReplicatedContent>, ByteBufMarshal<ReplicatedContent>
{
    public static final int REPLICATED_INTEGER_TYPE = 0;
    public static final int REPLICATED_STRING_TYPE = 1;

    @Override
    public void marshal( ReplicatedContent content, WritableChannel channel ) throws IOException
    {
        if ( content instanceof ReplicatedInteger )
        {
            channel.put( (byte) REPLICATED_INTEGER_TYPE );
            channel.putInt( ((ReplicatedInteger) content).get() );
        }
        else if ( content instanceof ReplicatedString )
        {
            String value = ((ReplicatedString) content).get();
            byte[] stringBytes = value.getBytes();
            channel.put( (byte) REPLICATED_STRING_TYPE );
            channel.putInt( stringBytes.length );
            channel.put( stringBytes, stringBytes.length );
        }
        else
        {
            throw new IllegalArgumentException( "Unknown content type: " + content );
        }
    }

    @Override
    public ReplicatedContent unmarshal( ReadableChannel channel ) throws IOException
    {
        try
        {
            byte type = channel.get();
            switch ( type )
            {
                case REPLICATED_INTEGER_TYPE:
                    return ReplicatedInteger.valueOf( channel.getInt() );
                case REPLICATED_STRING_TYPE:
                    int length = channel.getInt();
                    byte[] bytes = new byte[length];
                    channel.get( bytes, length );
                    return ReplicatedString.valueOf( new String( bytes ) );
                default:
                    throw new IllegalArgumentException( "Unknown content type: " + type );
            }
        }
        catch( ReadPastEndException notEnoughBytes )
        {
            return null;
        }
    }

    @Override
    public void marshal( ReplicatedContent content, ByteBuf buffer )
    {
        if ( content instanceof ReplicatedInteger )
        {
            buffer.writeByte( (byte) REPLICATED_INTEGER_TYPE );
            buffer.writeInt( ((ReplicatedInteger) content).get() );
        }
        else if ( content instanceof ReplicatedString )
        {
            String value = ((ReplicatedString) content).get();
            byte[] stringBytes = value.getBytes();
            buffer.writeByte( (byte) REPLICATED_STRING_TYPE );
            buffer.writeInt( stringBytes.length );
            buffer.writeBytes( stringBytes );
        }
        else
        {
            throw new IllegalArgumentException( "Unknown content type: " + content );
        }
    }

    @Override
    public ReplicatedContent unmarshal( ByteBuf buffer )
    {
        try
        {
            byte type = buffer.readByte();
            switch ( type )
            {
                case REPLICATED_INTEGER_TYPE:
                    return ReplicatedInteger.valueOf( buffer.readInt() );
                case REPLICATED_STRING_TYPE:
                    int length = buffer.readInt();
                    byte[] bytes = new byte[length];
                    buffer.readBytes( bytes );
                    return ReplicatedString.valueOf( new String( bytes ) );
                default:
                    throw new IllegalArgumentException( "Unknown content type: " + type );
            }
        }
        catch( IndexOutOfBoundsException notEnoughBytes )
        {
            return null;
        }
    }
}
