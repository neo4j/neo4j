/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;

public abstract class Protocol
{
    public static final int PORT = 8901;
    public static final int MEGA = 1024 * 1024;
    public static final int DEFAULT_FRAME_LENGTH = 16*MEGA;

    public static final ObjectSerializer<Integer> INTEGER_SERIALIZER = new ObjectSerializer<Integer>()
    {
        @SuppressWarnings( "boxing" )
        public void write( Integer responseObject, ChannelBuffer result ) throws IOException
        {
            result.writeInt( responseObject );
        }
    };
    public static final ObjectSerializer<Long> LONG_SERIALIZER = new ObjectSerializer<Long>()
    {
        @SuppressWarnings( "boxing" )
        public void write( Long responseObject, ChannelBuffer result ) throws IOException
        {
            result.writeLong( responseObject );
        }
    };
    public static final ObjectSerializer<Void> VOID_SERIALIZER = new ObjectSerializer<Void>()
    {
        public void write( Void responseObject, ChannelBuffer result ) throws IOException
        {
        }
    };
    public static final Deserializer<Integer> INTEGER_DESERIALIZER = new Deserializer<Integer>()
    {
        public Integer read( ChannelBuffer buffer, ByteBuffer temporaryBuffer ) throws IOException
        {
            return buffer.readInt();
        }
    };
    public static final Deserializer<Void> VOID_DESERIALIZER = new Deserializer<Void>()
    {
        public Void read( ChannelBuffer buffer, ByteBuffer temporaryBuffer ) throws IOException
        {
            return null;
        }
    };
    public static final Serializer EMPTY_SERIALIZER = new Serializer()
    {
        public void write( ChannelBuffer buffer, ByteBuffer readBuffer ) throws IOException
        {
        }
    };
    public static class FileStreamsDeserializer implements Deserializer<Void>
    {
        private final StoreWriter writer;

        public FileStreamsDeserializer( StoreWriter writer )
        {
            this.writer = writer;
        }
        
        // NOTICE: this assumes a "smart" ChannelBuffer that continues to next chunk
        public Void read( ChannelBuffer buffer, ByteBuffer temporaryBuffer ) throws IOException
        {
            int pathLength;
            while ( 0 != ( pathLength = buffer.readUnsignedShort() ) )
            {
                String path = readString( buffer, pathLength );
                boolean hasData = buffer.readByte() == 1;
                writer.write( path, hasData ? new BlockLogReader( buffer ) : null, temporaryBuffer, hasData );
            }
            writer.done();
            return null;
        }
    };
    
    public static void addLengthFieldPipes( ChannelPipeline pipeline, int frameLength )
    {
        pipeline.addLast( "frameDecoder",
                new LengthFieldBasedFrameDecoder( frameLength+4, 0, 4, 0, 4 ) );
        pipeline.addLast( "frameEncoder", new LengthFieldPrepender( 4 ) );
    }

    public static void writeString( ChannelBuffer buffer, String name )
    {
        char[] chars = name.toCharArray();
        buffer.writeInt( chars.length );
        writeChars( buffer, chars );
    }

    public static void writeChars( ChannelBuffer buffer, char[] chars )
    {
        // TODO optimize?
        for ( char ch : chars )
        {
            buffer.writeChar( ch );
        }
    }

    public static String readString( ChannelBuffer buffer )
    {
        return readString( buffer, buffer.readInt() );
    }

    public static boolean readBoolean( ChannelBuffer buffer )
    {
        byte value = buffer.readByte();
        switch ( value )
        {
        case 0: return false;
        case 1: return true;
        default: throw new ComException( "Invalid boolean value " + value );
        }
    }
    
    public static String readString( ChannelBuffer buffer, int length )
    {
        char[] chars = new char[length];
        for ( int i = 0; i < length; i++ )
        {
            chars[i] = buffer.readChar();
        }
        return new String( chars );
    }

    public static void assertChunkSizeIsWithinFrameSize( int chunkSize, int frameLength )
    {
        if ( chunkSize > frameLength )
            throw new IllegalArgumentException( "Chunk size " + chunkSize +
                    " needs to be equal or less than frame length " + frameLength );
    }
}
