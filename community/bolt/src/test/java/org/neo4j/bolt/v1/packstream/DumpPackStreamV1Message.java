/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.bolt.v1.packstream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.List;

import org.neo4j.bolt.v1.messaging.Neo4jPack;
import org.neo4j.bolt.v1.messaging.PackStreamMessageFormatV1.Reader;
import org.neo4j.bolt.v1.messaging.RecordingByteChannel;
import org.neo4j.bolt.v1.messaging.RecordingMessageHandler;
import org.neo4j.bolt.v1.messaging.message.Message;
import org.neo4j.bolt.v1.transport.ChunkedInput;

/**
 * Input: a hex string
 * output: the message dechunked and/or unpacked by the Bolt protocol PackStreamV1
 *
 * e.g.
 * Input:  00 0F B1 01 8C 4D 79 43    6C 69 65 6E 74 2F 31 2E    30 00 00
 * Output: INIT "MyClient/1.0"
 *
 * or
 * Input:  B1 01 8C 4D 79 43    6C 69 65 6E 74 2F 31 2E    30
 * Output: INIT "MyClient/1.0"
 */
public class DumpPackStreamV1Message
{

    public static void main( String args[] )
    {
        if ( args.length < 1 )
        {
            System.out.println(
                    "Please specify PackStreamV1 messages (or PackStreamV1 messages in chunks) " +
                    "that you want to unpack in hex strings. " );
            return;
        }
        StringBuilder hexStr = new StringBuilder();
        for ( String arg : args )
        {
            hexStr.append( arg );
        }

        byte[] bytes = hexStringToBytes( hexStr.toString() );

        List<Message> messages;
        try
        {
            // first try to interpret as a message with chunk header and 00 00 tail
            messages = dechunk( bytes );
        }
        catch ( IOException e )
        {
            // fall back to interpret as a message without chunk header and 00 00 tail
            try
            {
                messages = unpack( bytes );
            }
            catch ( IOException ee )
            {
                // If both of them failed, then print the debug info for both of them.
                System.err.println( "Failed to interpret the given hex string." );
                e.printStackTrace();

                System.out.println();
                ee.printStackTrace();
                return;
            }
        }

        for ( Message message : messages )
        {
            System.out.println( message );
        }
    }

    /**
     * Trying to dechunk the string first and then unpack the content
     */
    public static List<Message> dechunk( byte[] bytes ) throws IOException
    {
        TraceableChunkedInput input = new TraceableChunkedInput();
        ByteBuffer buffer = ByteBuffer.wrap( bytes );

        int offset = 2; // current chunks header
        do
        {
            int size = buffer.getShort() & 0xFFFF;
            if ( size < 1 || buffer.remaining() < size )
            {
                throw new IOException( "Wrong chunk size: " + size + " at pos in input stream: " + buffer.position() );
            }
            input.append( Unpooled.wrappedBuffer( bytes, offset, size ) );
            buffer.position( offset + size + 2 ); // size + tail
            offset += size + 2 + 2; // size + tail + next chunk's header
        }
        while ( buffer.hasRemaining() );

        return unpackMessages( bytes, input );
    }

    /**
     * Unpack messages directly
     */
    public static List<Message> unpack( byte[] bytes ) throws IOException
    {
        RecordingByteChannel channel = new RecordingByteChannel();
        BufferedChannelInput input = new BufferedChannelInput( bytes.length ).reset( channel );
        channel.write( ByteBuffer.wrap( bytes ) );
        channel.eof();

        return unpackMessages( bytes, new TraceablePackInput( input ) );
    }

    private static List<Message> unpackMessages( byte[] bytes, TraceablePackInput input )
            throws IOException
    {
        Reader reader = new Reader( new Neo4jPack.Unpacker( input ) );
        RecordingMessageHandler messages = new RecordingMessageHandler();

        try
        {
            while ( reader.hasNext() )
            {
                reader.read( messages );
            }
        }
        catch ( Exception e )
        {
            throw new IOException(
                    "Error when interpreting the message as PackStreamV1 message:" +
                    "\nMessage interpreted : " + messages.asList() +
                    "\n" + hexInOneLine( ByteBuffer.wrap( bytes ), 0, bytes.length ) + /* all bytes */
                    "\n" + padLeft( input.prePos ) /* the indicator of the error place*/, e );
        }

        return messages.asList();
    }

    private static class TraceableChunkedInput extends TraceablePackInput
    {
        public TraceableChunkedInput()
        {
            delegate = new ChunkedInput()
            {
                boolean isFirstChunk = true;

                protected ByteBuf getCurrentChunk()
                {
                    ByteBuf chunk = super.getCurrentChunk();

                    if ( isFirstChunk )
                    {
                        prePos += 2;
                        isFirstChunk = false;
                    }
                    else
                    {
                        prePos += 4;
                    }

                    return chunk;
                }
            };
        }

        public void append( ByteBuf byteBuf )
        {
            ((ChunkedInput) delegate).append( byteBuf );
        }
    }

    private static class TraceablePackInput implements PackInput
    {
        protected PackInput delegate;
        protected int prePos; // where unpacking starts
        protected int curPos; // where reading starts

        public TraceablePackInput()
        {
            this( null );
        }

        public TraceablePackInput( PackInput delegate )
        {
            prePos = curPos = 0;
            this.delegate = delegate;
        }

        @Override
        public boolean hasMoreData() throws IOException
        {
            return delegate.hasMoreData();
        }

        @Override
        public byte readByte() throws IOException
        {
            prePos = curPos;
            byte read = delegate.readByte();
            curPos += 1;
            return read;
        }

        @Override
        public short readShort() throws IOException
        {
            prePos = curPos;
            short read = delegate.readShort();
            curPos += 2;
            return read;
        }

        @Override
        public int readInt() throws IOException
        {
            prePos = curPos;
            int read = delegate.readInt();
            curPos += 4;
            return read;
        }

        @Override
        public long readLong() throws IOException
        {
            prePos = curPos;
            long read = delegate.readLong();
            curPos += 8;
            return read;
        }

        @Override
        public double readDouble() throws IOException
        {
            prePos = curPos;
            double read = delegate.readDouble();
            curPos += 8;
            return read;
        }

        @Override
        public PackInput readBytes( byte[] into, int offset, int toRead ) throws IOException
        {
            prePos = curPos;
            PackInput packInput = delegate.readBytes( into, offset, toRead );
            curPos += toRead;
            return packInput;
        }

        @Override
        public byte peekByte() throws IOException
        {
            return delegate.peekByte();
        }

    }

    static byte[] hexStringToBytes( String s )
    {
        int len = s.length();
        ByteArrayOutputStream data = new ByteArrayOutputStream( 1024 );
        for ( int i = 0; i < len; )
        {
            int firstDigit = Character.digit( s.charAt( i ), 16 );
            if ( firstDigit != -1 )
            {
                int secondDigit = Character.digit( s.charAt( i + 1 ), 16 );
                int toWrite = (firstDigit << 4) + secondDigit;
                data.write( toWrite );
                i += 2;
            }
            else
            {
                i += 1;
            }
        }
        return data.toByteArray();
    }

    private static String hexInOneLine( ByteBuffer bytes, int offset, int length )
    {
        try
        {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            PrintStream out;
            out = new PrintStream( baos, true, "UTF-8" );
            for ( int i = offset; i < offset + length; i++ )
            {
                out.print( String.format( "%02x", bytes.get( i ) ) );
                if ( i == offset + length - 1 )
                {
                    // no pending blanks
                }
                else if ( (i - offset + 1) % 8 == 0 )
                {
                    out.print( "    " );
                }
                else
                {
                    out.print( " " );
                }
            }
            return baos.toString( "UTF-8" );
        }
        catch ( UnsupportedEncodingException e )
        {
            throw new RuntimeException( e );
        }
    }

    private static String padLeft( int offset )
    {
        StringBuilder output = new StringBuilder();
        for ( int i = 0; i < offset; i++ )
        {
            output.append( "  " );
            if ( (i + 1) % 8 == 0 )
            {
                output.append( "    " );
            }
            else
            {
                output.append( " " );
            }
        }
        output.append( "^^" );
        return output.toString();
    }
}
