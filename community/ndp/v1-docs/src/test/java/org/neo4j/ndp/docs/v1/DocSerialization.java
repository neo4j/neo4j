/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.ndp.docs.v1;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.util.HexPrinter;
import org.neo4j.ndp.messaging.v1.PackStreamMessageFormatV1;
import org.neo4j.ndp.messaging.v1.RecordingByteChannel;
import org.neo4j.ndp.messaging.v1.RecordingMessageHandler;
import org.neo4j.ndp.messaging.v1.message.Message;
import org.neo4j.ndp.runtime.internal.Neo4jError;
import org.neo4j.ndp.transport.socket.ChunkedInput;
import org.neo4j.packstream.BufferedChannelOutput;
import org.neo4j.packstream.PackStream;
import org.neo4j.ndp.runtime.spi.ImmutableRecord;

import static java.util.Arrays.asList;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.ndp.transport.socket.Chunker.chunk;

/**
 * Takes human-readable value descriptions and packs them to binary data, and vice versa.
 * <p/>
 * Examples:
 * <p/>
 * - 1
 * - "hello, world"
 * - RUN "RETURN a", {a:12}
 * - SUCCESS {}
 */
public class DocSerialization
{
    public static byte[] packAndChunk( String value, int chunkSize ) throws IOException
    {
        return chunk( chunkSize, new byte[][]{pack( value )} );
    }

    public static byte[] pack( String value ) throws IOException
    {
        RecordingByteChannel ch = new RecordingByteChannel();
        PackStream.Packer packer = new PackStream.Packer( new BufferedChannelOutput( ch, 128 ) );
        PackStreamMessageFormatV1.Writer writer = new PackStreamMessageFormatV1.Writer( packer, PackStreamMessageFormatV1.Writer.NO_OP );

        pack( value, packer, writer );

        packer.flush();
        return ch.getBytes();
    }

    /**
     * @param value
     * @param packer
     * @param writer a message writer that delegates to the packer, for packing protocol messages
     * @throws IOException
     */
    public static void pack( String value, PackStream.Packer packer, PackStreamMessageFormatV1.Writer writer )
            throws IOException
    {
        // NOTE: This currently has hard-coded handling of specific messages, it did not seem worth the time
        // to write a custom parser for this yet. We may want to come back and do that as the docs evolve.
        if ( value.equalsIgnoreCase( "null" ) )
        {
            packer.packNull();
        }
        else if ( value.equalsIgnoreCase( "true" ) )
        {
            packer.pack( true );
        }
        else if ( value.equalsIgnoreCase( "false" ) )
        {
            packer.pack( false );
        }
        else if ( value.startsWith( "\"" ) )
        {
            packer.pack( value.substring( 1, value.length() - 1 ) );
        }
        else if ( value.startsWith( "[" ) )
        {
            if ( value.equals( "[]" ) )
            {
                packer.packListHeader( 0 );
            }
            else
            {
                String[] values = value.substring( 1, value.length() - 1 ).split( "," );
                packer.packListHeader( values.length );
                for ( String s : values )
                {
                    pack( s, packer, writer );
                }
            }
        }
        else if ( value.startsWith( "{" ) )
        {
            if ( value.equals( "{}" ) )
            {
                packer.packMapHeader( 0 );
            }
            else
            {
                String[] pairs = value.substring( 1, value.length() - 1 ).split( "," );
                packer.packMapHeader( pairs.length );
                for ( String pair : pairs )
                {
                    String[] split = pair.split( ":" );
                    packer.pack(
                            split[0] );  // Key, different from packing value because it doesn't use quotation marks
                    pack( split[1], packer, writer ); // Value
                }
            }
        }
        else if ( value.startsWith( "Struct" ) )
        {
            DocStructExample struct = new DocStructExample( value );
            packer.packStructHeader( struct.size(), (byte)struct.signature() );

            for ( String s : struct )
            {
                pack( s, packer, writer );
            }
        }
        else if ( value.matches( "-?[0-9]+\\.[0-9]+" ) )
        {
            packer.pack( Double.parseDouble( value ) );
        }
        else if ( value.matches( "-?[0-9]+" ) )
        {
            packer.pack( Long.parseLong( value ) );
        }
        else if ( value.equals( "DISCARD_ALL" ) )
        {
            writer.handleIgnoredMessage();
        }
        else if ( value.equals( "PULL_ALL" ) )
        {
            writer.handlePullAllMessage();
        }
        else if ( value.equals( "ACK_FAILURE" ) )
        {
            writer.handleAckFailureMessage();
        }
        else if ( value.equals( "IGNORED" ) ) // kiss..
        {
            writer.handleIgnoredMessage();
        }
        else if ( value.equals( "RUN \"RETURN 1 AS num\" {}" ) ) // kiss..
        {
            writer.handleRunMessage( "RETURN 1 AS num", Collections.<String,Object>emptyMap() );
        }
        else if ( value.equals( "RUN \"This will cause a syntax error\" {}" ) ) // kiss..
        {
            writer.handleRunMessage( "This will cause a syntax error", Collections.<String,Object>emptyMap() );
        }
        else if ( value.equals( "RECORD [1,2,3]" ) ) // kiss..
        {
            writer.handleRecordMessage( new ImmutableRecord( new Object[]{1, 2, 3} ) );
        }
        else if ( value.equals( "RECORD [1]" ) ) // kiss..
        {
            writer.handleRecordMessage( new ImmutableRecord( new Object[]{1} ) );
        }
        else if ( value.equals( "SUCCESS {fields:[\"name\", \"age\"]}" ) ) // kiss..
        {
            writer.handleSuccessMessage( map( "fields", asList( "name", "age" ) ) );
        }
        else if ( value.equals( "SUCCESS { fields: ['num'] }" ) ) // kiss..
        {
            writer.handleSuccessMessage( map( "fields", asList( "num" ) ) );
        }
        else if ( value.equals( "SUCCESS {}" ) ) // kiss..
        {
            writer.handleSuccessMessage( map() );
        }
        else if ( value.equals( "FAILURE {code:\"Neo.ClientError.Statement.InvalidSyntax\", " +
                                "message:\"Invalid syntax.\"}" ) ) // kiss..
        {
            writer.handleFailureMessage(  Status.Statement.InvalidSyntax, "Invalid syntax." );
        }
        else if ( value.equals( "FAILURE {code:\"Neo.ClientError.Statement.InvalidSyntax\"," ) ) // kiss..
        {
            writer.handleFailureMessage(  Status.Statement.InvalidSyntax,
                    "Invalid input 'T': expected <init> (line 1, column 1 (offset: 0))\n" +
                    "\"This will cause a syntax error\"\n" +
                    " ^" );
        }
        else if( value.equals( "INITIALIZE \"MyClient/1.0\"" ))
        {
            writer.handleInitializeMessage( "MyClient/1.0" );
        }
        else
        {
            throw new RuntimeException( "Unknown value: " + value );
        }
    }

    public static List<Message> unpackChunked( byte[] data ) throws Exception
    {
        ChunkedInput input = new ChunkedInput();
        PackStreamMessageFormatV1.Reader reader =
                new PackStreamMessageFormatV1.Reader( new PackStream.Unpacker(  input ) );
        RecordingMessageHandler messages = new RecordingMessageHandler();

        ByteBuf buf = Unpooled.wrappedBuffer( data );
        while ( buf.readableBytes() > 0 )
        {
            int chunkSize = buf.readUnsignedShort();
            if ( chunkSize > 0 )
            {
                input.append( buf.readSlice( chunkSize ) );
            }
            else
            {
                reader.read( messages );
                input.clear();
            }
        }
        return messages.asList();
    }

    public static String normalizedHex( byte[] data )
    {
        return normalizedHex( HexPrinter.hex( data ) );
    }

    /** Convert a hex string into a normalized format for string comparison */
    public static String normalizedHex( String dirtyHex )
    {
        StringBuilder str = new StringBuilder( dirtyHex.replace( "\r", "" ).replace( "\n", "" ).replace( " ", "" ) );
        int idx = str.length() - 2;

        while ( idx > 0 )
        {
            str.insert( idx, " " );
            idx = idx - 2;
        }

        return str.toString().toUpperCase();
    }
}
