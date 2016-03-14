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
package org.neo4j.bolt.v1.docs;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.bolt.v1.messaging.Neo4jPack;
import org.neo4j.bolt.v1.messaging.PackStreamMessageFormatV1;
import org.neo4j.bolt.v1.messaging.RecordingByteChannel;
import org.neo4j.bolt.v1.packstream.BufferedChannelOutput;
import org.neo4j.bolt.v1.runtime.internal.Neo4jError;
import org.neo4j.bolt.v1.runtime.spi.ImmutableRecord;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.util.HexPrinter;

import static org.neo4j.bolt.v1.transport.socket.Chunker.chunk;

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
        Neo4jPack.Packer packer = new Neo4jPack.Packer( new BufferedChannelOutput( ch, 128 ) );
        PackStreamMessageFormatV1.Writer writer = new PackStreamMessageFormatV1.Writer(
                packer, PackStreamMessageFormatV1.Writer.NO_OP );

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
    public static void pack( String value, Neo4jPack.Packer packer, PackStreamMessageFormatV1.Writer writer )
            throws IOException
    {
        ObjectMapper mapper = new ObjectMapper();

        // 1: Is the value a struct definition?
        if ( value.startsWith( "Struct" ) )
        {
            DocStructExample struct = new DocStructExample( DocPartName.unknown(), value );
            packer.packStructHeader( struct.size(), (byte) struct.signature() );

            for ( String s : struct )
            {
                pack( s, packer, writer );
            }
        }
        else if ( value.equals( "FAILURE { \"code\": \"Neo.ClientError.Statement.SyntaxError\",                  \"message\": \"Invalid input 'T': expected" +
                                " <init> (line 1, column 1 (offset: 0))                          \"This will cause a syntax error\"                          " +
                                " ^\"}" ) )
        {
            // Hard-coded special case, because we don't handle this message automatically yet
            writer.handleFailureMessage( Status.Statement.SyntaxError,
                    "Invalid input 'T': expected <init> (line 1, column 1 (offset: 0))\n" +
                    "\"This will cause a syntax error\"\n" +
                    " ^" );
        }
        else
        {
            try
            {
                // 2: Assume the value is a JSON value
                Object scalar = mapper.readValue( value, Object.class );
                packer.pack( scalar );
            }
            catch ( JsonParseException e )
            {
                // 3: If that fails, assume the value is a message with space-delimeted JSON-encoded arguments
                String[] parts = value.split( " ", 2 );
                List<Object> args = new ArrayList<>();
                String type = parts[0];
                if( parts.length == 2 )
                {
                    JsonParser parser = mapper.getJsonFactory().createJsonParser(
                            new ByteArrayInputStream( parts[1].getBytes( StandardCharsets.UTF_8 ) ) );
                    try
                    {
                        while ( !parser.isClosed() )
                        {
                            Object e1 = mapper.readValue( parser, Object.class );
                            args.add( e1 );
                        }
                    } catch( EOFException ignore ) {

                    } catch( JsonParseException je ) {
                        throw new RuntimeException( "Unable to parse documented protocol exchange in '" + value + "': " + je.getMessage(), je );
                    }
                }

                switch ( type )
                {
                    case "DISCARD_ALL":
                        writer.handleDiscardAllMessage();
                        break;
                    case "PULL_ALL":
                        writer.handlePullAllMessage();
                        break;
                    case "IGNORED":
                        writer.handleIgnoredMessage();
                        break;
                    case "RUN":
                        writer.handleRunMessage( (String) args.get( 0 ),
                                (Map<String, Object>) args.get( 1 ) );
                        break;
                    case "RECORD":
                        writer.handleRecordMessage( new ImmutableRecord( Iterables.asArray(
                                Object.class, (List<Object>) args.get( 0 ) ) ) );
                        break;
                    case "SUCCESS":
                        writer.handleSuccessMessage( (Map<String, Object>) args.get( 0 ) );
                        break;
                    case "FAILURE":
                        Map<String, Object> meta = (Map<String, Object>) args.get( 0 );
                        writer.handleFailureMessage( Neo4jError.codeFromString(
                                (String) meta.get( "code" ) ), (String) meta.get( "message" ) );
                        break;
                    case "INIT":
                        writer.handleInitMessage( (String) args.get( 0 ), (Map<String, Object>) args.get( 1 ) );
                        break;
                    case "RESET":
                        writer.handleResetMessage();
                        break;
                    case "ACK_FAILURE":
                        writer.handleAckFailureMessage();
                        break;
                    default:
                        throw new RuntimeException( "Unknown value: " + type );
                }
            }
        }
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
