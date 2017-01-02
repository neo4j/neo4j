/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.bolt.v1.messaging;

import org.neo4j.bolt.v1.packstream.PackStream;
import org.neo4j.kernel.api.exceptions.Status;

import java.io.IOException;
import java.util.Map;

import static org.neo4j.bolt.v1.runtime.Neo4jError.codeFromString;
import static org.neo4j.bolt.v1.runtime.spi.Records.record;

public class BoltResponseMessageReader
{
    private final Neo4jPack.Unpacker unpacker;

    public BoltResponseMessageReader( Neo4jPack.Unpacker unpacker )
    {
        this.unpacker = unpacker;
    }

    public boolean hasNext() throws IOException
    {
        return unpacker.hasNext();
    }

    public <E extends Exception> void read( BoltResponseMessageHandler<E> handler ) throws IOException, E
    {
        try
        {
            unpacker.unpackStructHeader();
            final int signature = (int) unpacker.unpackStructSignature();
            BoltResponseMessage message = BoltResponseMessage.withSignature( signature );
            try
            {
                switch ( message )
                {
                case SUCCESS:
                    Map<String, Object> successMetadata = unpacker.unpackMap();
                    handler.onSuccess( successMetadata );
                    break;
                case RECORD:
                    long length = unpacker.unpackListHeader();
                    final Object[] fields = new Object[(int) length];
                    for ( int i = 0; i < length; i++ )
                    {
                        fields[i] = unpacker.unpack();
                    }
                    handler.onRecord( record( fields ) );
                    break;
                case IGNORED:
                    handler.onIgnored();
                    break;
                case FAILURE:
                    Map<String, Object> failureMetadata = unpacker.unpackMap();
                    String code = failureMetadata.containsKey( "code" ) ?
                            (String) failureMetadata.get( "code" ) :
                            Status.General.UnknownError.name();
                    String msg = failureMetadata.containsKey( "message" ) ?
                            (String) failureMetadata.get( "message" ) :
                            "<No message supplied>";
                    handler.onFailure( codeFromString( code ), msg );
                    break;
                default:
                    throw new BoltIOException( Status.Request.Invalid,
                            "Message 0x" + Integer.toHexString( signature ) + " is not supported." );
                }
            }
            catch ( IllegalArgumentException e )
            {
                throw new BoltIOException( Status.Request.Invalid,
                        "0x" + Integer.toHexString( signature ) + " is not a valid message signature." );
            }
        }
        catch ( PackStream.PackStreamException e )
        {
            throw new BoltIOException( Status.Request.InvalidFormat, "Unable to read message type. " +
                    "Error was: " + e.getMessage(), e );
        }
    }

}
