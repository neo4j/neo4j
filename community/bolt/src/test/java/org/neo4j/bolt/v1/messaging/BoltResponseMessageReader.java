/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import java.io.IOException;

import org.neo4j.bolt.v1.packstream.PackStream;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.StringValue;
import org.neo4j.values.virtual.MapValue;

import static org.neo4j.bolt.v1.runtime.Neo4jError.codeFromString;
import static org.neo4j.bolt.v1.runtime.spi.Records.record;

public class BoltResponseMessageReader
{
    private final Neo4jPack.Unpacker unpacker;

    public BoltResponseMessageReader( Neo4jPack.Unpacker unpacker )
    {
        this.unpacker = unpacker;
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
                    MapValue successMetadata = unpacker.unpackMap();
                    handler.onSuccess( successMetadata );
                    break;
                case RECORD:
                    long length = unpacker.unpackListHeader();
                    final AnyValue[] fields = new AnyValue[(int) length];
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
                    MapValue failureMetadata = unpacker.unpackMap();
                    String code = failureMetadata.containsKey( "code" ) ?
                                  ((StringValue) failureMetadata.get( "code" )).stringValue() :
                            Status.General.UnknownError.name();
                    String msg = failureMetadata.containsKey( "message" ) ?
                                 ((StringValue) failureMetadata.get( "message" )).stringValue() :
                            "<No message supplied>";
                    handler.onFailure( codeFromString( code ), msg );
                    break;
                default:
                    throw new BoltIOException( Status.Request.InvalidFormat,
                            String.format( "Message 0x%s is not supported.", Integer.toHexString( signature ) ) );
                }
            }
            catch ( IllegalArgumentException e )
            {
                throw new BoltIOException( Status.Request.InvalidFormat,
                        String.format( "Message 0x%s is not a valid message signature.", Integer.toHexString( signature ) ) );
            }
        }
        catch ( PackStream.PackStreamException e )
        {
            throw new BoltIOException( Status.Request.InvalidFormat,
                    String.format( "Unable to read message type. Error was: %s.", e.getMessage() ), e );
        }
    }

}
