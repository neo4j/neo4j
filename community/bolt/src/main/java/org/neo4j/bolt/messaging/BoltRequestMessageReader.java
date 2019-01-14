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
package org.neo4j.bolt.messaging;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.neo4j.bolt.runtime.BoltConnection;
import org.neo4j.bolt.runtime.BoltResponseHandler;
import org.neo4j.bolt.runtime.Neo4jError;
import org.neo4j.bolt.v1.packstream.PackStream;
import org.neo4j.kernel.api.exceptions.Status;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

/**
 * Reader for Bolt request messages made available via a {@link Neo4jPack.Unpacker}.
 */
public abstract class BoltRequestMessageReader
{
    private final BoltConnection connection;
    private final BoltResponseHandler externalErrorResponseHandler;
    private final Map<Integer,RequestMessageDecoder> decoders;

    protected BoltRequestMessageReader( BoltConnection connection, BoltResponseHandler externalErrorResponseHandler,
            List<RequestMessageDecoder> decoders )
    {
        this.connection = connection;
        this.externalErrorResponseHandler = externalErrorResponseHandler;
        this.decoders = decoders.stream().collect( toMap( RequestMessageDecoder::signature, identity() ) );
    }

    public void read( Neo4jPack.Unpacker unpacker ) throws IOException
    {
        try
        {
            doRead( unpacker );
        }
        catch ( BoltIOException e )
        {
            if ( e.causesFailureMessage() )
            {
                Neo4jError error = Neo4jError.from( e );
                connection.enqueue( stateMachine -> stateMachine.handleExternalFailure( error, externalErrorResponseHandler ) );
            }
            else
            {
                throw e;
            }
        }
    }

    private void doRead( Neo4jPack.Unpacker unpacker ) throws IOException
    {
        try
        {
            unpacker.unpackStructHeader();
            int signature = unpacker.unpackStructSignature();

            RequestMessageDecoder decoder = decoders.get( signature );
            if ( decoder == null )
            {
                throw new BoltIOException( Status.Request.InvalidFormat,
                        String.format( "Message 0x%s is not a valid message signature.", Integer.toHexString( signature ) ) );
            }

            RequestMessage message = decoder.decode( unpacker );
            BoltResponseHandler responseHandler = decoder.responseHandler();

            connection.enqueue( stateMachine -> stateMachine.process( message, responseHandler ) );
        }
        catch ( PackStream.PackStreamException e )
        {
            throw new BoltIOException( Status.Request.InvalidFormat,
                    String.format( "Unable to read message type. Error was: %s.", e.getMessage() ), e );
        }
    }
}
