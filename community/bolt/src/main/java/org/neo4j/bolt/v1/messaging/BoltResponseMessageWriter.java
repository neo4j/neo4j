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

import java.io.IOException;

import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.values.AnyValue;
import org.neo4j.values.result.QueryResult;
import org.neo4j.values.virtual.MapValue;

import static org.neo4j.bolt.v1.messaging.BoltResponseMessage.FAILURE;
import static org.neo4j.bolt.v1.messaging.BoltResponseMessage.IGNORED;
import static org.neo4j.bolt.v1.messaging.BoltResponseMessage.RECORD;
import static org.neo4j.bolt.v1.messaging.BoltResponseMessage.SUCCESS;

/**
 * Writer for Bolt request messages to be sent to a {@link Neo4jPack.Packer}.
 */
public class BoltResponseMessageWriter implements BoltResponseMessageHandler<IOException>
{
    public static final BoltResponseMessageBoundaryHook NO_BOUNDARY_HOOK = () ->
    {
    };

    private final Neo4jPack.Packer packer;
    private final BoltResponseMessageBoundaryHook onMessageComplete;

    /**
     * @param packer            serializer to output channel
     * @param onMessageComplete invoked for each message, after it's done writing to the output
     */
    public BoltResponseMessageWriter( Neo4jPack.Packer packer, BoltResponseMessageBoundaryHook onMessageComplete )
    {
        this.packer = packer;
        this.onMessageComplete = onMessageComplete;
    }

    @Override
    public void onRecord( QueryResult.Record item ) throws IOException
    {
        AnyValue[] fields = item.fields();
        packer.packStructHeader( 1, RECORD.signature() );
        packer.packListHeader( fields.length );
        for ( AnyValue field : fields )
        {
            packer.pack( field );
        }
        onMessageComplete.onMessageComplete();

        //The record might contain unpackable values,
        //hence we must consume any errors that might
        //have occurred.
        packer.consumeError();  // TODO: find a better way
    }

    @Override
    public void onSuccess( MapValue metadata ) throws IOException
    {
        packer.packStructHeader( 1, SUCCESS.signature() );
        packer.packRawMap( metadata );
        onMessageComplete.onMessageComplete();
    }

    @Override
    public void onIgnored() throws IOException
    {
        packer.packStructHeader( 0, IGNORED.signature() );
        onMessageComplete.onMessageComplete();
    }

    @Override
    public void onFailure( Status status, String message ) throws IOException
    {
        packer.packStructHeader( 1, FAILURE.signature() );
        packer.packMapHeader( 2 );

        packer.pack( "code" );
        packer.pack( status.code().serialize() );

        packer.pack( "message" );
        packer.pack( message );

        onMessageComplete.onMessageComplete();
    }

    @Override
    public void onFatal( Status status, String message ) throws IOException
    {
        onFailure( status, message );
        flush();
    }

    public void flush() throws IOException
    {
        packer.flush();
    }

}
