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

import org.neo4j.bolt.logging.BoltMessageLogger;
import org.neo4j.bolt.v1.packstream.PackOutput;
import org.neo4j.cypher.result.QueryResult;
import org.neo4j.function.ThrowingAction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.logging.Log;
import org.neo4j.values.AnyValue;
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
    private final PackOutput output;
    private final Neo4jPack.Packer packer;
    private final BoltMessageLogger messageLogger;
    private final Log log;

    public BoltResponseMessageWriter( Neo4jPack neo4jPack, PackOutput output, LogService logService, BoltMessageLogger messageLogger )
    {
        this.output = output;
        this.packer = neo4jPack.newPacker( output );
        this.messageLogger = messageLogger;
        this.log = logService.getInternalLog( getClass() );
    }

    @Override
    public void onRecord( QueryResult.Record item ) throws IOException
    {
        packCompleteMessageOrFail( RECORD, () ->
        {
            AnyValue[] fields = item.fields();
            packer.packStructHeader( 1, RECORD.signature() );
            packer.packListHeader( fields.length );
            for ( AnyValue field : fields )
            {
                packer.pack( field );
            }
        } );
    }

    @Override
    public void onSuccess( MapValue metadata ) throws IOException
    {
        packCompleteMessageOrFail( SUCCESS, () ->
        {
            packer.packStructHeader( 1, SUCCESS.signature() );
            packer.pack( metadata );
        } );

        messageLogger.logSuccess( () -> metadata );
    }

    @Override
    public void onIgnored() throws IOException
    {
        packCompleteMessageOrFail( IGNORED, () ->
        {
            packer.packStructHeader( 0, IGNORED.signature() );
        } );

        messageLogger.logIgnored();
    }

    @Override
    public void onFailure( Status status, String errorMessage ) throws IOException
    {
        packCompleteMessageOrFail( FAILURE, () ->
        {
            packer.packStructHeader( 1, FAILURE.signature() );
            packer.packMapHeader( 2 );

            packer.pack( "code" );
            packer.pack( status.code().serialize() );

            packer.pack( "message" );
            packer.pack( errorMessage );
        } );

        messageLogger.logFailure( status );
    }

    @Override
    public void onFatal( Status status, String errorMessage ) throws IOException
    {
        messageLogger.serverError( "FATAL", status);
        onFailure( status, errorMessage );
        flush();
    }

    public void flush() throws IOException
    {
        packer.flush();
    }

    private void packCompleteMessageOrFail( BoltResponseMessage message, ThrowingAction<IOException> action ) throws IOException
    {
        boolean packingFailed = true;
        output.beginMessage();
        try
        {
            action.apply();
            packingFailed = false;
            output.messageSucceeded();
        }
        catch ( Throwable error )
        {
            if ( packingFailed )
            {
                // packing failed, there might be some half-written data in the output buffer right now
                // notify output about the failure so that it cleans up the buffer
                output.messageFailed();
                log.error( "Failed to write full %s message because: %s", message, error.getMessage() );
            }
            throw error;
        }
    }
}
