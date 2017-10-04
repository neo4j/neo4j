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
import java.util.function.Supplier;

import org.neo4j.bolt.logging.BoltMessageLogger;
import org.neo4j.cypher.result.QueryResult;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.helpers.BaseToObjectValueWriter;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.values.AnyValue;
import org.neo4j.values.utils.PrettyPrinter;
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
    private final BoltMessageLogger messageLogger;

    /**
     * @param packer            serializer to output channel
     * @param onMessageComplete invoked for each message, after it's done writing to the output
     * @param messageLogger     logger for Bolt messages
     */
    public BoltResponseMessageWriter( Neo4jPack.Packer packer, BoltResponseMessageBoundaryHook onMessageComplete,
                                      BoltMessageLogger messageLogger )
    {
        this.packer = packer;
        this.onMessageComplete = onMessageComplete;
        this.messageLogger = messageLogger;
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
        messageLogger.logSuccess( () -> metadata );
        packer.packStructHeader( 1, SUCCESS.signature() );
        packer.packRawMap( metadata );
        onMessageComplete.onMessageComplete();
    }

    private Supplier<String> metadataSupplier( MapValue metadata )
    {
        return () ->
        {
            PrettyPrinter printer = new PrettyPrinter();
            metadata.writeTo( printer );
            return printer.value();
        };
    }

    @Override
    public void onIgnored() throws IOException
    {
        messageLogger.logIgnored();
        packer.packStructHeader( 0, IGNORED.signature() );
        onMessageComplete.onMessageComplete();
    }

    @Override
    public void onFailure( Status status, String errorMessage ) throws IOException
    {
        messageLogger.logFailure( status );
        packer.packStructHeader( 1, FAILURE.signature() );
        packer.packMapHeader( 2 );

        packer.pack( "code" );
        packer.pack( status.code().serialize() );

        packer.pack( "message" );
        packer.pack( errorMessage );

        onMessageComplete.onMessageComplete();
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

    private class MapToObjectWriter extends BaseToObjectValueWriter<RuntimeException>
    {

        private UnsupportedOperationException exception =
                new UnsupportedOperationException( "Functionality not implemented." );

        @Override
        protected Node newNodeProxyById( long id )
        {
            throw exception;
        }

        @Override
        protected Relationship newRelationshipProxyById( long id )
        {
            throw exception;
        }

        @Override
        protected Point newGeographicPoint( double longitude, double latitude, String name, int code, String href )
        {
            throw exception;
        }

        @Override
        protected Point newCartesianPoint( double x, double y, String name, int code, String href )
        {
            throw exception;
        }
    }

}
