/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.bolt.routing;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.neo4j.bolt.messaging.ResultConsumer;
import org.neo4j.bolt.runtime.AccessMode;
import org.neo4j.bolt.runtime.BoltResult;
import org.neo4j.bolt.runtime.statemachine.StatementMetadata;
import org.neo4j.bolt.runtime.statemachine.StatementProcessor;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;

/**
 * Implementation of @{@link RoutingTableGetter} which uses the `dbms.routing.getRoutingTable` procedure.
 */
public class ProcedureRoutingTableGetter implements RoutingTableGetter
{
    private static final String GET_ROUTING_TABLE_STATEMENT = "CALL dbms.routing.getRoutingTable($routingContext, $databaseName)";
    private static final String ROUTING_CONTEXT_PARAM = "routingContext";
    private static final String DATABASE_NAME_PARAM = "databaseName";

    @Override
    public CompletableFuture<MapValue> get( StatementProcessor statementProcessor, MapValue routingContext, String databaseName )
    {
        var params = getParams( routingContext, databaseName );

        var future = new CompletableFuture<MapValue>();

        try
        {
            StatementMetadata statementMetadata =
                    statementProcessor.run( GET_ROUTING_TABLE_STATEMENT, params, List.of(), null, AccessMode.READ, Map.of() );

            statementProcessor.streamResult( statementMetadata.queryId(), new RoutingTableConsumer( future ) );
        }
        catch ( Throwable throwable )
        {
            future.completeExceptionally( throwable );
        }

        return future;
    }

    private MapValue getParams( MapValue routingContext, String databaseName )
    {
        var paramsBuilder = new MapValueBuilder();
        paramsBuilder.add( ROUTING_CONTEXT_PARAM, routingContext );
        paramsBuilder.add( DATABASE_NAME_PARAM, Values.stringOrNoValue( databaseName ) );
        return paramsBuilder.build();
    }

    private static class RoutingTableConsumer implements ResultConsumer
    {
        private final CompletableFuture<MapValue> future;

        private RoutingTableConsumer( CompletableFuture<MapValue> future )
        {
            this.future = future;
        }

        @Override
        public void consume( BoltResult result ) throws Throwable
        {
            var consumer = new RoutingTableRecordConsumer( future, result.fieldNames() );
            result.handleRecords( consumer, 1L );
        }

        @Override
        public boolean hasMore()
        {
            return false;
        }
    }

    private static class RoutingTableRecordConsumer implements BoltResult.RecordConsumer
    {
        private final CompletableFuture<MapValue> future;
        private final MapValueBuilder mapValueBuilder;
        private final List<String> fields;
        private Iterator<String> fieldsIt;

        private RoutingTableRecordConsumer( CompletableFuture<MapValue> future, String[] fields )
        {
            this.future = future;
            this.mapValueBuilder = new MapValueBuilder();
            this.fields = List.of( fields );
        }

        @Override
        public void addMetadata( String key, AnyValue value )
        {
        }

        @Override
        public void beginRecord( int numberOfFields ) throws IOException
        {
            this.fieldsIt = this.fields.iterator();
        }

        @Override
        public void consumeField( AnyValue value ) throws IOException
        {
            this.mapValueBuilder.add( this.fieldsIt.next(), value );
        }

        @Override
        public void endRecord() throws IOException
        {
            future.complete( mapValueBuilder.build() );
        }

        @Override
        public void onError() throws IOException
        {
            future.completeExceptionally( new RuntimeException( "Error processing the record" ) );
        }
    }
}
