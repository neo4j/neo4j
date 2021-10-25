/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.server.http.cypher.consumer;

import java.util.Arrays;
import java.util.Map;

import org.neo4j.bolt.messaging.ResultConsumer;
import org.neo4j.bolt.runtime.BoltResult;
import org.neo4j.bolt.runtime.statemachine.StatementMetadata;
import org.neo4j.fabric.stream.summary.EmptyExecutionPlanDescription;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.server.http.cypher.OutputEventStream;
import org.neo4j.server.http.cypher.TransactionIndependentValueMapper;
import org.neo4j.server.http.cypher.entity.HttpExecutionPlanDescription;
import org.neo4j.server.http.cypher.format.api.Statement;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.TextValue;

import static org.neo4j.graphdb.QueryExecutionType.QueryType.READ_ONLY;
import static org.neo4j.graphdb.QueryExecutionType.QueryType.READ_WRITE;
import static org.neo4j.graphdb.QueryExecutionType.QueryType.SCHEMA_WRITE;
import static org.neo4j.graphdb.QueryExecutionType.QueryType.WRITE;
import static org.neo4j.graphdb.QueryExecutionType.explained;
import static org.neo4j.graphdb.QueryExecutionType.profiled;
import static org.neo4j.graphdb.QueryExecutionType.query;
import static org.neo4j.server.http.cypher.entity.HttpNotification.iterableFromAnyValue;
import static org.neo4j.server.http.cypher.entity.HttpQueryStatistics.fromAnyValue;
import static org.neo4j.values.storable.Values.stringValue;

public class OutputEventStreamResultConsumer implements ResultConsumer
{
    private final TextValue READ_TEXT = stringValue( "r" );
    private final TextValue READ_WRITE_TEXT = stringValue( "rw" );
    private final TextValue WRITE_TEXT = stringValue( "w" );
    private final TextValue SCHEMA_WRITE_TEXT = stringValue( "s" );
    private static final String TYPE = "type";
    private static final String STATS = "stats";
    private static final String PROFILE = "profile";
    private static final String PLAN = "plan";
    private static final String NOTIFICATIONS = "notifications";

    private final OutputEventStream outputEventStream;
    private final Statement statement;
    private final StatementMetadata statementMetadata;
    private final TransactionIndependentValueMapper valueMapper;

    public OutputEventStreamResultConsumer( OutputEventStream outputEventStream, Statement statement, StatementMetadata statementMetadata,
                                            TransactionIndependentValueMapper valueMapper )
    {
        this.outputEventStream = outputEventStream;
        this.statement = statement;
        this.statementMetadata = statementMetadata;
        this.valueMapper = valueMapper;
    }

    @Override
    public void consume( BoltResult t ) throws Throwable
    {
        outputEventStream.writeStatementStart( statement, Arrays.asList( statementMetadata.fieldNames().clone() ) );
        var outputEventStreamRecordConsumer = new OutputEventStreamRecordConsumer( t, outputEventStream, valueMapper );
        t.handleRecords( outputEventStreamRecordConsumer, -1 );

        //todo this can be a lot tidier. We're converting back from what AbstractCypherAdapterCypherStream
        QueryExecutionType queryExecutionType = extractQueryExecutionType( outputEventStreamRecordConsumer.metadataMap() );
        QueryStatistics queryStatistics = fromAnyValue( outputEventStreamRecordConsumer.metadataMap().getOrDefault( STATS, null ) );

        ExecutionPlanDescription executionPlanDescription = extractExecutionPlanDescription( outputEventStreamRecordConsumer.metadataMap() );

        Iterable<Notification> notifications = iterableFromAnyValue( outputEventStreamRecordConsumer.metadataMap().getOrDefault( NOTIFICATIONS, null ) );
        outputEventStream.writeStatementEnd( queryExecutionType, queryStatistics, executionPlanDescription, notifications );
    }

    @Override
    public boolean hasMore()
    {
        return false;
    }

    private QueryExecutionType extractQueryExecutionType( Map<String,AnyValue> metaDataMap )
    {
        AnyValue queryType = metaDataMap.get( TYPE );

        if ( metaDataMap.containsKey( PLAN ) )
        {
            if ( queryType.equals( READ_TEXT ) )
            {
                return explained( READ_ONLY );
            }
            else if ( queryType.equals( READ_WRITE_TEXT ) )
            {
                return explained( READ_WRITE );
            }
            else if ( queryType.equals( WRITE_TEXT ) )
            {
                return explained( WRITE );
            }
            else if ( queryType.equals( SCHEMA_WRITE_TEXT ) )
            {
                return explained( SCHEMA_WRITE );
            }
            else
            {
                // should not happen but default to read write if so.
                return explained( READ_WRITE );
            }
        }
        else if ( metaDataMap.containsKey( PROFILE ) )
        {
            if ( queryType.equals( READ_TEXT ) )
            {
                return profiled( READ_ONLY );
            }
            else if ( queryType.equals( READ_WRITE_TEXT ) )
            {
                return profiled( READ_WRITE );
            }
            else if ( queryType.equals( WRITE_TEXT ) )
            {
                return profiled( WRITE );
            }
            else if ( queryType.equals( SCHEMA_WRITE_TEXT ) )
            {
                return profiled( SCHEMA_WRITE );
            }
            else
            {
                // should not happen but default to read write if so.
                return explained( READ_WRITE );
            }
        }
        else
        {
            if ( queryType.equals( READ_TEXT ) )
            {
                return query( READ_ONLY );
            }
            else if ( queryType.equals( READ_WRITE_TEXT ) )
            {
                return query( READ_WRITE );
            }
            else if ( queryType.equals( WRITE_TEXT ) )
            {
                return query( WRITE );
            }
            else if ( queryType.equals( SCHEMA_WRITE_TEXT ) )
            {
                return query( SCHEMA_WRITE );
            }
            else
            {
                // should not happen but default to read write if so.
                return query( READ_WRITE );
            }
        }
    }

    private ExecutionPlanDescription extractExecutionPlanDescription( Map<String,AnyValue> metadataMap )
    {
        if ( metadataMap.containsKey( PLAN ) )
        {
            return HttpExecutionPlanDescription.fromAnyValue( metadataMap.get( PLAN ) );
        }
        else if ( metadataMap.containsKey( PROFILE ) )
        {
            return HttpExecutionPlanDescription.fromAnyValue( metadataMap.get( PROFILE ) );
        }
        else
        {
            return new EmptyExecutionPlanDescription();
        }
    }
}
