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
package org.neo4j.server.http.cypher.format.output.eventsource;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.OutputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.server.http.cypher.TransactionHandle;
import org.neo4j.server.http.cypher.format.api.FailureEvent;
import org.neo4j.server.http.cypher.format.api.RecordEvent;
import org.neo4j.server.http.cypher.format.api.StatementEndEvent;
import org.neo4j.server.http.cypher.format.api.StatementStartEvent;
import org.neo4j.server.http.cypher.format.api.TransactionInfoEvent;
import org.neo4j.server.http.cypher.format.api.TransactionNotificationState;
import org.neo4j.server.http.cypher.format.input.json.InputStatement;
import org.neo4j.server.http.cypher.format.jolt.JoltCodec;
import org.neo4j.server.http.cypher.format.output.json.ResultDataContent;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.test.mockito.mock.Link;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.server.rest.domain.JsonHelper.jsonNode;
import static org.neo4j.test.mockito.mock.GraphMock.node;
import static org.neo4j.test.mockito.mock.GraphMock.path;
import static org.neo4j.test.mockito.mock.GraphMock.relationship;
import static org.neo4j.test.mockito.mock.Properties.properties;

public abstract class AbstractEventSourceJoltSerializerTest
{

    protected void writeStatementStart( LineDelimitedEventSourceJoltSerializer serializer, String... columns )
    {
        writeStatementStart( serializer, null, columns );
    }

    protected void writeStatementStart( LineDelimitedEventSourceJoltSerializer serializer, List<ResultDataContent> resultDataContents, String... columns )
    {
        serializer.writeStatementStart( new StatementStartEvent( null, Arrays.asList( columns ) ),
                                        new InputStatement( null, null, false, resultDataContents ) );
    }

    protected void writeRecord( LineDelimitedEventSourceJoltSerializer serializer, Map<String,?> row, String... columns )
    {
        serializer.writeRecord( new RecordEvent( Arrays.asList( columns ), row::get ) );
    }

    protected void writeStatementEnd( LineDelimitedEventSourceJoltSerializer serializer )
    {
        writeStatementEnd( serializer, null, Collections.emptyList() );
    }

    protected void writeStatementEnd( LineDelimitedEventSourceJoltSerializer serializer, ExecutionPlanDescription planDescription,
                                      Iterable<Notification> notifications )
    {
        QueryExecutionType queryExecutionType = null != planDescription ? QueryExecutionType.profiled( QueryExecutionType.QueryType.READ_WRITE )
                                                                        : QueryExecutionType.query( QueryExecutionType.QueryType.READ_WRITE );

        serializer.writeStatementEnd( new StatementEndEvent( queryExecutionType, null, planDescription, notifications ) );
    }

    protected void writeTransactionInfo( LineDelimitedEventSourceJoltSerializer serializer )
    {
        serializer.writeErrorWrapper();
        serializer.writeTransactionInfo( new TransactionInfoEvent( TransactionNotificationState.NO_TRANSACTION, null, -1 ) );
    }

    protected void writeTransactionInfo( LineDelimitedEventSourceJoltSerializer serializer, String commitUri )
    {
        serializer.writeErrorWrapper();
        serializer.writeTransactionInfo( new TransactionInfoEvent( TransactionNotificationState.NO_TRANSACTION, URI.create( commitUri ), -1 ) );
    }

    protected void writeError( LineDelimitedEventSourceJoltSerializer serializer, Status status, String message )
    {
        serializer.writeFailure( new FailureEvent( status, message ) );
    }

    protected Path mockPath( Map<String,Object> startNodeProperties, Map<String,Object> relationshipProperties, Map<String,Object> endNodeProperties )
    {
        Node startNode = node( 1, properties( startNodeProperties ) );
        Node endNode = node( 2, properties( endNodeProperties ) );
        Relationship relationship = relationship( 1, properties( relationshipProperties ), startNode, "RELATED", endNode );
        return path( startNode, Link.link( relationship, endNode ) );
    }

    protected Set<JsonNode> identifiersOf( JsonNode root )
    {
        Set<JsonNode> parentIds = new HashSet<>();
        for ( JsonNode id : root.get( "identifiers" ) )
        {
            parentIds.add( id );
        }
        return parentIds;
    }

    protected ExecutionPlanDescription mockedPlanDescription( String operatorType, Set<String> identifiers, Map<String,Object> args,
                                                              List<ExecutionPlanDescription> children )
    {
        ExecutionPlanDescription planDescription = mock( ExecutionPlanDescription.class );
        when( planDescription.getChildren() ).thenReturn( children );
        when( planDescription.getName() ).thenReturn( operatorType );
        when( planDescription.getArguments() ).thenReturn( args );
        when( planDescription.getIdentifiers() ).thenReturn( identifiers );
        return planDescription;
    }

    protected JsonNode wrapWithType( String sigil, Object value ) throws JsonParseException
    {
        return jsonNode( "{\"" + sigil + "\":\"" + value + "\"}" );
    }

    protected JsonNode assertIsPlanRoot( JsonNode result ) throws JsonParseException
    {
        JsonNode plan = result.get( "plan" );
        assertTrue( plan != null && plan.isObject(), "Expected plan to be an object" );

        JsonNode root = plan.get( "root" );
        assertTrue( root != null && root.isObject(), "Expected plan to be an object" );

        return root;
    }
}
