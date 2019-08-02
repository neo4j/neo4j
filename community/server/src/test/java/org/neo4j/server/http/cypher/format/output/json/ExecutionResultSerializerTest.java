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
package org.neo4j.server.http.cypher.format.output.json;

import org.codehaus.jackson.JsonNode;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.InputPosition;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.impl.notification.NotificationCode;
import org.neo4j.graphdb.spatial.Coordinate;
import org.neo4j.internal.helpers.collection.MapUtil;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.server.http.cypher.TxStateCheckerTestSupport;
import org.neo4j.server.http.cypher.format.api.FailureEvent;
import org.neo4j.server.http.cypher.format.api.RecordEvent;
import org.neo4j.server.http.cypher.format.api.StatementEndEvent;
import org.neo4j.server.http.cypher.format.api.StatementStartEvent;
import org.neo4j.server.http.cypher.format.api.TransactionInfoEvent;
import org.neo4j.server.http.cypher.format.api.TransactionNotificationState;
import org.neo4j.server.http.cypher.format.input.json.InputStatement;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.test.mockito.mock.GraphMock;
import org.neo4j.test.mockito.mock.Link;
import org.neo4j.test.mockito.mock.SpatialMocks;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.server.rest.domain.JsonHelper.jsonNode;
import static org.neo4j.server.rest.domain.JsonHelper.readJson;
import static org.neo4j.test.Property.property;
import static org.neo4j.test.mockito.mock.GraphMock.link;
import static org.neo4j.test.mockito.mock.GraphMock.node;
import static org.neo4j.test.mockito.mock.GraphMock.path;
import static org.neo4j.test.mockito.mock.GraphMock.relationship;
import static org.neo4j.test.mockito.mock.Properties.properties;
import static org.neo4j.test.mockito.mock.SpatialMocks.mockCartesian;
import static org.neo4j.test.mockito.mock.SpatialMocks.mockCartesian_3D;
import static org.neo4j.test.mockito.mock.SpatialMocks.mockWGS84;
import static org.neo4j.test.mockito.mock.SpatialMocks.mockWGS84_3D;

public class ExecutionResultSerializerTest extends TxStateCheckerTestSupport
{

    private static final Map<String,Object> NO_ARGS = Collections.emptyMap();
    private static final Set<String> NO_IDS = Collections.emptySet();
    private static final List<ExecutionPlanDescription> NO_PLANS = Collections.emptyList();

    private final ByteArrayOutputStream output = new ByteArrayOutputStream();
    private ExecutionResultSerializer serializer = getSerializerWith( output );

    @Test
    public void shouldSerializeResponseWithCommitUriOnly() throws Exception
    {
        // when
        serializer.writeTransactionInfo( new TransactionInfoEvent( TransactionNotificationState.NO_TRANSACTION, URI.create( "commit/uri/1" ) , -1 ) );

        // then
        String result = output.toString( UTF_8.name() );
        assertEquals( "{\"results\":[],\"errors\":[],\"commit\":\"commit/uri/1\"}", result );
    }

    @Test
    public void shouldSerializeResponseWithCommitUriAndResults() throws Exception
    {
        // given
        Map<String, Object> row = new HashMap<>();
        row.put( "column1", "value1" );
        row.put( "column2", "value2" );

        // when

        writeStatementStart( "column1", "column2" );
        writeRecord( row, "column1", "column2" );
        writeStatementEnd();

        serializer.writeTransactionInfo( new TransactionInfoEvent( TransactionNotificationState.NO_TRANSACTION, URI.create( "commit/uri/1" ) , -1 ) );

        // then
        String result = output.toString( UTF_8.name() );
        assertEquals( "{\"results\":[{\"columns\":[\"column1\",\"column2\"]," +
                      "\"data\":[{\"row\":[\"value1\",\"value2\"],\"meta\":[null,null]}]}],\"errors\":[],\"commit\":\"commit/uri/1\"}", result );
    }

    @Test
    public void shouldSerializeResponseWithResultsOnly() throws Exception
    {
        // given
        Map<String, Object> row = new HashMap<>();
        row.put( "column1", "value1" );
        row.put( "column2", "value2" );

        // when
        writeStatementStart( "column1", "column2" );
        writeRecord( row, "column1", "column2" );
        writeStatementEnd();
        writeTransactionInfo();

        // then
        String result = output.toString( UTF_8.name() );
        assertEquals( "{\"results\":[{\"columns\":[\"column1\",\"column2\"]," +
                      "\"data\":[{\"row\":[\"value1\",\"value2\"],\"meta\":[null,null]}]}],\"errors\":[]}", result );
    }

    @Test
    public void shouldSerializeResponseWithCommitUriAndResultsAndErrors() throws Exception
    {
        // given
        Map<String, Object> row = new HashMap<>();
        row.put( "column1", "value1" );
        row.put( "column2", "value2" );

        // when
        writeStatementStart( "column1", "column2" );
        writeRecord( row, "column1", "column2");
        writeStatementEnd();
        writeError( Status.Request.InvalidFormat, "cause1" );
        writeTransactionInfo("commit/uri/1");

        // then
        String result = output.toString( UTF_8.name() );
        assertEquals( "{\"results\":[{\"columns\":[\"column1\",\"column2\"]," +
                      "\"data\":[{\"row\":[\"value1\",\"value2\"],\"meta\":[null,null]}]}]," +
                      "\"errors\":[{\"code\":\"Neo.ClientError.Request.InvalidFormat\",\"message\":\"cause1\"}],\"commit\":\"commit/uri/1\"}",
                      result );
    }

    @Test
    public void shouldSerializeResponseWithResultsAndErrors() throws Exception
    {
        // given
        Map<String, Object> row = new HashMap<>();
        row.put( "column1", "value1" );
        row.put( "column2", "value2" );

        // when
        writeStatementStart( "column1", "column2" );
        writeRecord( row, "column1", "column2" );
        writeStatementEnd();
        writeError( Status.Request.InvalidFormat, "cause1" );
        writeTransactionInfo();

        // then
        String result = output.toString( UTF_8.name() );
        assertEquals( "{\"results\":[{\"columns\":[\"column1\",\"column2\"]," +
                      "\"data\":[{\"row\":[\"value1\",\"value2\"],\"meta\":[null,null]}]}]," +
                      "\"errors\":[{\"code\":\"Neo.ClientError.Request.InvalidFormat\",\"message\":\"cause1\"}]}",
                      result );
    }

    @Test
    public void shouldSerializeResponseWithCommitUriAndErrors() throws Exception
    {

        // when
        writeError( Status.Request.InvalidFormat, "cause1" );
        writeTransactionInfo("commit/uri/1");

        // then
        String result = output.toString( UTF_8.name() );
        assertEquals( "{\"results\":[],\"errors\":[{\"code\":\"Neo.ClientError.Request.InvalidFormat\"," +
                      "\"message\":\"cause1\"}],\"commit\":\"commit/uri/1\"}", result );
    }

    @Test
    public void shouldSerializeResponseWithErrorsOnly() throws Exception
    {
        // when
        writeError( Status.Request.InvalidFormat, "cause1" );
        writeTransactionInfo();

        // then
        String result = output.toString( UTF_8.name() );
        assertEquals(
                "{\"results\":[],\"errors\":[{\"code\":\"Neo.ClientError.Request.InvalidFormat\",\"message\":\"cause1\"}]}",
                result );
    }

    @Test
    public void shouldSerializeResponseWithNoCommitUriResultsOrErrors() throws Exception
    {

        // when
        writeTransactionInfo();

        // then
        String result = output.toString( UTF_8.name() );
        assertEquals( "{\"results\":[],\"errors\":[]}", result );
    }

    @Test
    public void shouldSerializeResponseWithMultipleResultRows() throws Exception
    {
        // given
        Map<String, Object> row1 = new HashMap<>();
        row1.put( "column1", "value1" );
        row1.put( "column2", "value2" );

        Map<String, Object> row2 = new HashMap<>();
        row2.put( "column1", "value3" );
        row2.put( "column2", "value4" );

        // when
        writeStatementStart( "column1", "column2" );
        writeRecord( row1, "column1", "column2");
        writeRecord( row2, "column1", "column2");
        writeStatementEnd();
        writeTransactionInfo();

        // then
        String result = output.toString( UTF_8.name() );
        assertEquals( "{\"results\":[{\"columns\":[\"column1\",\"column2\"]," +
                      "\"data\":[{\"row\":[\"value1\",\"value2\"],\"meta\":[null,null]}," +
                      "{\"row\":[\"value3\",\"value4\"],\"meta\":[null,null]}]}]," +
                      "\"errors\":[]}", result );
    }

    @Test
    public void shouldSerializeResponseWithMultipleResults() throws Exception
    {
        // given
        Map<String, Object> row1 = new HashMap<>();
        row1.put( "column1", "value1" );
        row1.put( "column2", "value2" );

        Map<String, Object> row2 = new HashMap<>();
        row2.put( "column3", "value3" );
        row2.put( "column4", "value4" );

        // when
        writeStatementStart( "column1", "column2" );
        writeRecord( row1, "column1", "column2");
        writeStatementEnd();
        writeStatementStart( "column3", "column4" );
        writeRecord( row2, "column3", "column4" );
        writeStatementEnd();
        writeTransactionInfo();

        // then
        String result = output.toString( UTF_8.name() );
        assertEquals( "{\"results\":[" +
                "{\"columns\":[\"column1\",\"column2\"],\"data\":[{\"row\":[\"value1\",\"value2\"],\"meta\":[null,null]}]}," +
                "{\"columns\":[\"column3\",\"column4\"],\"data\":[{\"row\":[\"value3\",\"value4\"],\"meta\":[null,null]}]}]," +
                "\"errors\":[]}", result );
    }

    @Test
    public void shouldSerializeNodeAsMapOfProperties() throws Exception
    {
        // given
        Map<String, Object> row = new HashMap<>();
        row.put( "node", node( 1, properties(
                property( "a", 12 ),
                property( "b", true ),
                property( "c", new int[]{1, 0, 1, 2} ),
                property( "d", new byte[]{1, 0, 1, 2} ),
                property( "e", new String[]{"a", "b", "ääö"} ) ) ) );

        // when
        writeStatementStart( "node");
        writeRecord( row, "node" );
        writeStatementEnd();
        writeTransactionInfo();

        // then
        String result = output.toString( UTF_8.name() );
        assertEquals( "{\"results\":[{\"columns\":[\"node\"]," +
                      "\"data\":[{\"row\":[{\"a\":12,\"b\":true,\"c\":[1,0,1,2],\"d\":[1,0,1,2],\"e\":[\"a\",\"b\",\"ääö\"]}]," +
                      "\"meta\":[{\"id\":1,\"type\":\"node\",\"deleted\":false}]}]}]," +
                      "\"errors\":[]}", result );
    }

    @Test
    public void shouldSerializeNestedEntities() throws Exception
    {
        // given
        Node a = node( 1, properties( property( "foo", 12 ) ) );
        Node b = node( 2, properties( property( "bar", false ) ) );
        Relationship r = relationship( 1, properties( property( "baz", "quux" ) ), a, "FRAZZLE", b );
        Map<String, Object> row = new HashMap<>();
        row.put( "nested", new TreeMap<>( map( "node", a, "edge", r, "path", path( a, link( r, b ) ) ) ) );

        // when
        writeStatementStart( "nested");
        writeRecord( row, "nested" );
        writeStatementEnd();
        writeTransactionInfo();

        // then
        String result = output.toString( UTF_8.name() );
        assertEquals( "{\"results\":[{\"columns\":[\"nested\"]," +
                "\"data\":[{\"row\":[{\"edge\":{\"baz\":\"quux\"},\"node\":{\"foo\":12}," +
                "\"path\":[{\"foo\":12},{\"baz\":\"quux\"},{\"bar\":false}]}]," +
                "\"meta\":[{\"id\":1,\"type\":\"relationship\",\"deleted\":false}," +
                "{\"id\":1,\"type\":\"node\",\"deleted\":false},[{\"id\":1,\"type\":\"node\",\"deleted\":false}," +
                "{\"id\":1,\"type\":\"relationship\",\"deleted\":false},{\"id\":2,\"type\":\"node\",\"deleted\":false}]]}]}]," +
                "\"errors\":[]}", result );
    }

    @Test
    public void shouldSerializePathAsListOfMapsOfProperties() throws Exception
    {
        // given
        Map<String, Object> row = new HashMap<>();
        row.put( "path", mockPath( map( "key1", "value1" ), map( "key2", "value2" ), map( "key3", "value3" ) ) );

        // when
        writeStatementStart( "path");
        writeRecord( row, "path" );
        writeStatementEnd();
        writeTransactionInfo();

        // then
        String result = output.toString( UTF_8.name() );
        assertEquals( "{\"results\":[{\"columns\":[\"path\"]," +
                "\"data\":[{\"row\":[[{\"key1\":\"value1\"},{\"key2\":\"value2\"},{\"key3\":\"value3\"}]]," +
                "\"meta\":[[{\"id\":1,\"type\":\"node\",\"deleted\":false}," +
                "{\"id\":1,\"type\":\"relationship\",\"deleted\":false},{\"id\":2,\"type\":\"node\",\"deleted\":false}]]}]}]," +
                "\"errors\":[]}", result );
    }

    @Test
    public void shouldSerializePointsAsListOfMapsOfProperties() throws Exception
    {
        // given
        Map<String, Object> row1 = new HashMap<>();
        row1.put( "geom", SpatialMocks.mockPoint( 12.3, 45.6, mockWGS84() ) );
        Map<String, Object> row2 = new HashMap<>();
        row2.put( "geom", SpatialMocks.mockPoint( 123, 456, mockCartesian() ) );
        Map<String, Object> row3 = new HashMap<>();
        row3.put( "geom", SpatialMocks.mockPoint( 12.3, 45.6, 78.9, mockWGS84_3D() )  );
        Map<String, Object> row4 = new HashMap<>();
        row4.put(  "geom", SpatialMocks.mockPoint( 123, 456, 789, mockCartesian_3D() ) );

        // when
        writeStatementStart( "geom");
        writeRecord( row1, "geom" );
        writeRecord( row2, "geom" );
        writeRecord( row3, "geom" );
        writeRecord( row4, "geom" );
        writeStatementEnd();
        writeTransactionInfo();

        // then
        String result = output.toString( UTF_8.name() );
        assertEquals( "{\"results\":[{\"columns\":[\"geom\"],\"data\":[" +
                      "{\"row\":[{\"type\":\"Point\",\"coordinates\":[12.3,45.6],\"crs\":" +
                        "{\"srid\":4326,\"name\":\"WGS-84\",\"type\":\"link\",\"properties\":" +
                          "{\"href\":\"http://spatialreference.org/ref/epsg/4326/ogcwkt/\",\"type\":\"ogcwkt\"}" +
                        "}}],\"meta\":[{\"type\":\"point\"}]}," +
                      "{\"row\":[{\"type\":\"Point\",\"coordinates\":[123.0,456.0],\"crs\":" +
                        "{\"srid\":7203,\"name\":\"cartesian\",\"type\":\"link\",\"properties\":" +
                          "{\"href\":\"http://spatialreference.org/ref/sr-org/7203/ogcwkt/\",\"type\":\"ogcwkt\"}" +
                        "}}],\"meta\":[{\"type\":\"point\"}]}," +
                      "{\"row\":[{\"type\":\"Point\",\"coordinates\":[12.3,45.6,78.9],\"crs\":" +
                        "{\"srid\":4979,\"name\":\"WGS-84-3D\",\"type\":\"link\",\"properties\":" +
                          "{\"href\":\"http://spatialreference.org/ref/epsg/4979/ogcwkt/\",\"type\":\"ogcwkt\"}" +
                        "}}],\"meta\":[{\"type\":\"point\"}]}," +
                      "{\"row\":[{\"type\":\"Point\",\"coordinates\":[123.0,456.0,789.0],\"crs\":" +
                        "{\"srid\":9157,\"name\":\"cartesian-3D\",\"type\":\"link\",\"properties\":" +
                          "{\"href\":\"http://spatialreference.org/ref/sr-org/9157/ogcwkt/\",\"type\":\"ogcwkt\"}" +
                        "}}],\"meta\":[{\"type\":\"point\"}]}" +
                        "]}],\"errors\":[]}",
                result );
    }

    @Test
    public void shouldSerializeTemporalAsListOfMapsOfProperties() throws Exception
    {
        // given
        Map<String, Object> row1 = new HashMap<>();
        row1.put( "temporal", LocalDate.of( 2018, 3, 12 ) );
        Map<String, Object> row2 = new HashMap<>();
        row2.put( "temporal", ZonedDateTime.of( 2018, 3, 12, 13, 2, 10, 10, ZoneId.of( "UTC+1" ) ) );
        Map<String, Object> row3 = new HashMap<>();
        row3.put( "temporal", OffsetTime.of( 12, 2, 4, 71, ZoneOffset.UTC ) );
        Map<String, Object> row4 = new HashMap<>();
        row4.put( "temporal", LocalDateTime.of( 2018, 3, 12, 13, 2, 10, 10 ) );
        Map<String, Object> row5 = new HashMap<>();
        row5.put( "temporal", LocalTime.of( 13, 2, 10, 10 ) );
        Map<String, Object> row6 = new HashMap<>();
        row6.put( "temporal", Duration.of( 12, ChronoUnit.HOURS ) );

        // when
        writeStatementStart( "temporal");
        writeRecord( row1, "temporal" );
        writeRecord( row2, "temporal" );
        writeRecord( row3, "temporal" );
        writeRecord( row4, "temporal" );
        writeRecord( row5, "temporal" );
        writeRecord( row6, "temporal" );
        writeStatementEnd();

        serializer.writeTransactionInfo( new TransactionInfoEvent( TransactionNotificationState.NO_TRANSACTION, null , -1 ) );

        // then
        String result = output.toString( UTF_8.name() );
        assertEquals( "{\"results\":[{\"columns\":[\"temporal\"],\"data\":[" +
                        "{\"row\":[\"2018-03-12\"],\"meta\":[{\"type\":\"date\"}]}," +
                        "{\"row\":[\"2018-03-12T13:02:10.000000010+01:00[UTC+01:00]\"],\"meta\":[{\"type\":\"datetime\"}]}," +
                        "{\"row\":[\"12:02:04.000000071Z\"],\"meta\":[{\"type\":\"time\"}]}," +
                        "{\"row\":[\"2018-03-12T13:02:10.000000010\"],\"meta\":[{\"type\":\"localdatetime\"}]}," +
                        "{\"row\":[\"13:02:10.000000010\"],\"meta\":[{\"type\":\"localtime\"}]}," +
                        "{\"row\":[\"PT12H\"],\"meta\":[{\"type\":\"duration\"}]}" +
                        "]}],\"errors\":[]}",
                result );
    }

    @Test
    public void shouldErrorWhenSerializingUnknownGeometryType() throws Exception
    {
        // given
        List<Coordinate> points = new ArrayList<>();
        points.add( new Coordinate( 1, 2 ) );
        points.add( new Coordinate( 2, 3 ) );

        Map<String, Object> row = new HashMap<>();
        row.put("geom", SpatialMocks.mockGeometry( "LineString", points, mockCartesian() ) );

        // when
        try
        {
            writeStatementStart( "geom");
            writeRecord( row, "geom" );

            fail();
        }
        catch ( RuntimeException e )
        {
            writeError( Status.Statement.ExecutionFailed, e.getMessage() );
            writeTransactionInfo();
        }

        // then
        String result = output.toString( UTF_8.name() );
        assertThat( result, startsWith(
                "{\"results\":[{\"columns\":[\"geom\"],\"data\":[" + "{\"row\":[{\"type\":\"LineString\",\"coordinates\":[[1.0,2.0],[2.0,3.0]],\"crs\":" +
                        "{\"srid\":7203,\"name\":\"cartesian\",\"type\":\"link\",\"properties\":" +
                        "{\"href\":\"http://spatialreference.org/ref/sr-org/7203/ogcwkt/\",\"type\":\"ogcwkt\"}}}],\"meta\":[]}]}]," +
                        "\"errors\":[{\"code\":\"Neo.DatabaseError.Statement.ExecutionFailed\"," +
                        "\"message\":\"Unsupported Geometry type: type=MockGeometry, value=LineString\"" ) );
    }

    @Test
    public void shouldProduceWellFormedJsonEvenIfResultIteratorThrowsExceptionOnNext() throws Exception
    {
        // given
        Map<String, Object> row = new HashMap<>();
        row.put( "column1", "value1" );
        row.put( "column2", "value2" );

        RecordEvent recordEvent = mock(RecordEvent.class);
        when(recordEvent.getValue( any() )).thenThrow( new RuntimeException( "Stuff went wrong!" ) );
        when( recordEvent.getColumns() ).thenReturn( Arrays.asList( "column1", "column2" ) );

        // when
        try
        {
            writeStatementStart(  "column1", "column2");
            writeRecord( row,  "column1", "column2");
            serializer.writeRecord(recordEvent);
            fail( "should have thrown exception" );
        }
        catch ( RuntimeException e )
        {
            writeError( Status.Statement.ExecutionFailed, e.getMessage() );
            writeTransactionInfo();
        }

        // then
        String result = output.toString( UTF_8.name() );
        assertEquals(
                "{\"results\":[{\"columns\":[\"column1\",\"column2\"]," +
                        "\"data\":[{\"row\":[\"value1\",\"value2\"],\"meta\":[null,null]},{\"row\":[],\"meta\":[]}]}]," +
                        "\"errors\":[{\"code\":\"Neo.DatabaseError.Statement.ExecutionFailed\"," +
                        "\"message\":\"Stuff went wrong!\"}]}", result );
    }

    @Test
    public void shouldProduceResultStreamWithGraphEntries() throws Exception
    {
        // given
        Node[] node = {
                node( 0, properties( property( "name", "node0" ) ), "Node" ),
                node( 1, properties( property( "name", "node1" ) ) ),
                node( 2, properties( property( "name", "node2" ) ), "This", "That" ),
                node( 3, properties( property( "name", "node3" ) ), "Other" )};
        Relationship[] rel = {
                relationship( 0, node[0], "KNOWS", node[1], property( "name", "rel0" ) ),
                relationship( 1, node[2], "LOVES", node[3], property( "name", "rel1" ) )};

        Map<String, Object> resultRow1 = new HashMap<>();
        resultRow1.put( "node", node[0] );
        resultRow1.put( "rel", rel[0] );

        Map<String, Object> resultRow2 = new HashMap<>();
        resultRow2.put( "node", node[2] );
        resultRow2.put( "rel", rel[1] );

        // when
        writeStatementStart( Arrays.asList( ResultDataContent.row, ResultDataContent.graph ), "node", "rel" );
        writeRecord( resultRow1, "node", "rel" );
        writeRecord( resultRow2, "node", "rel" );
        writeStatementEnd();
        writeTransactionInfo();

        // then
        String result = output.toString( UTF_8.name() );

        // Nodes and relationships form sets, so we cannot test for a fixed string, since we don't know the order.
        String node0 = "{\"id\":\"0\",\"labels\":[\"Node\"],\"properties\":{\"name\":\"node0\"}}";
        String node1 = "{\"id\":\"1\",\"labels\":[],\"properties\":{\"name\":\"node1\"}}";
        String node2 = "{\"id\":\"2\",\"labels\":[\"This\",\"That\"],\"properties\":{\"name\":\"node2\"}}";
        String node3 = "{\"id\":\"3\",\"labels\":[\"Other\"],\"properties\":{\"name\":\"node3\"}}";
        String rel0 = "\"relationships\":[{\"id\":\"0\",\"type\":\"KNOWS\"," +
                "\"startNode\":\"0\",\"endNode\":\"1\",\"properties\":{\"name\":\"rel0\"}}]}";
        String rel1 = "\"relationships\":[{\"id\":\"1\",\"type\":\"LOVES\"," +
                "\"startNode\":\"2\",\"endNode\":\"3\",\"properties\":{\"name\":\"rel1\"}}]}";
        String row0 = "{\"row\":[{\"name\":\"node0\"},{\"name\":\"rel0\"}]," +
                "\"meta\":[{\"id\":0,\"type\":\"node\",\"deleted\":false}," +
                "{\"id\":0,\"type\":\"relationship\",\"deleted\":false}],\"graph\":{\"nodes\":[";
        String row1 = "{\"row\":[{\"name\":\"node2\"},{\"name\":\"rel1\"}]," +
                "\"meta\":[{\"id\":2,\"type\":\"node\",\"deleted\":false}," +
                "{\"id\":1,\"type\":\"relationship\",\"deleted\":false}],\"graph\":{\"nodes\":[";
        int n0 = result.indexOf( node0 );
        int n1 = result.indexOf( node1 );
        int n2 = result.indexOf( node2 );
        int n3 = result.indexOf( node3 );
        int r0 = result.indexOf( rel0 );
        int r1 = result.indexOf( rel1 );
        int row0Index = result.indexOf( row0 );
        int row1Index = result.indexOf( row1 );
        assertTrue( "result should contain row0", row0Index > 0 );
        assertTrue( "result should contain row1 after row0", row1Index > row0Index );
        assertTrue( "result should contain node0 after row0", n0 > row0Index );
        assertTrue( "result should contain node1 after row0", n1 > row0Index );
        assertTrue( "result should contain node2 after row1", n2 > row1Index );
        assertTrue( "result should contain node3 after row1", n3 > row1Index );
        assertTrue( "result should contain rel0 after node0 and node1", r0 > n0 && r0 > n1 );
        assertTrue( "result should contain rel1 after node2 and node3", r1 > n2 && r1 > n3 );
    }

    @Test
    public void shouldProduceResultStreamWithLegacyRestFormat() throws Exception
    {
        // given
        Node[] node = {
                node( 0, properties( property( "name", "node0" ) ) ),
                node( 1, properties( property( "name", "node1" ) ) ),
                node( 2, properties( property( "name", "node2" ) ) )};
        Relationship[] rel = {
                relationship( 0, node[0], "KNOWS", node[1], property( "name", "rel0" ) ),
                relationship( 1, node[2], "LOVES", node[1], property( "name", "rel1" ) )};
        Path path = GraphMock.path( node[0], link( rel[0], node[1] ), link( rel[1], node[2] ) );

        serializer = getSerializerWith( output, "http://base.uri/" );

        Map<String, Object> resultRow = new HashMap<>();
        resultRow.put( "node", node[0] );
        resultRow.put( "rel", rel[0] );
        resultRow.put( "path", path );
        resultRow.put( "map", map( "n1", node[1], "r1", rel[1] ) );

        // when
        writeStatementStart( Collections.singletonList( ResultDataContent.rest ), "node", "rel", "path", "map" );
        writeRecord( resultRow, "node", "rel", "path", "map" );
        writeStatementEnd();
        writeTransactionInfo();

        // then
        String result = output.toString( UTF_8.name() );
        JsonNode json = jsonNode( result );
        Map<String, Integer> columns = new HashMap<>();
        int col = 0;
        JsonNode results = json.get( "results" ).get( 0 );
        for ( JsonNode column : results.get( "columns" ) )
        {
            columns.put( column.getTextValue(), col++ );
        }
        JsonNode row = results.get( "data" ).get( 0 ).get( "rest" );
        JsonNode jsonNode = row.get( columns.get( "node" ) );
        JsonNode jsonRel = row.get( columns.get( "rel" ) );
        JsonNode jsonPath = row.get( columns.get( "path" ) );
        JsonNode jsonMap = row.get( columns.get( "map" ) );
        assertEquals( "http://base.uri/node/0", jsonNode.get( "self" ).getTextValue() );
        assertEquals( "http://base.uri/relationship/0", jsonRel.get( "self" ).getTextValue() );
        assertEquals( 2, jsonPath.get( "length" ).getNumberValue() );
        assertEquals( "http://base.uri/node/0", jsonPath.get( "start" ).getTextValue() );
        assertEquals( "http://base.uri/node/2", jsonPath.get( "end" ).getTextValue() );
        assertEquals( "http://base.uri/node/1", jsonMap.get( "n1" ).get( "self" ).getTextValue() );
        assertEquals( "http://base.uri/relationship/1", jsonMap.get( "r1" ).get( "self" ).getTextValue() );
    }

    @Test
    public void shouldProduceResultStreamWithLegacyRestFormatAndNestedMaps() throws Exception
    {
        // given
        serializer = getSerializerWith( output, "http://base.uri/" );

        Map<String, Object> resultRow = new HashMap<>();
        // RETURN {one:{two:['wait for it...', {three: 'GO!'}]}}
        resultRow.put( "map", map("one", map( "two", asList("wait for it...", map("three", "GO!") ) ) ) );

        // when
        writeStatementStart( Collections.singletonList( ResultDataContent.rest ), "map" );
        writeRecord( resultRow, "map" );
        writeStatementEnd();
        writeTransactionInfo();

        // then
        String result = output.toString( UTF_8.name() );
        JsonNode json = jsonNode(result);
        Map<String, Integer> columns = new HashMap<>();
        int col = 0;
        JsonNode results = json.get( "results" ).get( 0 );
        for ( JsonNode column : results.get( "columns" ) )
        {
            columns.put( column.getTextValue(), col++ );
        }
        JsonNode row = results.get( "data" ).get( 0 ).get( "rest" );
        JsonNode jsonMap = row.get( columns.get( "map" ) );
        assertEquals( "wait for it...", jsonMap.get( "one" ).get( "two" ).get( 0 ).asText() );
        assertEquals( "GO!", jsonMap.get( "one" ).get( "two" ).get( 1 ).get( "three" ).asText() );
    }

    @Test
    public void shouldSerializePlanWithoutChildButAllKindsOfSupportedArguments() throws Exception
    {
        // given
        serializer = getSerializerWith( output, "http://base.uri/" );

        String operatorType = "Ich habe einen Plan";

        // This is the full set of types that we allow in plan arguments

        Map<String, Object> args = new HashMap<>();
        args.put( "string", "A String" );
        args.put( "bool", true );
        args.put( "number", 1 );
        args.put( "double", 2.3 );
        args.put( "listOfInts", asList(1, 2, 3) );
        args.put( "listOfListOfInts", asList( asList(1, 2, 3) ) );

        ExecutionPlanDescription planDescription = mockedPlanDescription( operatorType, NO_IDS, args, NO_PLANS );

        // when

        writeStatementStart( Collections.singletonList( ResultDataContent.rest ) );
        writeRecord( Collections.emptyMap());
        writeStatementEnd(planDescription, Collections.emptyList());
        writeTransactionInfo();

        String resultString = output.toString( UTF_8.name() );

        // then
        assertIsPlanRoot( resultString );
        Map<String, ?> rootMap = planRootMap( resultString );

        assertEquals( asSet( "operatorType", "identifiers", "children", "string", "bool", "number", "double",
                "listOfInts", "listOfListOfInts" ), rootMap.keySet() );

        assertEquals( operatorType, rootMap.get( "operatorType" ) );
        assertEquals( args.get( "string" ), rootMap.get( "string" ) );
        assertEquals( args.get( "bool" ), rootMap.get( "bool" ) );
        assertEquals( args.get( "number" ), rootMap.get( "number" ) );
        assertEquals( args.get( "double" ), rootMap.get( "double" ) );
        assertEquals( args.get( "listOfInts" ), rootMap.get( "listOfInts" ) );
        assertEquals( args.get( "listOfListOfInts" ), rootMap.get( "listOfListOfInts" ) );
    }

    @Test
    public void shouldSerializePlanWithoutChildButWithIdentifiers() throws Exception
    {
        // given
        serializer = getSerializerWith( output, "http://base.uri/" );

        String operatorType = "Ich habe einen Plan";
        String id1 = "id1";
        String id2 = "id2";
        String id3 = "id3";

        // This is the full set of types that we allow in plan arguments
        ExecutionPlanDescription planDescription = mockedPlanDescription( operatorType, asSet( id1, id2, id3 ), NO_ARGS, NO_PLANS );

        // when
        writeStatementStart( Collections.singletonList( ResultDataContent.rest ) );
        writeRecord( Collections.emptyMap());
        writeStatementEnd(planDescription, Collections.emptyList());
        writeTransactionInfo();

        String resultString = output.toString( UTF_8.name() );

        // then
        assertIsPlanRoot( resultString );
        Map<String,?> rootMap = planRootMap( resultString );

        assertEquals( asSet( "operatorType", "identifiers", "children" ), rootMap.keySet() );

        assertEquals( operatorType, rootMap.get( "operatorType" ) );
        assertEquals( asList( id2, id1, id3 ), rootMap.get( "identifiers" ) );
    }

    @Test
    public void shouldSerializePlanWithChildren() throws Exception
    {
        // given
        serializer = getSerializerWith( output, "http://base.uri/" );

        String leftId = "leftId";
        String rightId = "rightId";
        String parentId = "parentId";

        ExecutionPlanDescription left = mockedPlanDescription( "child", asSet( leftId ), MapUtil.map( "id", 1 ), NO_PLANS );
        ExecutionPlanDescription right = mockedPlanDescription( "child", asSet( rightId ), MapUtil.map( "id", 2 ), NO_PLANS );
        ExecutionPlanDescription parent =
                mockedPlanDescription( "parent", asSet( parentId ), MapUtil.map( "id", 0 ), asList( left, right ) );

        // when
        writeStatementStart( Collections.singletonList( ResultDataContent.rest ) );
        writeRecord( Collections.emptyMap());
        writeStatementEnd(parent, Collections.emptyList());
        writeTransactionInfo();

        // then
        String result = output.toString( UTF_8.name() );
        JsonNode root = assertIsPlanRoot( result );

        assertEquals( "parent", root.get( "operatorType" ).getTextValue() );
        assertEquals( 0, root.get( "id" ).asLong() );
        assertEquals( asSet( parentId ), identifiersOf( root ) );

        Set<Integer> childIds = new HashSet<>();
        Set<Set<String>> identifiers = new HashSet<>();
        for ( JsonNode child : root.get( "children" ) )
        {
            assertTrue( "Expected object", child.isObject() );
            assertEquals( "child", child.get( "operatorType" ).getTextValue() );
            identifiers.add( identifiersOf( child ) );
            childIds.add( child.get( "id" ).asInt() );
        }
        assertEquals( asSet( 1, 2 ), childIds );
        assertEquals( asSet( asSet( leftId ), asSet( rightId ) ), identifiers );
    }

    @Test
    public void shouldReturnNotifications() throws IOException
    {
        // given
        Notification notification = NotificationCode.CARTESIAN_PRODUCT.notification( new InputPosition( 1, 2, 3 ) );
        List<Notification> notifications = Collections.singletonList( notification );

        Map<String, Object> row = new HashMap<>();
        row.put( "column1", "value1" );
        row.put( "column2", "value2" );

        // when
        writeStatementStart( "column1", "column2" );
        writeRecord( row, "column1", "column2" );
        writeStatementEnd(null, notifications);
        writeTransactionInfo("commit/uri/1");

        // then
        String result = output.toString( UTF_8.name() );

        assertEquals(
                "{\"results\":[{\"columns\":[\"column1\",\"column2\"]," +
                        "\"data\":[{\"row\":[\"value1\",\"value2\"],\"meta\":[null,null]}]}],\"notifications\":[{\"code\":\"Neo" +
                        ".ClientNotification.Statement.CartesianProductWarning\",\"severity\":\"WARNING\",\"title\":\"This " +
                        "query builds a cartesian product between disconnected patterns.\",\"description\":\"If a " +
                        "part of a query contains multiple disconnected patterns, this will build a cartesian product" +
                        " between all those parts. This may produce a large amount of data and slow down query " +
                        "processing. While occasionally intended, it may often be possible to reformulate the query " +
                        "that avoids the use of this cross product, perhaps by adding a relationship between the " +
                        "different parts or by using OPTIONAL MATCH\",\"position\":{\"offset\":1,\"line\":2," +
                        "\"column\":3}}],\"errors\":[],\"commit\":\"commit/uri/1\"}", result );
    }

    @Test
    public void shouldNotReturnNotificationsWhenEmptyNotifications() throws IOException
    {
        // given
        Map<String, Object> row = new HashMap<>();
        row.put( "column1", "value1" );
        row.put( "column2", "value2" );

        // when
        writeStatementStart( "column1", "column2" );
        writeRecord( row, "column1", "column2" );
        writeStatementEnd(null, Collections.emptyList());
        writeTransactionInfo("commit/uri/1");

        // then
        String result = output.toString( UTF_8.name() );

        assertEquals(
                "{\"results\":[{\"columns\":[\"column1\",\"column2\"]," +
                        "\"data\":[{\"row\":[\"value1\",\"value2\"],\"meta\":[null,null]}]}],\"errors\":[],\"commit\":\"commit/uri/1\"}", result );
    }

    @Test
    public void shouldNotReturnPositionWhenEmptyPosition() throws IOException
    {
        // given
        Map<String, Object> row = new HashMap<>();
        row.put( "column1", "value1" );
        row.put( "column2", "value2" );

        Notification notification = NotificationCode.CARTESIAN_PRODUCT.notification( InputPosition.empty );

        List<Notification> notifications = Collections.singletonList( notification );

        // when
        writeStatementStart( "column1", "column2" );
        writeRecord( row, "column1", "column2" );
        writeStatementEnd(null, notifications);
        writeTransactionInfo("commit/uri/1");

        // then
        String result = output.toString( UTF_8.name() );

        assertEquals(
                "{\"results\":[{\"columns\":[\"column1\",\"column2\"]," +
                        "\"data\":[{\"row\":[\"value1\",\"value2\"],\"meta\":[null,null]}]}],\"notifications\":[{\"code\":\"Neo" +
                        ".ClientNotification.Statement.CartesianProductWarning\",\"severity\":\"WARNING\",\"title\":\"This " +
                        "query builds a cartesian product between disconnected patterns.\",\"description\":\"If a " +
                        "part of a query contains multiple disconnected patterns, this will build a cartesian product" +
                        " between all those parts. This may produce a large amount of data and slow down query " +
                        "processing. While occasionally intended, it may often be possible to reformulate the query " +
                        "that avoids the use of this cross product, perhaps by adding a relationship between the " +
                        "different parts or by using OPTIONAL MATCH\"}],\"errors\":[],\"commit\":\"commit/uri/1\"}", result );
    }

    private ExecutionResultSerializer getSerializerWith( OutputStream output )
    {
        return getSerializerWith( output, null );
    }

    private ExecutionResultSerializer getSerializerWith( OutputStream output, String uri )
    {
        return new ExecutionResultSerializer( output, uri == null ? null : URI.create( uri ), CONTAINER );
    }

    private void writeStatementStart( String... columns )
    {
        writeStatementStart( null, columns );
    }

    private void writeStatementStart( List<ResultDataContent> resultDataContents, String... columns )
    {
        serializer.writeStatementStart( new StatementStartEvent( null, Arrays.asList( columns ) ),
                new InputStatement( null, null, false, resultDataContents ) );
    }

    private void writeRecord( Map<String,Object> row, String... columns )
    {
        serializer.writeRecord( new RecordEvent( Arrays.asList( columns ), row::get ) );
    }

    private void writeStatementEnd()
    {
        writeStatementEnd( null, Collections.emptyList() );
    }

    private void writeStatementEnd( ExecutionPlanDescription planDescription, Iterable<Notification> notifications )
    {
        QueryExecutionType queryExecutionType = null != planDescription ? QueryExecutionType.profiled( QueryExecutionType.QueryType.READ_WRITE )
                                                                        : QueryExecutionType.query( QueryExecutionType.QueryType.READ_WRITE );

        serializer.writeStatementEnd( new StatementEndEvent( queryExecutionType, null, planDescription, notifications ) );
    }

    private void writeTransactionInfo()
    {
        serializer.writeTransactionInfo( new TransactionInfoEvent( TransactionNotificationState.NO_TRANSACTION, null, -1 ) );
    }

    private void writeTransactionInfo( String commitUri )
    {
        serializer.writeTransactionInfo( new TransactionInfoEvent( TransactionNotificationState.NO_TRANSACTION, URI.create( commitUri ), -1 ) );
    }

    private void writeError( Status status, String message )
    {
        serializer.writeFailure( new FailureEvent( status, message ) );
    }

    private static Path mockPath( Map<String,Object> startNodeProperties, Map<String,Object> relationshipProperties, Map<String,Object> endNodeProperties )
    {
        Node startNode = node( 1, properties( startNodeProperties ) );
        Node endNode = node( 2, properties( endNodeProperties ) );
        Relationship relationship = relationship( 1, properties( relationshipProperties ), startNode, "RELATED", endNode );
        return path( startNode, Link.link( relationship, endNode ) );
    }

    private Set<String> identifiersOf( JsonNode root )
    {
        Set<String> parentIds = new HashSet<>();
        for ( JsonNode id : root.get( "identifiers" ) )
        {
            parentIds.add( id.asText() );
        }
        return parentIds;
    }

    private ExecutionPlanDescription mockedPlanDescription( String operatorType, Set<String> identifiers, Map<String,Object> args,
            List<ExecutionPlanDescription> children )
    {
        ExecutionPlanDescription planDescription = mock( ExecutionPlanDescription.class );
        when( planDescription.getChildren() ).thenReturn( children );
        when( planDescription.getName() ).thenReturn( operatorType );
        when( planDescription.getArguments() ).thenReturn( args );
        when( planDescription.getIdentifiers() ).thenReturn( identifiers );
        return planDescription;
    }

    private JsonNode assertIsPlanRoot( String result ) throws JsonParseException
    {
        JsonNode json = jsonNode( result );
        JsonNode results = json.get( "results" ).get( 0 );

        JsonNode plan = results.get( "plan" );
        assertTrue( "Expected plan to be an object", plan != null && plan.isObject() );

        JsonNode root = plan.get( "root" );
        assertTrue( "Expected plan to be an object", root != null && root.isObject() );

        return root;
    }

    @SuppressWarnings( "unchecked" )
    private Map<String,?> planRootMap( String resultString ) throws JsonParseException
    {
        Map<String,?> resultMap = (Map<String,?>) ((List<?>) ((Map<String,?>) (readJson( resultString ))).get( "results" )).get( 0 );
        Map<String,?> planMap = (Map<String,?>) (resultMap.get( "plan" ));
        return (Map<String,?>) (planMap.get( "root" ));
    }
}
