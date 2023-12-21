/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.server.http.cypher.format.output.json;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.server.rest.domain.JsonHelper.jsonNode;
import static org.neo4j.server.rest.domain.JsonHelper.readJson;
import static org.neo4j.test.mockito.mock.GraphMock.link;
import static org.neo4j.test.mockito.mock.GraphMock.path;
import static org.neo4j.test.mockito.mock.SpatialMocks.mockCartesian;
import static org.neo4j.test.mockito.mock.SpatialMocks.mockCartesian_3D;
import static org.neo4j.test.mockito.mock.SpatialMocks.mockWGS84;
import static org.neo4j.test.mockito.mock.SpatialMocks.mockWGS84_3D;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.ByteArrayOutputStream;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.InputPosition;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.spatial.Coordinate;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.notifications.NotificationCodeWithDescription;
import org.neo4j.server.http.cypher.TransitionalTxManagementKernelTransaction;
import org.neo4j.server.http.cypher.entity.HttpNode;
import org.neo4j.server.http.cypher.entity.HttpRelationship;
import org.neo4j.server.http.cypher.format.api.FailureEvent;
import org.neo4j.server.http.cypher.format.api.RecordEvent;
import org.neo4j.server.http.cypher.format.api.StatementEndEvent;
import org.neo4j.server.http.cypher.format.api.StatementStartEvent;
import org.neo4j.server.http.cypher.format.api.TransactionInfoEvent;
import org.neo4j.server.http.cypher.format.api.TransactionNotificationState;
import org.neo4j.server.http.cypher.format.common.Neo4jJsonCodec;
import org.neo4j.server.http.cypher.format.input.json.InputStatement;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.test.mockito.mock.GraphMock;
import org.neo4j.test.mockito.mock.Link;
import org.neo4j.test.mockito.mock.SpatialMocks;

class ExecutionResultSerializerTest {
    private static final Map<String, Object> NO_ARGS = Collections.emptyMap();
    private static final Set<String> NO_IDS = Collections.emptySet();
    private static final List<ExecutionPlanDescription> NO_PLANS = emptyList();
    private static final JsonFactory JSON_FACTORY =
            new JsonFactory().disable(JsonGenerator.Feature.FLUSH_PASSED_TO_STREAM);

    private final ByteArrayOutputStream output = new ByteArrayOutputStream();
    private ExecutionResultSerializer serializer;
    private InternalTransaction internalTransaction;

    @BeforeEach
    void init() {
        var context = mock(TransitionalTxManagementKernelTransaction.class);
        internalTransaction = mock(InternalTransaction.class);
        var kernelTransaction = mock(KernelTransactionImplementation.class);

        when(internalTransaction.kernelTransaction()).thenReturn(kernelTransaction);
        when(context.getInternalTransaction()).thenReturn(internalTransaction);
        serializer = getSerializerWith(output);
    }

    @Test
    void shouldSerializeResponseWithCommitUriOnly() {
        // when
        serializer.writeTransactionInfo(new TransactionInfoEvent(
                TransactionNotificationState.NO_TRANSACTION, URI.create("commit/uri/1"), -1, null));

        // then
        String result = output.toString(UTF_8);
        assertEquals("{\"results\":[],\"errors\":[],\"commit\":\"commit/uri/1\"}", result);
    }

    @Test
    void shouldSerializeBookmarkOnCommittedNotificationState() {
        // when
        serializer.writeTransactionInfo(new TransactionInfoEvent(
                TransactionNotificationState.COMMITTED, URI.create("commit/uri/1"), -1, "I AM BOOKMARK!"));

        // then
        String result = output.toString(UTF_8);
        assertEquals(
                "{\"results\":[],\"errors\":[],\"commit\":\"commit/uri/1\",\"lastBookmarks\":[\"I AM BOOKMARK!\"]}",
                result);
    }

    @Test
    void shouldNotSerializeBookmarkOnNonCommittedNotificationStates() {
        // when
        serializer.writeTransactionInfo(new TransactionInfoEvent(
                TransactionNotificationState.NO_TRANSACTION, URI.create("commit/uri/1"), -1, "NOT SEEN!"));

        // then
        String result = output.toString(UTF_8);
        assertEquals("{\"results\":[],\"errors\":[],\"commit\":\"commit/uri/1\"}", result);
    }

    @Test
    void shouldSerializeResponseWithCommitUriAndResults() {
        // given
        var row = Map.of(
                "column1", "value1",
                "column2", "value2");

        // when
        writeStatementStart(serializer, "column1", "column2");
        writeRecord(serializer, row, "column1", "column2");
        writeStatementEnd(serializer);

        serializer.writeTransactionInfo(new TransactionInfoEvent(
                TransactionNotificationState.NO_TRANSACTION, URI.create("commit/uri/1"), -1, null));

        // then
        String result = output.toString(UTF_8);
        assertEquals(
                "{\"results\":[{\"columns\":[\"column1\",\"column2\"],"
                        + "\"data\":[{\"row\":[\"value1\",\"value2\"],\"meta\":[null,null]}]}],\"errors\":[],\"commit\":\"commit/uri/1\"}",
                result);
    }

    @Test
    void shouldSerializeResponseWithResultsOnly() {
        // given
        var row = Map.of(
                "column1", "value1",
                "column2", "value2");

        // when
        writeStatementStart(serializer, "column1", "column2");
        writeRecord(serializer, row, "column1", "column2");
        writeStatementEnd(serializer);
        writeTransactionInfo(serializer);

        // then
        String result = output.toString(UTF_8);
        assertEquals(
                "{\"results\":[{\"columns\":[\"column1\",\"column2\"],"
                        + "\"data\":[{\"row\":[\"value1\",\"value2\"],\"meta\":[null,null]}]}],\"errors\":[]}",
                result);
    }

    @Test
    void shouldSerializeResponseWithCommitUriAndResultsAndErrors() {
        // given
        var row = Map.of(
                "column1", "value1",
                "column2", "value2");

        // when
        writeStatementStart(serializer, "column1", "column2");
        writeRecord(serializer, row, "column1", "column2");
        writeStatementEnd(serializer);
        writeError(serializer, Status.Request.InvalidFormat, "cause1");
        writeTransactionInfo(serializer, "commit/uri/1");

        // then
        String result = output.toString(UTF_8);
        assertEquals(
                "{\"results\":[{\"columns\":[\"column1\",\"column2\"],"
                        + "\"data\":[{\"row\":[\"value1\",\"value2\"],\"meta\":[null,null]}]}],"
                        + "\"errors\":[{\"code\":\"Neo.ClientError.Request.InvalidFormat\",\"message\":\"cause1\"}],\"commit\":\"commit/uri/1\"}",
                result);
    }

    @Test
    void shouldSerializeResponseWithResultsAndErrors() {
        // given
        var row = Map.of(
                "column1", "value1",
                "column2", "value2");

        // when
        writeStatementStart(serializer, "column1", "column2");
        writeRecord(serializer, row, "column1", "column2");
        writeStatementEnd(serializer);
        writeError(serializer, Status.Request.InvalidFormat, "cause1");
        writeTransactionInfo(serializer);

        // then
        String result = output.toString(UTF_8);
        assertEquals(
                "{\"results\":[{\"columns\":[\"column1\",\"column2\"],"
                        + "\"data\":[{\"row\":[\"value1\",\"value2\"],\"meta\":[null,null]}]}],"
                        + "\"errors\":[{\"code\":\"Neo.ClientError.Request.InvalidFormat\",\"message\":\"cause1\"}]}",
                result);
    }

    @Test
    void shouldSerializeResponseWithCommitUriAndErrors() {

        // when
        writeError(serializer, Status.Request.InvalidFormat, "cause1");
        writeTransactionInfo(serializer, "commit/uri/1");

        // then
        String result = output.toString(UTF_8);
        assertEquals(
                "{\"results\":[],\"errors\":[{\"code\":\"Neo.ClientError.Request.InvalidFormat\","
                        + "\"message\":\"cause1\"}],\"commit\":\"commit/uri/1\"}",
                result);
    }

    @Test
    void shouldSerializeResponseWithErrorsOnly() {
        // when
        writeError(serializer, Status.Request.InvalidFormat, "cause1");
        writeTransactionInfo(serializer);

        // then
        String result = output.toString(UTF_8);
        assertEquals(
                "{\"results\":[],\"errors\":[{\"code\":\"Neo.ClientError.Request.InvalidFormat\",\"message\":\"cause1\"}]}",
                result);
    }

    @Test
    void shouldSerializeResponseWithNoCommitUriResultsOrErrors() {

        // when
        writeTransactionInfo(serializer);

        // then
        String result = output.toString(UTF_8);
        assertEquals("{\"results\":[],\"errors\":[]}", result);
    }

    @Test
    void shouldSerializeResponseWithMultipleResultRows() {
        // given
        var row1 = Map.of(
                "column1", "value1",
                "column2", "value2");

        var row2 = Map.of(
                "column1", "value3",
                "column2", "value4");

        // when
        writeStatementStart(serializer, "column1", "column2");
        writeRecord(serializer, row1, "column1", "column2");
        writeRecord(serializer, row2, "column1", "column2");
        writeStatementEnd(serializer);
        writeTransactionInfo(serializer);

        // then
        String result = output.toString(UTF_8);
        assertEquals(
                "{\"results\":[{\"columns\":[\"column1\",\"column2\"],"
                        + "\"data\":[{\"row\":[\"value1\",\"value2\"],\"meta\":[null,null]},"
                        + "{\"row\":[\"value3\",\"value4\"],\"meta\":[null,null]}]}],"
                        + "\"errors\":[]}",
                result);
    }

    @Test
    void shouldSerializeResponseWithMultipleResults() {
        // given
        Map<String, Object> row1 = Map.of(
                "column1", "value1",
                "column2", "value2");

        Map<String, Object> row2 = Map.of(
                "column3", "value3",
                "column4", "value4");

        // when
        writeStatementStart(serializer, "column1", "column2");
        writeRecord(serializer, row1, "column1", "column2");
        writeStatementEnd(serializer);
        writeStatementStart(serializer, "column3", "column4");
        writeRecord(serializer, row2, "column3", "column4");
        writeStatementEnd(serializer);
        writeTransactionInfo(serializer);

        // then
        String result = output.toString(UTF_8);
        assertEquals(
                "{\"results\":["
                        + "{\"columns\":[\"column1\",\"column2\"],\"data\":[{\"row\":[\"value1\",\"value2\"],\"meta\":[null,null]}]},"
                        + "{\"columns\":[\"column3\",\"column4\"],\"data\":[{\"row\":[\"value3\",\"value4\"],\"meta\":[null,null]}]}],"
                        + "\"errors\":[]}",
                result);
    }

    @Test
    void shouldSerializeNodeAsMapOfProperties() throws JsonParseException {
        // given
        var node = new HttpNode(
                "1",
                1,
                emptyList(),
                Map.of(
                        "a", 12,
                        "b", true,
                        "c", new int[] {1, 0, 1, 2},
                        "d", new byte[] {1, 0, 1, 2},
                        "e", new String[] {"a", "b", "ääö"}),
                false);
        var record = Map.of("node", node);

        when(internalTransaction.getNodeById(1)).thenReturn(node);

        // when
        writeStatementStart(serializer, "node");
        writeRecord(serializer, record, "node");
        writeStatementEnd(serializer);
        writeTransactionInfo(serializer);

        // then
        var result = output.toString(UTF_8);
        var json = jsonNode(result);
        var results = json.get("results").get(0);

        var row = results.get("data").get(0).get("row").get(0);
        assertEquals(
                row, jsonNode("{\"b\":true,\"c\":[1,0,1,2],\"d\":[1,0,1,2],\"e\":[\"a\",\"b\",\"ääö\"],\"a\":12}"));
        var meta = results.get("data").get(0).get("meta");
        assertEquals(meta, jsonNode("[{\"id\":1,\"elementId\":\"1\",\"type\":\"node\",\"deleted\":false}]"));
    }

    @Test
    void shouldHandleTransactionHandleStateCorrectly() throws Exception {

        // The serializer is stateful, as the underlying Neo4jJsonCodec uses a handle to the transaction.
        // Therefore, the JSON Factory must not be reused respectively the codec used on a factory cannot be changed
        // Otherwise, we will end up with transaction handles belonging to other threads.

        Function<Integer, Node> selectNode = i -> new HttpNode("1", 1, emptyList(), Map.of("i", i), false);
        Function<Integer, Callable<String>> callableProvider = selectNode.andThen(node -> () -> {

            // given
            var localContext = mock(TransitionalTxManagementKernelTransaction.class);
            var localInternalTransaction = mock(InternalTransaction.class);
            var localKernelTransaction = mock(KernelTransactionImplementation.class);

            when(localInternalTransaction.getNodeById(1)).thenReturn(node);
            when(localInternalTransaction.kernelTransaction()).thenReturn(localKernelTransaction);
            when(localContext.getInternalTransaction()).thenReturn(localInternalTransaction);

            // when
            final ByteArrayOutputStream output = new ByteArrayOutputStream();
            ExecutionResultSerializer serializer = getSerializerWith(output);

            writeStatementStart(serializer, "node");
            writeRecord(serializer, Collections.singletonMap("node", node), "node");
            writeStatementEnd(serializer);
            writeTransactionInfo(serializer);

            // then
            return output.toString(UTF_8);
        });

        int numberOfRequests = 10;
        ExecutorService executor = Executors.newCachedThreadPool();

        List<Future<String>> calledRequests = executor.invokeAll(IntStream.range(0, numberOfRequests)
                .boxed()
                .map(callableProvider)
                .collect(Collectors.toList()));
        try {
            int i = 0;
            for (Future<String> request : calledRequests) {
                var expectedResult = "{\"results\":[{\"columns\":[\"node\"]," + "\"data\":[{\"row\":[{\"i\":"
                        + i + "}],"
                        + "\"meta\":[{\"id\":1,\"elementId\":\"1\",\"type\":\"node\",\"deleted\":false}]}]}],"
                        + "\"errors\":[]}";
                try {
                    var result = request.get();
                    assertEquals(expectedResult, result);
                } catch (ExecutionException e) {
                    e.printStackTrace();
                    Assertions.fail("At least one request failed " + e.getMessage());
                }
                ++i;
            }
        } finally {
            executor.shutdown();
        }
    }

    @Test
    void shouldSerializeNestedEntities() {
        // given
        var a = new HttpNode("1", 1, List.of(), Map.of("foo", 12), false);
        var b = new HttpNode("2", 2, List.of(), Map.of("bar", false), false);
        var r = new HttpRelationship(
                "1",
                1,
                "1",
                1,
                "2",
                2,
                "FRAZZLE",
                Map.of("baz", "quux"),
                false,
                (ignoredA, ignoredB) -> Optional.empty());
        var row = Map.of("nested", new TreeMap<>(Map.of("node", a, "edge", r, "path", path(a, link(r, b)))));

        // when
        writeStatementStart(serializer, "nested");
        writeRecord(serializer, row, "nested");
        writeStatementEnd(serializer);
        writeTransactionInfo(serializer);

        // then
        String result = output.toString(UTF_8);
        assertEquals(
                "{\"results\":[{\"columns\":[\"nested\"],"
                        + "\"data\":[{\"row\":[{\"edge\":{\"baz\":\"quux\"},\"node\":{\"foo\":12},"
                        + "\"path\":[{\"foo\":12},{\"baz\":\"quux\"},{\"bar\":false}]}],"
                        + "\"meta\":[{\"id\":1,\"elementId\":\"1\",\"type\":\"relationship\",\"deleted\":false},"
                        + "{\"id\":1,\"elementId\":\"1\",\"type\":\"node\",\"deleted\":false},[{\"id\":1,\"elementId\":\"1\",\"type\":\"node\",\"deleted\":false},"
                        + "{\"id\":1,\"elementId\":\"1\",\"type\":\"relationship\",\"deleted\":false},{\"id\":2,\"elementId\":\"2\",\"type\":\"node\",\"deleted\":false}]]}]}],"
                        + "\"errors\":[]}",
                result);
    }

    @Test
    void shouldSerializePathAsListOfMapsOfProperties() {
        // given
        var path =
                mockPathWithHttpEntities(Map.of("key1", "value1"), Map.of("key2", "value2"), Map.of("key3", "value3"));
        var row = Map.of("path", path);

        // when
        writeStatementStart(serializer, "path");
        writeRecord(serializer, row, "path");
        writeStatementEnd(serializer);
        writeTransactionInfo(serializer);

        // then
        String result = output.toString(UTF_8);
        assertEquals(
                "{\"results\":[{\"columns\":[\"path\"],"
                        + "\"data\":[{\"row\":[[{\"key1\":\"value1\"},{\"key2\":\"value2\"},{\"key3\":\"value3\"}]],"
                        + "\"meta\":[[{\"id\":1,\"elementId\":\"1\",\"type\":\"node\",\"deleted\":false},"
                        + "{\"id\":1,\"elementId\":\"1\",\"type\":\"relationship\",\"deleted\":false},{\"id\":2,\"elementId\":\"2\",\"type\":\"node\",\"deleted\":false}]]}]}],"
                        + "\"errors\":[]}",
                result);
    }

    @Test
    void shouldSerializePointsAsListOfMapsOfProperties() {
        // given
        var row1 = Map.of("geom", SpatialMocks.mockPoint(12.3, 45.6, mockWGS84()));
        var row2 = Map.of("geom", SpatialMocks.mockPoint(123, 456, mockCartesian()));
        var row3 = Map.of("geom", SpatialMocks.mockPoint(12.3, 45.6, 78.9, mockWGS84_3D()));
        var row4 = Map.of("geom", SpatialMocks.mockPoint(123, 456, 789, mockCartesian_3D()));

        // when
        writeStatementStart(serializer, "geom");
        writeRecord(serializer, row1, "geom");
        writeRecord(serializer, row2, "geom");
        writeRecord(serializer, row3, "geom");
        writeRecord(serializer, row4, "geom");
        writeStatementEnd(serializer);
        writeTransactionInfo(serializer);

        // then
        String result = output.toString(UTF_8);
        assertEquals(
                "{\"results\":[{\"columns\":[\"geom\"],\"data\":["
                        + "{\"row\":[{\"type\":\"Point\",\"coordinates\":[12.3,45.6],\"crs\":"
                        + "{\"srid\":4326,\"name\":\"WGS-84\",\"type\":\"link\",\"properties\":"
                        + "{\"href\":\"https://spatialreference.org/ref/epsg/4326/ogcwkt/\",\"type\":\"ogcwkt\"}"
                        + "}}],\"meta\":[{\"type\":\"point\"}]},"
                        + "{\"row\":[{\"type\":\"Point\",\"coordinates\":[123.0,456.0],\"crs\":"
                        + "{\"srid\":7203,\"name\":\"cartesian\",\"type\":\"link\",\"properties\":"
                        + "{\"href\":\"https://spatialreference.org/ref/sr-org/7203/ogcwkt/\",\"type\":\"ogcwkt\"}"
                        + "}}],\"meta\":[{\"type\":\"point\"}]},"
                        + "{\"row\":[{\"type\":\"Point\",\"coordinates\":[12.3,45.6,78.9],\"crs\":"
                        + "{\"srid\":4979,\"name\":\"WGS-84-3D\",\"type\":\"link\",\"properties\":"
                        + "{\"href\":\"https://spatialreference.org/ref/epsg/4979/ogcwkt/\",\"type\":\"ogcwkt\"}"
                        + "}}],\"meta\":[{\"type\":\"point\"}]},"
                        + "{\"row\":[{\"type\":\"Point\",\"coordinates\":[123.0,456.0,789.0],\"crs\":"
                        + "{\"srid\":9157,\"name\":\"cartesian-3D\",\"type\":\"link\",\"properties\":"
                        + "{\"href\":\"https://spatialreference.org/ref/sr-org/9157/ogcwkt/\",\"type\":\"ogcwkt\"}"
                        + "}}],\"meta\":[{\"type\":\"point\"}]}"
                        + "]}],\"errors\":[]}",
                result);
    }

    @Test
    void shouldSerializeTemporalAsListOfMapsOfProperties() {
        // given
        var row1 = Map.of("temporal", LocalDate.of(2018, 3, 12));
        var row2 = Map.of("temporal", ZonedDateTime.of(2018, 3, 12, 13, 2, 10, 10, ZoneId.of("UTC+1")));
        var row3 = Map.of("temporal", OffsetTime.of(12, 2, 4, 71, ZoneOffset.UTC));
        var row4 = Map.of("temporal", LocalDateTime.of(2018, 3, 12, 13, 2, 10, 10));
        var row5 = Map.of("temporal", LocalTime.of(13, 2, 10, 10));
        var row6 = Map.of("temporal", Duration.of(12, ChronoUnit.HOURS));

        // when
        writeStatementStart(serializer, "temporal");
        writeRecord(serializer, row1, "temporal");
        writeRecord(serializer, row2, "temporal");
        writeRecord(serializer, row3, "temporal");
        writeRecord(serializer, row4, "temporal");
        writeRecord(serializer, row5, "temporal");
        writeRecord(serializer, row6, "temporal");
        writeStatementEnd(serializer);

        serializer.writeTransactionInfo(
                new TransactionInfoEvent(TransactionNotificationState.NO_TRANSACTION, null, -1, null));

        // then
        String result = output.toString(UTF_8);
        assertEquals(
                "{\"results\":[{\"columns\":[\"temporal\"],\"data\":["
                        + "{\"row\":[\"2018-03-12\"],\"meta\":[{\"type\":\"date\"}]},"
                        + "{\"row\":[\"2018-03-12T13:02:10.000000010+01:00[UTC+01:00]\"],\"meta\":[{\"type\":\"datetime\"}]},"
                        + "{\"row\":[\"12:02:04.000000071Z\"],\"meta\":[{\"type\":\"time\"}]},"
                        + "{\"row\":[\"2018-03-12T13:02:10.000000010\"],\"meta\":[{\"type\":\"localdatetime\"}]},"
                        + "{\"row\":[\"13:02:10.000000010\"],\"meta\":[{\"type\":\"localtime\"}]},"
                        + "{\"row\":[\"PT12H\"],\"meta\":[{\"type\":\"duration\"}]}"
                        + "]}],\"errors\":[]}",
                result);
    }

    @Test
    void shouldErrorWhenSerializingUnknownGeometryType() {
        // given
        var points = List.of(new Coordinate(1, 2), new Coordinate(2, 3));

        var row = Map.of("geom", SpatialMocks.mockGeometry("LineString", points, mockCartesian()));

        // when
        var e = assertThrows(RuntimeException.class, () -> {
            writeStatementStart(serializer, "geom");
            writeRecord(serializer, row, "geom");
        });

        writeError(serializer, Status.Statement.ExecutionFailed, e.getMessage());
        writeTransactionInfo(serializer);

        // then
        String result = output.toString(UTF_8);
        assertThat(result)
                .startsWith("{\"results\":[{\"columns\":[\"geom\"],\"data\":["
                        + "{\"row\":[{\"type\":\"LineString\",\"coordinates\":[[1.0,2.0],[2.0,3.0]],\"crs\":"
                        + "{\"srid\":7203,\"name\":\"cartesian\",\"type\":\"link\",\"properties\":"
                        + "{\"href\":\"https://spatialreference.org/ref/sr-org/7203/ogcwkt/\",\"type\":\"ogcwkt\"}}}],\"meta\":[]}]}],"
                        + "\"errors\":[{\"code\":\"Neo.DatabaseError.Statement.ExecutionFailed\","
                        + "\"message\":\"Unsupported Geometry type: type=MockGeometry, value=LineString\"");
    }

    @Test
    void shouldProduceWellFormedJsonEvenIfResultIteratorThrowsExceptionOnNext() {
        // given
        var row = Map.of(
                "column1", "value1",
                "column2", "value2");

        var recordEvent = mock(RecordEvent.class);
        when(recordEvent.getValue(any())).thenThrow(new RuntimeException("Stuff went wrong!"));
        when(recordEvent.getColumns()).thenReturn(List.of("column1", "column2"));

        // when
        var e = assertThrows(RuntimeException.class, () -> {
            writeStatementStart(serializer, "column1", "column2");
            writeRecord(serializer, row, "column1", "column2");
            serializer.writeRecord(recordEvent);
        });

        writeError(serializer, Status.Statement.ExecutionFailed, e.getMessage());
        writeTransactionInfo(serializer);

        // then
        String result = output.toString(UTF_8);
        assertEquals(
                "{\"results\":[{\"columns\":[\"column1\",\"column2\"],"
                        + "\"data\":[{\"row\":[\"value1\",\"value2\"],\"meta\":[null,null]},{\"row\":[],\"meta\":[]}]}],"
                        + "\"errors\":[{\"code\":\"Neo.DatabaseError.Statement.ExecutionFailed\","
                        + "\"message\":\"Stuff went wrong!\"}]}",
                result);
    }

    @Test
    void shouldProduceResultStreamWithGraphEntries() {
        // given
        Node[] node = {
            new HttpNode("0", 0, List.of(label("Node")), Map.of("name", "node0"), false),
            new HttpNode("1", 1, emptyList(), Map.of("name", "node1"), false),
            new HttpNode("2", 2, List.of(label("This"), label("That")), Map.of("name", "node2"), false),
            new HttpNode("3", 3, List.of(label("Other")), Map.of("name", "node3"), false)
        };
        Relationship[] rel = {
            new HttpRelationship(
                    "0",
                    0,
                    "0",
                    0,
                    "1",
                    1,
                    "KNOWS",
                    Map.of("name", "rel0"),
                    false,
                    (i, b) -> Optional.of(node[Math.toIntExact(i)])),
            new HttpRelationship(
                    "1",
                    1,
                    "2",
                    2,
                    "3",
                    3,
                    "LOVES",
                    Map.of("name", "rel1"),
                    false,
                    (i, b) -> Optional.of(node[Math.toIntExact(i)]))
        };

        when(internalTransaction.getRelationshipById(anyLong())).thenAnswer((Answer<Relationship>)
                invocation -> rel[invocation.getArgument(0, Number.class).intValue()]);
        when(internalTransaction.getNodeById(anyLong())).thenAnswer((Answer<Node>)
                invocation -> node[invocation.getArgument(0, Number.class).intValue()]);

        var resultRow1 = Map.of(
                "node", node[0],
                "rel", rel[0]);

        var resultRow2 = Map.of(
                "node", node[2],
                "rel", rel[1]);

        // when
        writeStatementStart(serializer, List.of(ResultDataContent.row, ResultDataContent.graph), "node", "rel");
        writeRecord(serializer, resultRow1, "node", "rel");
        writeRecord(serializer, resultRow2, "node", "rel");
        writeStatementEnd(serializer);
        writeTransactionInfo(serializer);

        // then
        String result = output.toString(UTF_8);

        // Nodes and relationships form sets, so we cannot test for a fixed string, since we don't know the order.
        String node0 = "{\"id\":\"0\",\"elementId\":\"0\",\"labels\":[\"Node\"],\"properties\":{\"name\":\"node0\"}}";
        String node1 = "{\"id\":\"1\",\"elementId\":\"1\",\"labels\":[],\"properties\":{\"name\":\"node1\"}}";
        String node2 =
                "{\"id\":\"2\",\"elementId\":\"2\",\"labels\":[\"This\",\"That\"],\"properties\":{\"name\":\"node2\"}}";
        String node3 = "{\"id\":\"3\",\"elementId\":\"3\",\"labels\":[\"Other\"],\"properties\":{\"name\":\"node3\"}}";

        String rel0 = "\"relationships\":[{\"id\":\"0\",\"elementId\":\"0\",\"type\":\"KNOWS\","
                + "\"startNode\":\"0\",\"startNodeElementId\":\"0\",\"endNode\":\"1\",\"endNodeElementId\":\"1\",\"properties\":{\"name\":\"rel0\"}}]}";

        String rel1 = "\"relationships\":[{\"id\":\"1\",\"elementId\":\"1\",\"type\":\"LOVES\","
                + "\"startNode\":\"2\",\"startNodeElementId\":\"2\",\"endNode\":\"3\",\"endNodeElementId\":\"3\",\"properties\":{\"name\":\"rel1\"}}]}";

        String row0 = "{\"row\":[{\"name\":\"node0\"},{\"name\":\"rel0\"}],"
                + "\"meta\":[{\"id\":0,\"elementId\":\"0\",\"type\":\"node\",\"deleted\":false},"
                + "{\"id\":0,\"elementId\":\"0\",\"type\":\"relationship\",\"deleted\":false}],\"graph\":{\"nodes\":[";

        String row1 = "{\"row\":[{\"name\":\"node2\"},{\"name\":\"rel1\"}],"
                + "\"meta\":[{\"id\":2,\"elementId\":\"2\",\"type\":\"node\",\"deleted\":false},"
                + "{\"id\":1,\"elementId\":\"1\",\"type\":\"relationship\",\"deleted\":false}],\"graph\":{\"nodes\":[";
        int n0 = result.indexOf(node0);
        int n1 = result.indexOf(node1);
        int n2 = result.indexOf(node2);
        int n3 = result.indexOf(node3);
        int r0 = result.indexOf(rel0);
        int r1 = result.indexOf(rel1);
        int row0Index = result.indexOf(row0);
        int row1Index = result.indexOf(row1);
        assertTrue(row0Index > 0, "result should contain row0");
        assertTrue(row1Index > row0Index, "result should contain row1 after row0");
        assertTrue(n0 > row0Index, "result should contain node0 after row0");
        assertTrue(n1 > row0Index, "result should contain node1 after row0");
        assertTrue(n2 > row1Index, "result should contain node2 after row1");
        assertTrue(n3 > row1Index, "result should contain node3 after row1");
        assertTrue(r0 > n0 && r0 > n1, "result should contain rel0 after node0 and node1");
        assertTrue(r1 > n2 && r1 > n3, "result should contain rel1 after node2 and node3");
    }

    @Test
    void shouldProduceResultStreamWithLegacyRestFormat() throws Exception {
        // given
        Node[] node = {
            new HttpNode("0", 0, emptyList(), Map.of("name", "node0"), false),
            new HttpNode("1", 1, emptyList(), Map.of("name", "node1"), false),
            new HttpNode("2", 2, emptyList(), Map.of("name", "node2"), false)
        };
        Relationship[] rel = {
            new HttpRelationship(
                    "0",
                    0,
                    "0",
                    0,
                    "1",
                    1,
                    "KNOWS",
                    Map.of("name", "rel0"),
                    false,
                    (ignoredA, ignoredB) -> Optional.empty()),
            new HttpRelationship(
                    "1",
                    1,
                    "2",
                    2,
                    "1",
                    1,
                    "LOVES",
                    Map.of("name", "rel1"),
                    false,
                    (ignoredA, ignoredB) -> Optional.empty())
        };
        Path path = GraphMock.path(node[0], link(rel[0], node[1]), link(rel[1], node[2]));

        serializer = getSerializerWith(output, "http://base.uri/");

        var resultRow =
                Map.of("node", node[0], "rel", rel[0], "path", path, "map", Map.of("n1", node[1], "r1", rel[1]));

        // when
        writeStatementStart(
                serializer, Collections.singletonList(ResultDataContent.rest), "node", "rel", "path", "map");
        writeRecord(serializer, resultRow, "node", "rel", "path", "map");
        writeStatementEnd(serializer);
        writeTransactionInfo(serializer);

        // then
        String result = output.toString(UTF_8);
        JsonNode json = jsonNode(result);
        Map<String, Integer> columns = new HashMap<>();
        int col = 0;
        JsonNode results = json.get("results").get(0);
        for (JsonNode column : results.get("columns")) {
            columns.put(column.asText(), col++);
        }
        JsonNode row = results.get("data").get(0).get("rest");
        JsonNode jsonNode = row.get(columns.get("node"));
        JsonNode jsonRel = row.get(columns.get("rel"));
        JsonNode jsonPath = row.get(columns.get("path"));
        JsonNode jsonMap = row.get(columns.get("map"));
        assertEquals("http://base.uri/node/0", jsonNode.get("self").asText());
        assertEquals("http://base.uri/relationship/0", jsonRel.get("self").asText());
        assertEquals(2, jsonPath.get("length").asInt());
        assertEquals("http://base.uri/node/0", jsonPath.get("start").asText());
        assertEquals("http://base.uri/node/2", jsonPath.get("end").asText());
        assertEquals("http://base.uri/node/1", jsonMap.get("n1").get("self").asText());
        assertEquals(
                "http://base.uri/relationship/1", jsonMap.get("r1").get("self").asText());
    }

    @Test
    void shouldProduceResultStreamWithLegacyRestFormatAndNestedMaps() throws Exception {
        // given
        serializer = getSerializerWith(output, "http://base.uri/");

        // RETURN {one:{two:['wait for it...', {three: 'GO!'}]}}
        var resultRow = Map.of("map", Map.of("one", Map.of("two", List.of("wait for it...", Map.of("three", "GO!")))));

        // when
        writeStatementStart(serializer, Collections.singletonList(ResultDataContent.rest), "map");
        writeRecord(serializer, resultRow, "map");
        writeStatementEnd(serializer);
        writeTransactionInfo(serializer);

        // then
        String result = output.toString(UTF_8);
        JsonNode json = jsonNode(result);
        Map<String, Integer> columns = new HashMap<>();
        int col = 0;
        JsonNode results = json.get("results").get(0);
        for (JsonNode column : results.get("columns")) {
            columns.put(column.asText(), col++);
        }
        JsonNode row = results.get("data").get(0).get("rest");
        JsonNode jsonMap = row.get(columns.get("map"));
        assertEquals("wait for it...", jsonMap.get("one").get("two").get(0).asText());
        assertEquals("GO!", jsonMap.get("one").get("two").get(1).get("three").asText());
    }

    @Test
    void shouldSerializePlanWithoutChildButAllKindsOfSupportedArguments() throws Exception {
        // given
        serializer = getSerializerWith(output, "http://base.uri/");

        String operatorType = "Ich habe einen Plan";

        // This is the full set of types that we allow in plan arguments

        var args = Map.of(
                "string",
                "A String",
                "bool",
                true,
                "number",
                1,
                "double",
                2.3,
                "listOfInts",
                List.of(1, 2, 3),
                "listOfListOfInts",
                List.of(List.of(1, 2, 3)));

        ExecutionPlanDescription planDescription = mockedPlanDescription(operatorType, NO_IDS, args, NO_PLANS);

        // when

        writeStatementStart(serializer, Collections.singletonList(ResultDataContent.rest));
        writeRecord(serializer, Collections.emptyMap());
        writeStatementEnd(serializer, planDescription, emptyList());
        writeTransactionInfo(serializer);

        String resultString = output.toString(UTF_8);

        // then
        assertIsPlanRoot(resultString);
        Map<String, ?> rootMap = planRootMap(resultString);

        assertEquals(
                asSet(
                        "operatorType",
                        "identifiers",
                        "children",
                        "string",
                        "bool",
                        "number",
                        "double",
                        "listOfInts",
                        "listOfListOfInts"),
                rootMap.keySet());

        assertEquals(operatorType, rootMap.get("operatorType"));
        assertEquals(args.get("string"), rootMap.get("string"));
        assertEquals(args.get("bool"), rootMap.get("bool"));
        assertEquals(args.get("number"), rootMap.get("number"));
        assertEquals(args.get("double"), rootMap.get("double"));
        assertEquals(args.get("listOfInts"), rootMap.get("listOfInts"));
        assertEquals(args.get("listOfListOfInts"), rootMap.get("listOfListOfInts"));
    }

    @Test
    void shouldSerializePlanWithoutChildButWithIdentifiers() throws Exception {
        // given
        serializer = getSerializerWith(output, "http://base.uri/");

        String operatorType = "Ich habe einen Plan";
        String id1 = "id1";
        String id2 = "id2";
        String id3 = "id3";

        // This is the full set of types that we allow in plan arguments
        ExecutionPlanDescription planDescription =
                mockedPlanDescription(operatorType, asSet(id1, id2, id3), NO_ARGS, NO_PLANS);

        // when
        writeStatementStart(serializer, Collections.singletonList(ResultDataContent.rest));
        writeRecord(serializer, Collections.emptyMap());
        writeStatementEnd(serializer, planDescription, emptyList());
        writeTransactionInfo(serializer);

        String resultString = output.toString(UTF_8);

        // then
        assertIsPlanRoot(resultString);
        Map<String, ?> rootMap = planRootMap(resultString);

        assertEquals(asSet("operatorType", "identifiers", "children"), rootMap.keySet());

        assertEquals(operatorType, rootMap.get("operatorType"));
        assertEquals(List.of(id2, id1, id3), rootMap.get("identifiers"));
    }

    @Test
    void shouldSerializePlanWithChildren() throws Exception {
        // given
        serializer = getSerializerWith(output, "http://base.uri/");

        String leftId = "leftId";
        String rightId = "rightId";
        String parentId = "parentId";

        ExecutionPlanDescription left = mockedPlanDescription("child", asSet(leftId), Map.of("id", 1), NO_PLANS);
        ExecutionPlanDescription right = mockedPlanDescription("child", asSet(rightId), Map.of("id", 2), NO_PLANS);
        ExecutionPlanDescription parent =
                mockedPlanDescription("parent", asSet(parentId), Map.of("id", 0), List.of(left, right));

        // when
        writeStatementStart(serializer, Collections.singletonList(ResultDataContent.rest));
        writeRecord(serializer, Collections.emptyMap());
        writeStatementEnd(serializer, parent, emptyList());
        writeTransactionInfo(serializer);

        // then
        String result = output.toString(UTF_8);
        JsonNode root = assertIsPlanRoot(result);

        assertEquals("parent", root.get("operatorType").asText());
        assertEquals(0, root.get("id").asLong());
        assertEquals(asSet(parentId), identifiersOf(root));

        Set<Integer> childIds = new HashSet<>();
        Set<Set<String>> identifiers = new HashSet<>();
        for (JsonNode child : root.get("children")) {
            assertTrue(child.isObject(), "Expected object");
            assertEquals("child", child.get("operatorType").asText());
            identifiers.add(identifiersOf(child));
            childIds.add(child.get("id").asInt());
        }
        assertEquals(asSet(1, 2), childIds);
        assertEquals(asSet(asSet(leftId), asSet(rightId)), identifiers);
    }

    @Test
    void shouldReturnNotifications() {
        // given
        Notification notification =
                NotificationCodeWithDescription.cartesianProduct(new InputPosition(1, 2, 3), "a", "(), ()");
        List<Notification> notifications = Collections.singletonList(notification);

        var row = Map.of(
                "column1", "value1",
                "column2", "value2");

        // when
        writeStatementStart(serializer, "column1", "column2");
        writeRecord(serializer, row, "column1", "column2");
        writeStatementEnd(serializer, null, notifications);
        writeTransactionInfo(serializer, "commit/uri/1");

        // then
        String result = output.toString(UTF_8);

        assertEquals(
                "{\"results\":[{\"columns\":[\"column1\",\"column2\"],"
                        + "\"data\":[{\"row\":[\"value1\",\"value2\"],\"meta\":[null,null]}]}],\"notifications\":[{\"code\":\"Neo"
                        + ".ClientNotification.Statement.CartesianProduct\",\"severity\":\"INFORMATION\",\"title\":\"This "
                        + "query builds a cartesian product between disconnected patterns.\",\"description\":\"If a "
                        + "part of a query contains multiple disconnected patterns, this will build a cartesian product"
                        + " between all those parts. This may produce a large amount of data and slow down query "
                        + "processing. While occasionally intended, it may often be possible to reformulate the query "
                        + "that avoids the use of this cross product, perhaps by adding a relationship between the "
                        + "different parts or by using OPTIONAL MATCH (a)\",\"position\":{\"offset\":1,\"line\":2,"
                        + "\"column\":3}}],\"errors\":[],\"commit\":\"commit/uri/1\"}",
                result);
    }

    @Test
    void shouldNotReturnNotificationsWhenEmptyNotifications() {
        // given
        var row = Map.of(
                "column1", "value1",
                "column2", "value2");

        // when
        writeStatementStart(serializer, "column1", "column2");
        writeRecord(serializer, row, "column1", "column2");
        writeStatementEnd(serializer, null, emptyList());
        writeTransactionInfo(serializer, "commit/uri/1");

        // then
        String result = output.toString(UTF_8);

        assertEquals(
                "{\"results\":[{\"columns\":[\"column1\",\"column2\"],"
                        + "\"data\":[{\"row\":[\"value1\",\"value2\"],\"meta\":[null,null]}]}],\"errors\":[],\"commit\":\"commit/uri/1\"}",
                result);
    }

    @Test
    void shouldNotReturnPositionWhenEmptyPosition() {
        // given
        var row = Map.of(
                "column1", "value1",
                "column2", "value2");

        Notification notification =
                NotificationCodeWithDescription.cartesianProduct(InputPosition.empty, "a", "(), ()");

        List<Notification> notifications = Collections.singletonList(notification);

        // when
        writeStatementStart(serializer, "column1", "column2");
        writeRecord(serializer, row, "column1", "column2");
        writeStatementEnd(serializer, null, notifications);
        writeTransactionInfo(serializer, "commit/uri/1");

        // then
        String result = output.toString(UTF_8);

        assertEquals(
                "{\"results\":[{\"columns\":[\"column1\",\"column2\"],"
                        + "\"data\":[{\"row\":[\"value1\",\"value2\"],\"meta\":[null,null]}]}],\"notifications\":[{\"code\":\"Neo"
                        + ".ClientNotification.Statement.CartesianProduct\",\"severity\":\"INFORMATION\",\"title\":\"This "
                        + "query builds a cartesian product between disconnected patterns.\",\"description\":\"If a "
                        + "part of a query contains multiple disconnected patterns, this will build a cartesian product"
                        + " between all those parts. This may produce a large amount of data and slow down query "
                        + "processing. While occasionally intended, it may often be possible to reformulate the query "
                        + "that avoids the use of this cross product, perhaps by adding a relationship between the "
                        + "different parts or by using OPTIONAL MATCH (a)\"}],\"errors\":[],\"commit\":\"commit/uri/1\"}",
                result);
    }

    private static ExecutionResultSerializer getSerializerWith(OutputStream output) {
        return getSerializerWith(output, null);
    }

    private static ExecutionResultSerializer getSerializerWith(OutputStream output, String uri) {
        return new ExecutionResultSerializer(
                Collections.emptyMap(),
                uri == null ? null : URI.create(uri),
                Neo4jJsonCodec.class,
                JSON_FACTORY,
                output);
    }

    private static void writeStatementStart(ExecutionResultSerializer serializer, String... columns) {
        writeStatementStart(serializer, null, columns);
    }

    private static void writeStatementStart(
            ExecutionResultSerializer serializer, List<ResultDataContent> resultDataContents, String... columns) {
        serializer.writeStatementStart(
                new StatementStartEvent(null, Arrays.asList(columns)),
                new InputStatement(null, null, false, resultDataContents));
    }

    private static void writeRecord(ExecutionResultSerializer serializer, Map<String, ?> row, String... columns) {
        serializer.writeRecord(new RecordEvent(Arrays.asList(columns), row::get));
    }

    private static void writeStatementEnd(ExecutionResultSerializer serializer) {
        writeStatementEnd(serializer, null, emptyList());
    }

    private static void writeStatementEnd(
            ExecutionResultSerializer serializer,
            ExecutionPlanDescription planDescription,
            Iterable<Notification> notifications) {
        QueryExecutionType queryExecutionType = null != planDescription
                ? QueryExecutionType.profiled(QueryExecutionType.QueryType.READ_WRITE)
                : QueryExecutionType.query(QueryExecutionType.QueryType.READ_WRITE);

        serializer.writeStatementEnd(new StatementEndEvent(queryExecutionType, null, planDescription, notifications));
    }

    private static void writeTransactionInfo(ExecutionResultSerializer serializer) {
        serializer.writeTransactionInfo(
                new TransactionInfoEvent(TransactionNotificationState.NO_TRANSACTION, null, -1, null));
    }

    private static void writeTransactionInfo(ExecutionResultSerializer serializer, String commitUri) {
        serializer.writeTransactionInfo(
                new TransactionInfoEvent(TransactionNotificationState.NO_TRANSACTION, URI.create(commitUri), -1, null));
    }

    private static void writeError(ExecutionResultSerializer serializer, Status status, String message) {
        serializer.writeFailure(new FailureEvent(status, message));
    }

    private static Set<String> identifiersOf(JsonNode root) {
        Set<String> parentIds = new HashSet<>();
        for (JsonNode id : root.get("identifiers")) {
            parentIds.add(id.asText());
        }
        return parentIds;
    }

    private static ExecutionPlanDescription mockedPlanDescription(
            String operatorType,
            Set<String> identifiers,
            Map<String, Object> args,
            List<ExecutionPlanDescription> children) {
        ExecutionPlanDescription planDescription = mock(ExecutionPlanDescription.class);
        when(planDescription.getChildren()).thenReturn(children);
        when(planDescription.getName()).thenReturn(operatorType);
        when(planDescription.getArguments()).thenReturn(args);
        when(planDescription.getIdentifiers()).thenReturn(identifiers);
        return planDescription;
    }

    private static JsonNode assertIsPlanRoot(String result) throws JsonParseException {
        JsonNode json = jsonNode(result);
        JsonNode results = json.get("results").get(0);

        JsonNode plan = results.get("plan");
        assertTrue(plan != null && plan.isObject(), "Expected plan to be an object");

        JsonNode root = plan.get("root");
        assertTrue(root != null && root.isObject(), "Expected plan to be an object");

        return root;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, ?> planRootMap(String resultString) throws JsonParseException {
        Map<String, ?> resultMap =
                (Map<String, ?>) ((List<?>) ((Map<String, ?>) (readJson(resultString))).get("results")).get(0);
        Map<String, ?> planMap = (Map<String, ?>) (resultMap.get("plan"));
        return (Map<String, ?>) (planMap.get("root"));
    }

    private static Path mockPathWithHttpEntities(
            Map<String, Object> startNodeProperties,
            Map<String, Object> relationshipProperties,
            Map<String, Object> endNodeProperties) {
        Node startNode = new HttpNode("1", 1, emptyList(), startNodeProperties, false);
        Node endNode = new HttpNode("2", 2, emptyList(), endNodeProperties, false);
        Relationship relationship = new HttpRelationship(
                "1",
                1,
                "1",
                1,
                "2",
                2,
                "RELATED",
                relationshipProperties,
                false,
                (ignoredA, ignoredB) -> Optional.empty());
        return path(startNode, Link.link(relationship, endNode));
    }
}
