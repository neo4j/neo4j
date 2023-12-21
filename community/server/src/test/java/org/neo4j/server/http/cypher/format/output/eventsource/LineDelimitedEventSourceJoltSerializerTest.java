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
package org.neo4j.server.http.cypher.format.output.eventsource;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.server.rest.domain.JsonHelper.jsonNode;
import static org.neo4j.test.mockito.mock.GraphMock.link;
import static org.neo4j.test.mockito.mock.GraphMock.node;
import static org.neo4j.test.mockito.mock.GraphMock.path;
import static org.neo4j.test.mockito.mock.GraphMock.relationship;
import static org.neo4j.test.mockito.mock.Properties.properties;
import static org.neo4j.test.mockito.mock.Property.property;
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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.InputPosition;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Notification;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.notifications.NotificationCodeWithDescription;
import org.neo4j.server.http.cypher.TransitionalTxManagementKernelTransaction;
import org.neo4j.server.http.cypher.format.api.RecordEvent;
import org.neo4j.server.http.cypher.format.api.TransactionInfoEvent;
import org.neo4j.server.http.cypher.format.api.TransactionNotificationState;
import org.neo4j.server.http.cypher.format.jolt.v1.JoltV1Codec;
import org.neo4j.server.http.cypher.format.output.json.ResultDataContent;
import org.neo4j.test.mockito.mock.SpatialMocks;
import org.neo4j.values.storable.DurationValue;

public class LineDelimitedEventSourceJoltSerializerTest extends AbstractEventSourceJoltSerializerTest {

    private static final Map<String, Object> NO_ARGS = Collections.emptyMap();
    private static final Set<String> NO_IDS = Collections.emptySet();
    private static final List<ExecutionPlanDescription> NO_PLANS = Collections.emptyList();
    private static final JsonFactory JSON_FACTORY =
            new JsonFactory().disable(JsonGenerator.Feature.FLUSH_PASSED_TO_STREAM);

    private final ByteArrayOutputStream output = new ByteArrayOutputStream();
    private LineDelimitedEventSourceJoltSerializer serializer;

    @BeforeEach
    void init() {
        var context = mock(TransitionalTxManagementKernelTransaction.class);
        var kernelTransaction = mock(KernelTransactionImplementation.class);

        serializer = getSerializerWith(output);
    }

    @Test
    void shouldSerializeResponseWithCommitUriOnly() {
        // when
        serializer.writeTransactionInfo(new TransactionInfoEvent(
                TransactionNotificationState.NO_TRANSACTION, URI.create("commit/uri/1"), -1, null));

        // then
        String result = output.toString(UTF_8);
        assertEquals("{\"info\":{\"commit\":\"commit/uri/1\"}}\n", result);
    }

    @Test
    void shouldSerializeBookmarkOnCommittedNotificationState() {
        // when

        serializer.writeTransactionInfo(new TransactionInfoEvent(
                TransactionNotificationState.COMMITTED, URI.create("commit/uri/1"), -1, "I AM BOOKMARK!"));

        // then
        String result = output.toString(UTF_8);
        assertEquals("{\"info\":{\"commit\":\"commit/uri/1\",\"lastBookmarks\":[\"I AM BOOKMARK!\"]}}\n", result);
    }

    @Test
    void shouldNotSerializeBookmarkOnNonCommittedNotificationStates() {
        // when
        serializer.writeTransactionInfo(new TransactionInfoEvent(
                TransactionNotificationState.NO_TRANSACTION, URI.create("commit/uri/1"), -1, "NOT SEEN!"));

        // then
        String result = output.toString(UTF_8);
        assertEquals("{\"info\":{\"commit\":\"commit/uri/1\"}}\n", result);
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
                "{\"header\":{\"fields\":[\"column1\",\"column2\"]}}\n"
                        + "{\"data\":[{\"U\":\"value1\"},{\"U\":\"value2\"}]}\n"
                        + "{\"summary\":{}}\n"
                        + "{\"info\":{\"commit\":\"commit/uri/1\"}}\n",
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
                "{\"header\":{\"fields\":[\"column1\",\"column2\"]}}\n"
                        + "{\"data\":[{\"U\":\"value1\"},{\"U\":\"value2\"}]}\n"
                        + "{\"summary\":{}}\n"
                        + "{\"info\":{}}\n",
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
                "{\"header\":{\"fields\":[\"column1\",\"column2\"]}}\n"
                        + "{\"data\":[{\"U\":\"value1\"},{\"U\":\"value2\"}]}\n"
                        + "{\"summary\":{}}\n"
                        + "{\"error\":{\"errors\":[{\"code\":{\"U\":\"Neo.ClientError.Request.InvalidFormat\"},\"message\":{\"U\":\"cause1\"}}]}}\n"
                        + "{\"info\":{\"commit\":\"commit/uri/1\"}}\n",
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
                "{\"header\":{\"fields\":[\"column1\",\"column2\"]}}\n"
                        + "{\"data\":[{\"U\":\"value1\"},{\"U\":\"value2\"}]}\n"
                        + "{\"summary\":{}}\n"
                        + "{\"error\":{\"errors\":[{\"code\":{\"U\":\"Neo.ClientError.Request.InvalidFormat\"},\"message\":{\"U\":\"cause1\"}}]}}\n"
                        + "{\"info\":{}}\n",
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
                "{\"error\":{\"errors\":[{\"code\":{\"U\":\"Neo.ClientError.Request.InvalidFormat\"},\"message\":{\"U\":\"cause1\"}}]}}\n"
                        + "{\"info\":{\"commit\":\"commit/uri/1\"}}\n",
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
                "{\"error\":{\"errors\":[{\"code\":{\"U\":\"Neo.ClientError.Request.InvalidFormat\"},\"message\":{\"U\":\"cause1\"}}]}}\n"
                        + "{\"info\":{}}\n",
                result);
    }

    @Test
    void shouldSerializeResponseWithNoCommitUriResultsOrErrors() {

        // when
        writeTransactionInfo(serializer);

        // then
        String result = output.toString(UTF_8);
        assertEquals("{\"info\":{}}\n", result);
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
                "{\"header\":{\"fields\":[\"column1\",\"column2\"]}}\n"
                        + "{\"data\":[{\"U\":\"value1\"},{\"U\":\"value2\"}]}\n"
                        + "{\"data\":[{\"U\":\"value3\"},{\"U\":\"value4\"}]}\n"
                        + "{\"summary\":{}}\n"
                        + "{\"info\":{}}\n",
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
                "{\"header\":{\"fields\":[\"column1\",\"column2\"]}}\n"
                        + "{\"data\":[{\"U\":\"value1\"},{\"U\":\"value2\"}]}\n"
                        + "{\"summary\":{}}\n"
                        + "{\"header\":{\"fields\":[\"column3\",\"column4\"]}}\n"
                        + "{\"data\":[{\"U\":\"value3\"},{\"U\":\"value4\"}]}\n"
                        + "{\"summary\":{}}\n"
                        + "{\"info\":{}}\n",
                result);
    }

    @Test
    void shouldSerializeNodeAsMapOfProperties() {
        // given
        var node = node(
                1,
                properties(
                        property("a", 12),
                        property("b", true),
                        property("c", new int[] {1, 0, 1, 2}),
                        property("d", new byte[] {1, 0, 1, 2}),
                        property("e", new String[] {"a", "b", "ääö"})));
        var row = Map.of("node", node);

        // when
        writeStatementStart(serializer, "node");
        writeRecord(serializer, row, "node");
        writeStatementEnd(serializer);
        writeTransactionInfo(serializer);

        // then
        String result = output.toString(UTF_8);
        assertEquals(
                "{\"header\":{\"fields\":[\"node\"]}}\n"
                        + "{\"data\":[{\"()\":[1,[],{\"a\":{\"Z\":\"12\"},\"b\":{\"?\":\"true\"},\"c\":[1,0,1,2],\"d\":{\"#\":\"01000102\"},"
                        + "\"e\":[{\"U\":\"a\"},{\"U\":\"b\"},{\"U\":\"ääö\"}]}]}]}\n"
                        + "{\"summary\":{}}\n"
                        + "{\"info\":{}}\n",
                result);
    }

    @Test
    void shouldHandleTransactionHandleStateCorrectly() throws Exception {

        // The serializer is stateful, as the underlying Neo4jJsonCodec uses a handle to the transaction.
        // Therefore, the JSON Factory must not be reused respectively the codec used on a factory cannot be changed
        // Otherwise, we will end up with transaction handles belonging to other threads.

        Function<Integer, Node> selectNode = i -> node(1, properties(property("i", i)));
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
            LineDelimitedEventSourceJoltSerializer serializer = getSerializerWith(output);

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
                var expectedResult =
                        "{\"header\":{\"fields\":[\"node\"]}}\n" + "{\"data\":[{\"()\":[1,[],{\"i\":{\"Z\":\""
                                + i + "\"}}]}]}\n" + "{\"summary\":{}}\n"
                                + "{\"info\":{}}\n";
                try {
                    var result = request.get();
                    assertEquals(expectedResult, result);
                } catch (ExecutionException e) {
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
        var a = node(1, properties(property("foo", 12)));
        var b = node(2, properties(property("bar", false)));
        var r = relationship(1, properties(property("baz", "quux")), a, "FRAZZLE", b);
        var row = Map.of("nested", new TreeMap<>(Map.of("node", a, "edge", r, "path", path(a, link(r, b)))));

        // when
        writeStatementStart(serializer, "nested");
        writeRecord(serializer, row, "nested");
        writeStatementEnd(serializer);
        writeTransactionInfo(serializer);

        // then
        String result = output.toString(UTF_8);
        assertEquals(
                "{\"header\":{\"fields\":[\"nested\"]}}\n"
                        + "{\"data\":[{\"{}\":{\"edge\":{\"->\":[1,1,\"FRAZZLE\",2,{\"baz\":{\"U\":\"quux\"}}]},"
                        + "\"node\":{\"()\":[1,[],{\"foo\":{\"Z\":\"12\"}}]},"
                        + "\"path\":{\"..\":[{\"()\":[1,[],{\"foo\":{\"Z\":\"12\"}}]},"
                        + "{\"->\":[1,1,\"FRAZZLE\",2,{\"baz\":{\"U\":\"quux\"}}]},"
                        + "{\"()\":[2,[],{\"bar\":{\"?\":\"false\"}}]}]}}}]}\n"
                        + "{\"summary\":{}}\n"
                        + "{\"info\":{}}\n",
                result);
    }

    @Test
    void shouldSerializePathAsListOfMapsOfProperties() {
        // given
        var path = mockPath(Map.of("key1", "value1"), Map.of("key2", "value2"), Map.of("key3", "value3"));
        var row = Map.of("path", path);

        var startNode = path.startNode();
        var endNode = path.endNode();
        var rel = path.lastRelationship();

        // when
        writeStatementStart(serializer, "path");
        writeRecord(serializer, row, "path");
        writeStatementEnd(serializer);
        writeTransactionInfo(serializer);

        // then
        String result = output.toString(UTF_8);
        assertEquals(
                "{\"header\":{\"fields\":[\"path\"]}}\n"
                        + "{\"data\":[{\"..\":[{\"()\":[1,[],{\"key1\":{\"U\":\"value1\"}}]},"
                        + "{\"->\":[1,1,\"RELATED\",2,{\"key2\":{\"U\":\"value2\"}}]},"
                        + "{\"()\":[2,[],{\"key3\":{\"U\":\"value3\"}}]}]}]}\n"
                        + "{\"summary\":{}}\n"
                        + "{\"info\":{}}\n",
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
                "{\"header\":{\"fields\":[\"geom\"]}}\n" + "{\"data\":[{\"@\":\"SRID=4326;POINT(12.3 45.6)\"}]}\n"
                        + "{\"data\":[{\"@\":\"SRID=7203;POINT(123.0 456.0)\"}]}\n"
                        + "{\"data\":[{\"@\":\"SRID=4979;POINT Z (12.3 45.6 78.9)\"}]}\n"
                        + "{\"data\":[{\"@\":\"SRID=9157;POINT Z (123.0 456.0 789.0)\"}]}\n"
                        + "{\"summary\":{}}\n"
                        + "{\"info\":{}}\n",
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
        var row6 = Map.of("temporal", DurationValue.duration(Duration.of(12, ChronoUnit.HOURS)));

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
                "{\"header\":{\"fields\":[\"temporal\"]}}\n" + "{\"data\":[{\"T\":\"2018-03-12\"}]}\n"
                        + "{\"data\":[{\"T\":\"2018-03-12T13:02:10.00000001+01:00[UTC+01:00]\"}]}\n"
                        + "{\"data\":[{\"T\":\"12:02:04.000000071Z\"}]}\n"
                        + "{\"data\":[{\"T\":\"2018-03-12T13:02:10.00000001\"}]}\n"
                        + "{\"data\":[{\"T\":\"13:02:10.00000001\"}]}\n"
                        + "{\"data\":[{\"T\":\"PT12H\"}]}\n"
                        + "{\"summary\":{}}\n"
                        + "{\"info\":{}}\n",
                result);
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
        writeStatementStart(serializer, "column1", "column2");
        writeRecord(serializer, row, "column1", "column2");
        var e = assertThrows(RuntimeException.class, () -> serializer.writeRecord(recordEvent));

        writeError(serializer, Status.Statement.ExecutionFailed, e.getMessage());
        writeTransactionInfo(serializer);

        // then
        String result = output.toString(UTF_8);
        assertEquals(
                "{\"header\":{\"fields\":[\"column1\",\"column2\"]}}\n"
                        + "{\"data\":[{\"U\":\"value1\"},{\"U\":\"value2\"}]}\n"
                        + "{\"data\":[]}\n"
                        + "{\"error\":{\"errors\":[{\"code\":{\"U\":\"Neo.DatabaseError.Statement.ExecutionFailed\"},\"message\":{\"U\":\"Stuff went wrong!\"}}]}}\n"
                        + "{\"info\":{}}\n",
                result);
    }

    @Test
    void shouldSerializePlanWithoutChildButAllKindsOfSupportedArguments() throws Exception {
        // given
        serializer = getSerializerWith(output);

        String operatorType = "Ich habe einen Plan!";

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
        writeStatementStart(serializer);
        writeRecord(serializer, Collections.emptyMap());
        writeStatementEnd(serializer, planDescription, Collections.emptyList());
        writeTransactionInfo(serializer);

        String resultString = output.toString(UTF_8);

        // then
        JsonNode plan =
                jsonNode(resultString.split("\n")[2]).get("summary").get("plan").get("root");

        assertTrue(asSet(
                        "operatorType",
                        "identifiers",
                        "children",
                        "string",
                        "bool",
                        "number",
                        "double",
                        "listOfInts",
                        "listOfListOfInts")
                .containsAll(Iterators.asList(plan.fieldNames())));

        assertEquals(wrapWithType("U", operatorType), plan.get("operatorType"));
        assertEquals(wrapWithType("U", args.get("string")), plan.get("string"));
        assertEquals(wrapWithType("?", args.get("bool")), plan.get("bool"));
        assertEquals(wrapWithType("Z", args.get("number")), plan.get("number"));
        assertEquals(wrapWithType("R", args.get("double")), plan.get("double"));
        assertEquals(jsonNode("{\"[]\":[{\"Z\":\"1\"},{\"Z\":\"2\"},{\"Z\":\"3\"}]}"), plan.get("listOfInts"));
        assertEquals(
                jsonNode("{\"[]\":[{\"[]\":[{\"Z\":\"1\"},{\"Z\":\"2\"},{\"Z\":\"3\"}]}]}"),
                plan.get("listOfListOfInts"));
    }

    @Test
    void shouldSerializePlanWithoutChildButWithIdentifiers() throws Exception {
        // given
        serializer = getSerializerWith(output);

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
        writeStatementEnd(serializer, planDescription, Collections.emptyList());
        writeTransactionInfo(serializer);

        String resultString = output.toString(UTF_8);

        // then
        JsonNode plan = jsonNode(resultString.split("\n")[2]).get("summary");
        JsonNode rootPlan = assertIsPlanRoot(plan);

        assertTrue(
                asSet("operatorType", "identifiers", "children").containsAll(Iterators.asList(rootPlan.fieldNames())));

        assertEquals(wrapWithType("U", operatorType), rootPlan.get("operatorType"));
        assertEquals(jsonNode("[{\"U\":\"id2\"},{\"U\":\"id1\"},{\"U\":\"id3\"}]"), rootPlan.get("identifiers"));
    }

    @Test
    void shouldSerializePlanWithChildren() throws Exception {
        // given
        serializer = getSerializerWith(output);

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
        writeStatementEnd(serializer, parent, Collections.emptyList());
        writeTransactionInfo(serializer);

        // then
        var result = output.toString(UTF_8);
        var endEventContents = jsonNode(result.split("\n")[2]).get("summary");
        JsonNode root = assertIsPlanRoot(endEventContents);

        assertEquals("parent", root.get("operatorType").get("U").asText());
        assertEquals(0, root.get("id").asLong());
        assertEquals(asSet(wrapWithType("U", parentId)), identifiersOf(root));

        Set<Integer> childIds = new HashSet<>();
        Set<Set<JsonNode>> identifiers = new HashSet<>();
        for (JsonNode child : root.get("children")) {
            assertTrue(child.isObject(), "Expected object");
            assertEquals("child", child.get("operatorType").get("U").asText());
            identifiers.add(identifiersOf(child));
            childIds.add(child.get("id").get("Z").asInt());
        }
        assertEquals(asSet(1, 2), childIds);
        assertEquals(asSet(asSet(wrapWithType("U", leftId)), asSet(wrapWithType("U", rightId))), identifiers);
    }

    @Test
    void shouldReturnNotifications() {
        // given
        Notification notification =
                NotificationCodeWithDescription.cartesianProduct(new InputPosition(1, 2, 3), "a", "(),()");
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
                "{\"header\":{\"fields\":[\"column1\",\"column2\"]}}\n"
                        + "{\"data\":[{\"U\":\"value1\"},{\"U\":\"value2\"}]}\n"
                        + "{\"summary\":{}}\n"
                        + "{\"info\":{\"notifications\":[{\"code\":\"Neo.ClientNotification.Statement.CartesianProduct\","
                        + "\"severity\":\"INFORMATION\","
                        + "\"title\":\"This query builds a cartesian product between disconnected patterns.\","
                        + "\"description\":\"If a part of a query contains multiple disconnected patterns, this will build a cartesian "
                        + "product between all those parts. This may produce a large amount of data and slow down query "
                        + "processing. While occasionally intended, it may often be possible to reformulate the query "
                        + "that avoids the use of this cross product, perhaps by adding a relationship between the "
                        + "different parts or by using OPTIONAL MATCH (a)\","
                        + "\"position\":{\"offset\":1,\"line\":2,\"column\":3}}],"
                        + "\"commit\":\"commit/uri/1\"}}\n",
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
        writeStatementEnd(serializer, null, Collections.emptyList());
        writeTransactionInfo(serializer, "commit/uri/1");

        // then
        String result = output.toString(UTF_8);

        assertEquals(
                "{\"header\":{\"fields\":[\"column1\",\"column2\"]}}\n"
                        + "{\"data\":[{\"U\":\"value1\"},{\"U\":\"value2\"}]}\n"
                        + "{\"summary\":{}}\n"
                        + "{\"info\":{\"commit\":\"commit/uri/1\"}}\n",
                result);
    }

    @Test
    void shouldReturnDeprecationNotification() {
        // given
        var joltV2Serializer = new LineDelimitedEventSourceJoltSerializer(
                Collections.emptyMap(),
                JoltV1Codec.class,
                true,
                JSON_FACTORY,
                output,
                LineDelimitedEventSourceJoltMessageBodyWriter.JSON_JOLT_MIME_TYPE_VALUE);
        var row = Map.of(
                "column1", "value1",
                "column2", "value2");

        // when
        writeStatementStart(joltV2Serializer, "column1", "column2");
        writeRecord(joltV2Serializer, row, "column1", "column2");
        writeStatementEnd(joltV2Serializer, null, Collections.emptyList());
        writeTransactionInfo(joltV2Serializer, "commit/uri/1");

        // then
        String result = output.toString(UTF_8);

        assertEquals(
                "{\"header\":{\"fields\":[\"column1\",\"column2\"]}}\n"
                        + "{\"data\":[{\"U\":\"value1\"},{\"U\":\"value2\"}]}\n"
                        + "{\"summary\":{}}\n"
                        + "{\"info\":{\"notifications\":[{\"code\":\"Neo.ClientNotification.Request.DeprecatedFormat\","
                        + "\"severity\":\"WARNING\",\"title\":\"The client made a request for a format which has "
                        + "been deprecated.\","
                        + "\"description\":\"The requested format has been deprecated. ('application/vnd.neo4j.jolt' "
                        + "and 'application/vnd.neo4j.jolt-v1' have been deprecated and will be removed in a future version. "
                        + "Please use 'application/vnd.neo4j.jolt-v2'.)\"}],\"commit\":\"commit/uri/1\"}}\n",
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
                "{\"header\":{\"fields\":[\"column1\",\"column2\"]}}\n"
                        + "{\"data\":[{\"U\":\"value1\"},{\"U\":\"value2\"}]}\n"
                        + "{\"summary\":{}}\n"
                        + "{\"info\":{\"notifications\":[{\"code\":\"Neo.ClientNotification.Statement.CartesianProduct\","
                        + "\"severity\":\"INFORMATION\",\"title\":\"This query builds a cartesian product between disconnected patterns.\","
                        + "\"description\":\"If a part of a query contains multiple disconnected patterns, this will build a cartesian "
                        + "product between all those parts. This may produce a large amount of data and slow down query "
                        + "processing. While occasionally intended, it may often be possible to reformulate the query "
                        + "that avoids the use of this cross product, perhaps by adding a relationship between the "
                        + "different parts or by using OPTIONAL MATCH (a)\"}],"
                        + "\"commit\":\"commit/uri/1\"}}\n",
                result);
    }

    protected static LineDelimitedEventSourceJoltSerializer getSerializerWith(OutputStream output) {
        return new LineDelimitedEventSourceJoltSerializer(
                Collections.emptyMap(), JoltV1Codec.class, true, JSON_FACTORY, output, null);
    }
}
