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
package org.neo4j.bolt.local_channel;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.bolt.test.annotation.BoltTestExtension;
import org.neo4j.bolt.test.annotation.connection.initializer.Connected;
import org.neo4j.bolt.test.annotation.connection.transport.IncludeTransport;
import org.neo4j.bolt.test.annotation.test.TransportTest;
import org.neo4j.bolt.testing.client.BoltTestConnection;
import org.neo4j.bolt.testing.client.TransportType;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.boltmessages.AccessMode;
import org.neo4j.boltmessages.notifications.DisabledNotificationsConfig;
import org.neo4j.boltmessages.request.streaming.PullMessage;
import org.neo4j.boltmessages.request.transaction.BeginMessage;
import org.neo4j.boltmessages.request.transaction.CommitMessage;
import org.neo4j.boltmessages.request.transaction.RollbackMessage;
import org.neo4j.boltmessages.request.transaction.RunMessage;
import org.neo4j.notifications.StandardGqlStatusObject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.StringValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.ListValueBuilder;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.PathValue;
import org.neo4j.values.virtual.RelationshipValue;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
@BoltTestExtension
@IncludeTransport({TransportType.LOCAL})
@ParameterizedClass
@MethodSource("values")
public class Neo4jValuesLocalChannelIT extends AbstractLocalChannelIT {
    private final AnyValue param;

    public Neo4jValuesLocalChannelIT(AnyValue param) {
        this.param = param;
    }

    @TransportTest
    void shouldRunSimpleQuery(@Connected BoltTestConnection connection) throws Exception {
        connection.unwired(unwired -> {
            authenticate(unwired);

            var parametersBuilder = new MapValueBuilder();
            parametersBuilder.add("param", param);
            unwired.sendRequest(new RunMessage(
                            "RETURN $param as value",
                            parametersBuilder.build(),
                            List.of(),
                            Duration.of(10, ChronoUnit.SECONDS),
                            AccessMode.READ,
                            Map.of(),
                            "neo4j",
                            null,
                            DisabledNotificationsConfig.getInstance()))
                    .sendRequest(new PullMessage(1000, -1));

            assertSuccess(unwired.receiveResponse(), assertSuccessHasTFirst(), assertSuccessHasFields("value"));
            assertRecord(unwired.receiveResponse(), param);
            assertSuccess(
                    unwired.receiveResponse(),
                    assertSuccessDb("neo4j"),
                    assertSuccessType("r"),
                    assertSuccessHasTLast(),
                    assertSuccessHasBookmark(),
                    assertSuccessHasStatuses(status(
                            StandardGqlStatusObject.SUCCESS.gqlStatus(),
                            StandardGqlStatusObject.SUCCESS.statusDescription())));
        });
    }

    @TransportTest
    void shouldRunSingleQueryInTx(@Connected BoltTestConnection connection) throws Exception {
        connection.unwired(unwired -> {
            authenticate(unwired);

            var parametersBuilder = new MapValueBuilder();
            parametersBuilder.add("param", param);

            unwired.sendRequest(new BeginMessage(
                            List.of(), Duration.of(10, ChronoUnit.SECONDS), AccessMode.READ, Map.of(), "neo4j"))
                    .sendRequest(new RunMessage("RETURN $param as value", parametersBuilder.build()))
                    .sendRequest(new PullMessage(1000, -1))
                    .sendRequest(CommitMessage.getInstance());

            assertSuccess(unwired.receiveResponse(), assertSuccessEmpty());
            assertSuccess(unwired.receiveResponse(), assertSuccessHasTFirst(), assertSuccessHasFields("value"));

            assertRecord(unwired.receiveResponse(), param);
            assertSuccess(
                    unwired.receiveResponse(),
                    assertSuccessHasTLast(),
                    assertSuccessDb("neo4j"),
                    assertSuccessType("r"),
                    assertSuccessHasStatuses(status(
                            StandardGqlStatusObject.SUCCESS.gqlStatus(),
                            StandardGqlStatusObject.SUCCESS.statusDescription())));
            assertSuccess(unwired.receiveResponse(), assertSuccessHasBookmark());
        });
    }

    @TransportTest
    void shouldEchoDataInNodeQueryInTx(@Connected BoltTestConnection connection) throws Exception {
        assumeIsNotMapOrHeterogeneousList();

        connection.unwired(unwired -> {
            var expectedLabels = new String[] {"TheNodeLabel"};
            var expectedProperties = Map.of("param", param);

            authenticate(unwired);

            var parametersBuilder = new MapValueBuilder();
            parametersBuilder.add("param", param);

            unwired.sendRequest(new BeginMessage(
                            List.of(), Duration.of(10, ChronoUnit.SECONDS), AccessMode.WRITE, Map.of(), "neo4j"))
                    .sendRequest(new RunMessage(
                            "CREATE (n:TheNodeLabel{ param:$param }) RETURN n as value", parametersBuilder.build()))
                    .sendRequest(new PullMessage(1000, -1))
                    .sendRequest(RollbackMessage.getInstance());

            assertSuccess(unwired.receiveResponse(), assertSuccessEmpty());
            assertSuccess(unwired.receiveResponse(), assertSuccessHasTFirst(), assertSuccessHasFields("value"));

            assertRecord(unwired.receiveResponse(), assertNode(expectedLabels, expectedProperties));

            assertSuccess(
                    unwired.receiveResponse(),
                    assertSuccessHasTLast(),
                    assertSuccessDb("neo4j"),
                    assertSuccessType("rw"),
                    assertSuccessHasStatuses(status(
                            StandardGqlStatusObject.SUCCESS.gqlStatus(),
                            StandardGqlStatusObject.SUCCESS.statusDescription())));
            assertSuccess(unwired.receiveResponse(), assertSuccessEmpty());
        });
    }

    @TransportTest
    void shouldEchoDataInRelQueryInTx(@Connected BoltTestConnection connection) throws Exception {
        assumeIsNotMapOrHeterogeneousList();
        var expectedType = "TheRelType";
        var expectedProperties = Map.of("param", param);

        connection.unwired(unwired -> {
            authenticate(unwired);

            var parametersBuilder = new MapValueBuilder();
            parametersBuilder.add("param", param);

            unwired.sendRequest(new BeginMessage(
                            List.of(), Duration.of(10, ChronoUnit.SECONDS), AccessMode.WRITE, Map.of(), "neo4j"))
                    .sendRequest(new RunMessage(
                            "CREATE (:Start)-[r:TheRelType{ param:$param }]->(:End) RETURN r as value",
                            parametersBuilder.build()))
                    .sendRequest(new PullMessage(1000, -1))
                    .sendRequest(RollbackMessage.getInstance());

            assertSuccess(unwired.receiveResponse(), assertSuccessEmpty());
            assertSuccess(unwired.receiveResponse(), assertSuccessHasTFirst(), assertSuccessHasFields("value"));

            assertRecord(unwired.receiveResponse(), assertRelationship(expectedType, expectedProperties));

            assertSuccess(
                    unwired.receiveResponse(),
                    assertSuccessHasTLast(),
                    assertSuccessDb("neo4j"),
                    assertSuccessType("rw"),
                    assertSuccessHasStatuses(status(
                            StandardGqlStatusObject.SUCCESS.gqlStatus(),
                            StandardGqlStatusObject.SUCCESS.statusDescription())));
            assertSuccess(unwired.receiveResponse(), assertSuccessEmpty());
        });
    }

    @TransportTest
    void shouldEchoDataInPathQueryInTx(@Connected BoltTestConnection connection) throws Exception {
        assumeIsNotMapOrHeterogeneousList();

        connection.unwired(unwired -> {
            authenticate(unwired);

            var parametersBuilder = new MapValueBuilder();
            parametersBuilder.add("param", param);

            unwired.sendRequest(new BeginMessage(
                            List.of(), Duration.of(10, ChronoUnit.SECONDS), AccessMode.WRITE, Map.of(), "neo4j"))
                    .sendRequest(new RunMessage(
                            "CREATE p=(:Start)-[:TheRelType{ param:$param }]->(:End) RETURN p as value",
                            parametersBuilder.build()))
                    .sendRequest(new PullMessage(1000, -1))
                    .sendRequest(RollbackMessage.getInstance());

            assertSuccess(unwired.receiveResponse(), assertSuccessEmpty());
            assertSuccess(unwired.receiveResponse(), assertSuccessHasTFirst(), assertSuccessHasFields("value"));

            assertRecord(unwired.receiveResponse(), assertPathValue());

            assertSuccess(
                    unwired.receiveResponse(),
                    assertSuccessHasTLast(),
                    assertSuccessDb("neo4j"),
                    assertSuccessType("rw"),
                    assertSuccessHasStatuses(status(
                            StandardGqlStatusObject.SUCCESS.gqlStatus(),
                            StandardGqlStatusObject.SUCCESS.statusDescription())));
            assertSuccess(unwired.receiveResponse(), assertSuccessEmpty());
        });
    }

    private void assumeIsNotMapOrHeterogeneousList() {
        Assumptions.assumeTrue(
                !(this.param instanceof MapValue) && !(param instanceof ListValue),
                "Maps and heterogeneous lists are not supported as Node properties");
    }

    private static BiConsumer<Integer, AnyValue> assertNode(
            String[] expectedLabels, Map<String, AnyValue> expectedProperties) {
        return (i, anyValue) -> {
            assertThat(anyValue).isInstanceOf(NodeValue.class);
            var nodeValue = (NodeValue) anyValue;

            var labels = Stream.iterate(0, j -> j + 1)
                    .limit(nodeValue.labels().intSize())
                    .map(nodeValue.labels()::stringValue)
                    .map(StringValue::stringValue)
                    .toArray(String[]::new);

            var properties = mapValueToMap(nodeValue.properties());

            assertThat(nodeValue.elementId())
                    .as(() -> "Node element id is empty for field %d".formatted(i))
                    .isNotEmpty();
            assertThat(labels).as(() -> "Labels for field %d".formatted(i)).containsExactly(expectedLabels);
            assertThat(properties)
                    .as(() -> "Properties for field %d".formatted(i))
                    .isEqualTo(expectedProperties);
        };
    }

    private static BiConsumer<Integer, AnyValue> assertRelationship(
            String expectedTypeName, Map<String, AnyValue> expectedProperties) {
        return (i, anyValue) -> {
            assertThat(anyValue).isInstanceOf(RelationshipValue.class);
            var relationshipValue = (RelationshipValue) anyValue;

            var properties = mapValueToMap(relationshipValue.properties());

            assertThat(relationshipValue.elementId())
                    .as(() -> "Relationship element id is empty for field %d".formatted(i))
                    .isNotEmpty();
            assertThat(relationshipValue.startNodeElementId())
                    .as(() -> "Relationship start node element id is empty for field %d".formatted(i))
                    .isNotEmpty();
            assertThat(relationshipValue.endNodeElementId())
                    .as(() -> "Relationship end node element id is empty for field %d".formatted(i))
                    .isNotEmpty();

            assertThat(relationshipValue.type().asStringValue().stringValue())
                    .as(() -> "Type for field %d".formatted(i))
                    .isEqualTo(expectedTypeName);
            assertThat(properties)
                    .as(() -> "Properties for field %d".formatted(i))
                    .isEqualTo(expectedProperties);
        };
    }

    private static BiConsumer<Integer, AnyValue> assertPathValue() {
        return (i, anyValue) -> {
            assertThat(anyValue).isInstanceOf(PathValue.class);
            var pathValue = (PathValue) anyValue;

            assertThat(pathValue.startNode().elementId())
                    .as(() -> "Path start node element id is empty for field %d".formatted(i))
                    .isNotEmpty();
            assertThat(pathValue.endNode().elementId())
                    .as(() -> "Path end node element id is empty for field %d".formatted(i))
                    .isNotEmpty();

            for (var node : pathValue.nodes()) {
                assertThat(node.elementId())
                        .as(() -> "Path node element id is empty for field %d".formatted(i))
                        .isNotEmpty();
            }

            for (var relationship : pathValue.relationships()) {
                assertThat(relationship.elementId())
                        .as(() -> "Path relationship element id is empty for field %d".formatted(i))
                        .isNotEmpty();
            }
        };
    }

    private static HashMap<String, AnyValue> mapValueToMap(MapValue mapValue) {
        var properties = new HashMap<String, AnyValue>(mapValue.size());
        for (var key : mapValue.keySet()) {
            properties.put(key, mapValue.get(key));
        }
        return properties;
    }

    static Stream<Arguments> values() {
        var listValueBuilder = ListValueBuilder.newListBuilder(3);
        listValueBuilder.add(Values.stringValue("zero"));
        listValueBuilder.add(Values.booleanValue(true));
        listValueBuilder.add(Values.intValue(2));
        var listValue = listValueBuilder.build();

        var mapValueBuilder = new MapValueBuilder();
        mapValueBuilder.add("key0", Values.stringValue("zero"));
        mapValueBuilder.add("key1", listValue);
        mapValueBuilder.add("key2", Values.booleanValue(false));
        var mapValue = mapValueBuilder.build();

        return Stream.of(
                        Values.intValue(123),
                        Values.doubleValue(-1345.6),
                        Values.floatValue(-133.5f),
                        Values.longValue(101199191211311L),
                        Values.byteValue((byte) 0x60),
                        Values.shortValue((short) -12),
                        Values.booleanValue(false),
                        Values.stringValue("GOGO, OBJECTS!"),
                        Values.durationValue(Duration.ofMinutes(10)),
                        Values.pointValue(CoordinateReferenceSystem.CARTESIAN_3D, 1, 2, -3),
                        Values.uuidValue(UUID.randomUUID()),
                        Values.int32Vector(1, 2, 4),
                        Values.float32Vector(12, -12.2f, 3, -4246.6f),
                        Values.stringArray("ada", "love", "lace"),
                        Values.of(java.time.ZonedDateTime.parse(
                                "0800-01-01T00:00:00Z[Etc/GMT]", DateTimeFormatter.ISO_ZONED_DATE_TIME)),
                        listValue,
                        mapValue)
                .map(Arguments::of);
    }
}
