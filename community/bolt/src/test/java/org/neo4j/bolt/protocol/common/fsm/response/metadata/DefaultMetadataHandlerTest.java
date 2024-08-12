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
package org.neo4j.bolt.protocol.common.fsm.response.metadata;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.assertj.core.data.Index;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.neo4j.bolt.testing.assertions.ListValueAssertions;
import org.neo4j.bolt.testing.assertions.MapValueAssertions;
import org.neo4j.gqlstatus.NotificationClassification;
import org.neo4j.graphdb.GqlStatusObject;
import org.neo4j.graphdb.SeverityLevel;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;

class DefaultMetadataHandlerTest extends AbstractMetadataHandlerTest {
    private static final Map<String, Object> DEFAULT_DIAGNOSTIC_RECORD = Map.of(
            "OPERATION", "",
            "OPERATION_CODE", "0",
            "CURRENT_SCHEMA", "/");

    @Override
    protected MetadataHandler createMetadataHandler() {
        return DefaultMetadataHandler.getInstance();
    }

    @Test
    void shouldApplyGqlStatusObject() {
        var gqlStatusObject0 = Mockito.mock(GqlStatusObject.class);
        var gqlStatusObject1 = Mockito.mock(GqlStatusObjectAndNotification.class);
        var gqlStatusObject2 = Mockito.mock(GqlStatusObjectAndNotification.class);
        var gqlStatusObject3 = Mockito.mock(GqlStatusObjectAndNotification.class);

        Mockito.doReturn("00000").when(gqlStatusObject0).gqlStatus();
        Mockito.doReturn("Success").when(gqlStatusObject0).statusDescription();
        Mockito.doReturn(DEFAULT_DIAGNOSTIC_RECORD).when(gqlStatusObject0).diagnosticRecord();

        Mockito.doReturn("01N22").when(gqlStatusObject1).gqlStatus();
        Mockito.doReturn("warn: something warned is might not right.")
                .when(gqlStatusObject1)
                .statusDescription();
        Mockito.doReturn("Neo4j.Test.ThingsHappened").when(gqlStatusObject1).getCode();
        Mockito.doReturn("Something Happened").when(gqlStatusObject1).getTitle();
        Mockito.doReturn("Notification description").when(gqlStatusObject1).getDescription();
        Mockito.doReturn(diagnosticRecordFromDefault(
                        Map.entry("_classification", NotificationClassification.DEPRECATION.name()),
                        Map.entry("_severity", SeverityLevel.WARNING.name()),
                        Map.entry("_position", Map.of("offset", 5, "line", 42, "column", 3))))
                .when(gqlStatusObject1)
                .diagnosticRecord();

        Mockito.doReturn("01N23").when(gqlStatusObject2).gqlStatus();
        Mockito.doReturn("info: something warned is might not right tii.")
                .when(gqlStatusObject2)
                .statusDescription();
        Mockito.doReturn("Neo4j.Test.OtherThings").when(gqlStatusObject2).getCode();
        Mockito.doReturn("Something else").when(gqlStatusObject2).getTitle();
        Mockito.doReturn("Other notification description")
                .when(gqlStatusObject2)
                .getDescription();
        Mockito.doReturn(diagnosticRecordFromDefault(
                        Map.entry("_classification", NotificationClassification.HINT.name()),
                        Map.entry("_severity", SeverityLevel.INFORMATION.name()),
                        Map.entry(
                                "_position",
                                Map.of(
                                        "line", -1,
                                        "offset", -1,
                                        "column", -1)),
                        Map.entry(
                                "_status_parameters",
                                Map.of("int", 1, "string", "2", "boolean", true, "listOfString", List.of("b", "c")))))
                .when(gqlStatusObject2)
                .diagnosticRecord();

        Mockito.doReturn("50N22").when(gqlStatusObject3).gqlStatus();
        Mockito.doReturn("err?: something error is might not right.")
                .when(gqlStatusObject3)
                .statusDescription();
        Mockito.doReturn(Map.of(
                        "OPERATION", "A",
                        "OPERATION_CODE", "B",
                        "CURRENT_SCHEMA", "/C"))
                .when(gqlStatusObject3)
                .diagnosticRecord();

        this.handler.onNotifications(
                this.consumer,
                Collections.emptyList(),
                List.of(gqlStatusObject0, gqlStatusObject1, gqlStatusObject2, gqlStatusObject3));

        var captor = ArgumentCaptor.forClass(ListValue.class);
        Mockito.verify(this.consumer).onMetadata(Mockito.eq("statuses"), captor.capture());

        ListValueAssertions.assertThat(captor.getValue())
                .hasSize(4)
                .satisfies(
                        status -> Assertions.assertThat(status)
                                .asInstanceOf(MapValueAssertions.mapValue())
                                .hasSize(2)
                                .containsEntry("gql_status", Values.utf8Value("00000"))
                                .containsEntry("status_description", Values.utf8Value("Success")),
                        Index.atIndex(0))
                .satisfies(
                        status -> Assertions.assertThat(status)
                                .asInstanceOf(MapValueAssertions.mapValue())
                                .hasSize(6)
                                .containsEntry("gql_status", Values.utf8Value("01N22"))
                                .containsEntry(
                                        "status_description",
                                        Values.utf8Value("warn: something warned is might not right."))
                                .containsEntry("neo4j_code", Values.utf8Value("Neo4j.Test.ThingsHappened"))
                                .containsEntry("title", Values.utf8Value("Something Happened"))
                                .containsEntry("description", Values.utf8Value("Notification description"))
                                .containsEntry("diagnostic_record", diagnosticRecord -> Assertions.assertThat(
                                                diagnosticRecord)
                                        .asInstanceOf(MapValueAssertions.mapValue())
                                        .hasSize(3)
                                        .containsEntry(
                                                "_classification",
                                                Values.utf8Value(NotificationClassification.DEPRECATION.name()))
                                        .containsEntry("_severity", Values.utf8Value(SeverityLevel.WARNING.name()))
                                        .containsEntry("_position", position -> Assertions.assertThat(position)
                                                .asInstanceOf(MapValueAssertions.mapValue())
                                                .hasSize(3)
                                                .containsEntry("offset", Values.longValue(5))
                                                .containsEntry("line", Values.longValue(42))
                                                .containsEntry("column", Values.longValue(3)))),
                        Index.atIndex(1))
                .satisfies(
                        status -> Assertions.assertThat(status)
                                .asInstanceOf(MapValueAssertions.mapValue())
                                .hasSize(6)
                                .containsEntry("gql_status", Values.utf8Value("01N23"))
                                .containsEntry(
                                        "status_description",
                                        Values.utf8Value("info: something warned is might not right tii."))
                                .containsEntry("neo4j_code", Values.utf8Value("Neo4j.Test.OtherThings"))
                                .containsEntry("title", Values.utf8Value("Something else"))
                                .containsEntry("description", Values.utf8Value("Other notification description"))
                                .containsEntry("diagnostic_record", diagnosticRecord -> Assertions.assertThat(
                                                diagnosticRecord)
                                        .asInstanceOf(MapValueAssertions.mapValue())
                                        .hasSize(3)
                                        .containsEntry(
                                                "_classification",
                                                Values.utf8Value(NotificationClassification.HINT.name()))
                                        .containsEntry("_severity", Values.utf8Value(SeverityLevel.INFORMATION.name()))
                                        .containsEntry("_status_parameters", statusParameters -> Assertions.assertThat(
                                                        statusParameters)
                                                .asInstanceOf(MapValueAssertions.mapValue())
                                                .hasSize(4)
                                                .containsEntry("int", Values.intValue(1))
                                                .containsEntry("string", Values.utf8Value("2"))
                                                .containsEntry("boolean", Values.booleanValue(true))
                                                .containsEntry("listOfString", list -> ListValueAssertions.assertThat(
                                                                (ListValue) list)
                                                        .hasSize(2)
                                                        .contains(Values.utf8Value("b"), Index.atIndex(0))
                                                        .contains(Values.utf8Value("c"), Index.atIndex(1))))),
                        Index.atIndex(2))
                .satisfies(
                        status -> Assertions.assertThat(status)
                                .asInstanceOf(MapValueAssertions.mapValue())
                                .hasSize(3)
                                .containsEntry("gql_status", Values.utf8Value("50N22"))
                                .containsEntry(
                                        "status_description",
                                        Values.utf8Value("err?: something error is might not right."))
                                .containsEntry(
                                        "diagnostic_record", diagnosticRecord -> Assertions.assertThat(diagnosticRecord)
                                                .asInstanceOf(MapValueAssertions.mapValue())
                                                .hasSize(3)
                                                .containsEntry("OPERATION", Values.utf8Value("A"))
                                                .containsEntry("OPERATION_CODE", Values.utf8Value("B"))
                                                .containsEntry("CURRENT_SCHEMA", Values.utf8Value("/C"))),
                        Index.atIndex(3));
    }

    @Test
    void shouldOmitGqlNotificationsWhenEmpty() {
        this.handler.onNotifications(this.consumer, Collections.emptyList(), Collections.emptyList());

        Mockito.verifyNoInteractions(this.consumer);
    }

    @Override
    protected void verifyApplyUpdateQueryStatisticsResult(MapValue value) {
        super.verifyApplyUpdateQueryStatisticsResult(value);

        MapValueAssertions.assertThat(value)
                // system updates and regular updates don't mix
                .hasSize(12);
    }

    @Override
    protected void verifyOmitZeroUpdateQueryStatisticsResult(MapValue value) {
        MapValueAssertions.assertThat(value).hasSize(1).containsEntry("contains-updates", BooleanValue.TRUE);
    }

    @Override
    protected void verifyOmitZeroSystemQueryStatisticsResult(MapValue value) {
        MapValueAssertions.assertThat(value).hasSize(1).containsEntry("contains-system-updates", BooleanValue.TRUE);
    }

    @Override
    protected void verifyApplySystemQueryStatisticsResult(MapValue value) {
        super.verifyApplySystemQueryStatisticsResult(value);

        MapValueAssertions.assertThat(value)
                .containsEntry("contains-system-updates", BooleanValue.TRUE)
                // system updates and regular updates don't mix
                .hasSize(2);
    }

    @SafeVarargs
    protected static Map<String, Object> diagnosticRecordFromDefault(Map.Entry<String, Object>... entries) {
        return Stream.concat(DEFAULT_DIAGNOSTIC_RECORD.entrySet().stream(), Stream.of(entries))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
