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
import java.util.Set;
import java.util.function.Consumer;
import org.apache.logging.log4j.util.TriConsumer;
import org.assertj.core.api.Assertions;
import org.assertj.core.data.Index;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.neo4j.bolt.protocol.common.fsm.response.MetadataConsumer;
import org.neo4j.bolt.testing.assertions.ListValueAssertions;
import org.neo4j.bolt.testing.assertions.MapValueAssertions;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.InputPosition;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.NotificationCategory;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.graphdb.SeverityLevel;
import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.kernel.database.NormalizedDatabaseName;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;

abstract class AbstractMetadataHandlerTest {

    private MetadataHandler handler;
    private MetadataConsumer consumer;

    @BeforeEach
    void prepare() {
        this.handler = this.createMetadataHandler();
        this.consumer = Mockito.mock(MetadataConsumer.class);
    }

    protected abstract MetadataHandler createMetadataHandler();

    @Test
    void shouldApplyStatementId() {
        this.handler.onStatementId(this.consumer, 42);

        Mockito.verify(this.consumer).onMetadata("qid", Values.longValue(42));
        Mockito.verifyNoMoreInteractions(this.consumer);
    }

    @Test
    void shouldApplyTimeSpentPreparingResults() {
        this.handler.onTimeSpentPreparingResults(this.consumer, 42);

        Mockito.verify(this.consumer).onMetadata("t_first", Values.longValue(42));
        Mockito.verifyNoMoreInteractions(this.consumer);
    }

    @Test
    void shouldApplyTimeSpentStreaming() {
        this.handler.onTimeSpentStreaming(this.consumer, 42);

        Mockito.verify(this.consumer).onMetadata("t_last", Values.longValue(42));
        Mockito.verifyNoMoreInteractions(this.consumer);
    }

    @Test
    void shouldApplyFieldNames() {
        this.handler.onFieldNames(this.consumer, List.of("firstName", "lastName", "age"));

        var captor = ArgumentCaptor.forClass(ListValue.class);

        Mockito.verify(this.consumer).onMetadata(Mockito.eq("fields"), captor.capture());
        Mockito.verifyNoMoreInteractions(this.consumer);

        ListValueAssertions.assertThat(captor.getValue())
                .containsExactly(
                        Values.stringValue("firstName"), Values.stringValue("lastName"), Values.stringValue("age"));
    }

    @Test
    void shouldApplyDatabase() {
        var databaseReference = Mockito.mock(DatabaseReference.class);
        var databaseName = new NormalizedDatabaseName("neo4j");

        Mockito.doReturn(databaseName).when(databaseReference).alias();

        this.handler.onDatabase(this.consumer, databaseReference);

        Mockito.verify(databaseReference).alias();

        Mockito.verify(this.consumer).onMetadata("db", Values.stringValue("neo4j"));
    }

    @Test
    void shouldApplyUpdateQueryStatistics() {
        var statistics = Mockito.mock(QueryStatistics.class);

        Mockito.doReturn(true).when(statistics).containsUpdates();

        Mockito.doReturn(1).when(statistics).getNodesCreated();
        Mockito.doReturn(2).when(statistics).getNodesDeleted();
        Mockito.doReturn(3).when(statistics).getRelationshipsCreated();
        Mockito.doReturn(4).when(statistics).getRelationshipsDeleted();
        Mockito.doReturn(5).when(statistics).getPropertiesSet();
        Mockito.doReturn(6).when(statistics).getLabelsAdded();
        Mockito.doReturn(7).when(statistics).getLabelsRemoved();
        Mockito.doReturn(8).when(statistics).getIndexesAdded();
        Mockito.doReturn(9).when(statistics).getIndexesRemoved();
        Mockito.doReturn(10).when(statistics).getConstraintsAdded();
        Mockito.doReturn(11).when(statistics).getConstraintsRemoved();

        Mockito.doReturn(1).when(statistics).getSystemUpdates();

        handler.onQueryStatistics(this.consumer, statistics);

        var captor = ArgumentCaptor.forClass(MapValue.class);

        Mockito.verify(this.consumer).onMetadata(Mockito.eq("stats"), captor.capture());
        Mockito.verifyNoMoreInteractions(this.consumer);

        this.verifyApplyUpdateQueryStatisticsResult(captor.getValue());
    }

    protected void verifyApplyUpdateQueryStatisticsResult(MapValue value) {
        MapValueAssertions.assertThat(value)
                .containsEntry("nodes-created", Values.longValue(1))
                .containsEntry("nodes-created", Values.longValue(1))
                .containsEntry("nodes-deleted", Values.longValue(2))
                .containsEntry("relationships-created", Values.longValue(3))
                .containsEntry("relationships-deleted", Values.longValue(4))
                .containsEntry("properties-set", Values.longValue(5))
                .containsEntry("labels-added", Values.longValue(6))
                .containsEntry("labels-removed", Values.longValue(7))
                .containsEntry("indexes-added", Values.longValue(8))
                .containsEntry("indexes-removed", Values.longValue(9))
                .containsEntry("constraints-added", Values.longValue(10))
                .containsEntry("constraints-removed", Values.longValue(11));
    }

    @Test
    void shouldOmitZeroUpdateQueryStatistics() {
        var statistics = Mockito.mock(QueryStatistics.class);

        Mockito.doReturn(true).when(statistics).containsUpdates();

        Mockito.doReturn(0).when(statistics).getNodesCreated();
        Mockito.doReturn(0).when(statistics).getNodesDeleted();
        Mockito.doReturn(0).when(statistics).getRelationshipsCreated();
        Mockito.doReturn(0).when(statistics).getRelationshipsDeleted();
        Mockito.doReturn(0).when(statistics).getPropertiesSet();
        Mockito.doReturn(0).when(statistics).getLabelsAdded();
        Mockito.doReturn(0).when(statistics).getLabelsRemoved();
        Mockito.doReturn(0).when(statistics).getIndexesAdded();
        Mockito.doReturn(0).when(statistics).getIndexesRemoved();
        Mockito.doReturn(0).when(statistics).getConstraintsAdded();
        Mockito.doReturn(0).when(statistics).getConstraintsRemoved();

        Mockito.doReturn(1).when(statistics).getSystemUpdates();

        this.handler.onQueryStatistics(consumer, statistics);

        var captor = ArgumentCaptor.forClass(MapValue.class);

        Mockito.verify(this.consumer).onMetadata(Mockito.eq("stats"), captor.capture());
        Mockito.verifyNoMoreInteractions(this.consumer);

        this.verifyOmitZeroUpdateQueryStatisticsResult(captor.getValue());
    }

    protected void verifyOmitZeroUpdateQueryStatisticsResult(MapValue value) {
        // we deliberately misuse the interface in order to generate this result here so that we can
        // verify that values are omitted when they are set to their default value - in production
        // containsUpdates will return false if no updates are performed
        MapValueAssertions.assertThat(value).isEmpty();
    }

    @Test
    void shouldApplySystemQueryStatistics() {
        var statistics = Mockito.mock(QueryStatistics.class);

        Mockito.doReturn(true).when(statistics).containsSystemUpdates();

        Mockito.doReturn(1).when(statistics).getSystemUpdates();

        Mockito.doReturn(1).when(statistics).getNodesCreated();
        Mockito.doReturn(2).when(statistics).getNodesDeleted();
        Mockito.doReturn(3).when(statistics).getRelationshipsCreated();
        Mockito.doReturn(4).when(statistics).getRelationshipsDeleted();
        Mockito.doReturn(5).when(statistics).getPropertiesSet();
        Mockito.doReturn(6).when(statistics).getLabelsAdded();
        Mockito.doReturn(7).when(statistics).getLabelsRemoved();
        Mockito.doReturn(8).when(statistics).getIndexesAdded();
        Mockito.doReturn(9).when(statistics).getIndexesRemoved();
        Mockito.doReturn(10).when(statistics).getConstraintsAdded();
        Mockito.doReturn(11).when(statistics).getConstraintsRemoved();

        this.handler.onQueryStatistics(this.consumer, statistics);

        var captor = ArgumentCaptor.forClass(MapValue.class);

        Mockito.verify(this.consumer).onMetadata(Mockito.eq("stats"), captor.capture());
        Mockito.verifyNoMoreInteractions(this.consumer);

        this.verifyApplySystemQueryStatisticsResult(captor.getValue());
    }

    protected void verifyApplySystemQueryStatisticsResult(MapValue value) {
        MapValueAssertions.assertThat(value).containsEntry("system-updates", Values.longValue(1));
    }

    @Test
    void shouldOmitZeroSystemQueryStatistics() {
        var statistics = Mockito.mock(QueryStatistics.class);

        Mockito.doReturn(true).when(statistics).containsSystemUpdates();

        Mockito.doReturn(1).when(statistics).getNodesCreated();
        Mockito.doReturn(2).when(statistics).getNodesDeleted();
        Mockito.doReturn(3).when(statistics).getRelationshipsCreated();
        Mockito.doReturn(4).when(statistics).getRelationshipsDeleted();
        Mockito.doReturn(5).when(statistics).getPropertiesSet();
        Mockito.doReturn(6).when(statistics).getLabelsAdded();
        Mockito.doReturn(7).when(statistics).getLabelsRemoved();
        Mockito.doReturn(8).when(statistics).getIndexesAdded();
        Mockito.doReturn(9).when(statistics).getIndexesRemoved();
        Mockito.doReturn(10).when(statistics).getConstraintsAdded();
        Mockito.doReturn(11).when(statistics).getConstraintsRemoved();

        Mockito.doReturn(0).when(statistics).getSystemUpdates();

        handler.onQueryStatistics(this.consumer, statistics);

        var captor = ArgumentCaptor.forClass(MapValue.class);

        Mockito.verify(this.consumer).onMetadata(Mockito.eq("stats"), captor.capture());
        Mockito.verifyNoMoreInteractions(this.consumer);

        this.verifyOmitZeroSystemQueryStatisticsResult(captor.getValue());
    }

    protected void verifyOmitZeroSystemQueryStatisticsResult(MapValue value) {
        // we deliberately misuse the interface in order to generate this result here so that we can
        // verify that values are omitted when they are set to their default value - in production
        // containsUpdates will return false if no updates are performed
        MapValueAssertions.assertThat(value).isEmpty();
    }

    @Test
    void shouldOmitZeroQueryStatistics() {
        var statistics = Mockito.mock(QueryStatistics.class);

        Mockito.doReturn(false).when(statistics).containsUpdates();
        Mockito.doReturn(false).when(statistics).containsSystemUpdates();

        Mockito.doReturn(1).when(statistics).getNodesCreated();
        Mockito.doReturn(2).when(statistics).getNodesDeleted();
        Mockito.doReturn(3).when(statistics).getRelationshipsCreated();
        Mockito.doReturn(4).when(statistics).getRelationshipsDeleted();
        Mockito.doReturn(5).when(statistics).getPropertiesSet();
        Mockito.doReturn(6).when(statistics).getLabelsAdded();
        Mockito.doReturn(7).when(statistics).getLabelsRemoved();
        Mockito.doReturn(8).when(statistics).getIndexesAdded();
        Mockito.doReturn(9).when(statistics).getIndexesRemoved();
        Mockito.doReturn(10).when(statistics).getConstraintsAdded();
        Mockito.doReturn(11).when(statistics).getConstraintsRemoved();

        Mockito.doReturn(1).when(statistics).getSystemUpdates();

        handler.onQueryStatistics(this.consumer, statistics);

        Mockito.verifyNoInteractions(this.consumer);
    }

    private ExecutionPlanDescription prepareExecutionPlanDescription(
            TriConsumer<ExecutionPlanDescription, ExecutionPlanDescription, ExecutionPlanDescription> configurer) {
        var args1 = Map.of("foo", "bar");
        var args2 = Map.of("foo", "baz");

        var ids1 = Set.of("foo", "bar");
        var ids2 = Set.of("foo", "baz");

        var child1 = Mockito.mock(ExecutionPlanDescription.class);
        var child2 = Mockito.mock(ExecutionPlanDescription.class);
        var root = Mockito.mock(ExecutionPlanDescription.class);

        Mockito.doReturn("root").when(root).getName();
        Mockito.doReturn(args1).when(root).getArguments();
        Mockito.doReturn(ids1).when(root).getIdentifiers();
        Mockito.doReturn(List.of(child1, child2)).when(root).getChildren();

        Mockito.doReturn("child1").when(child1).getName();
        Mockito.doReturn(args2).when(child1).getArguments();
        Mockito.doReturn(ids2).when(child1).getIdentifiers();

        Mockito.doReturn("child2").when(child2).getName();
        Mockito.doReturn(args1).when(child2).getArguments();
        Mockito.doReturn(ids1).when(child2).getIdentifiers();

        configurer.accept(root, child1, child2);

        return root;
    }

    private void verifyExecutionPlanDescriptionMetadata(
            MapValue value,
            Consumer<MapValue> rootAssertions,
            Consumer<MapValue> child1Assertions,
            Consumer<MapValue> child2Assertions) {
        MapValueAssertions.assertThat(value)
                .containsEntry("operatorType", Values.utf8Value("root"))
                .containsEntry("args", args -> Assertions.assertThat(args)
                        .asInstanceOf(MapValueAssertions.mapValue())
                        .hasSize(1)
                        .containsEntry("foo", Values.stringValue("bar")))
                .containsEntry("identifiers", ids -> Assertions.assertThat(ids)
                        .asInstanceOf(ListValueAssertions.listValue())
                        .containsOnly(Values.stringValue("foo"), Values.stringValue("bar")))
                .containsEntry("children", children -> Assertions.assertThat(children)
                        .asInstanceOf(ListValueAssertions.listValue())
                        .hasSize(2)
                        .satisfies(
                                child -> Assertions.assertThat(child)
                                        .asInstanceOf(MapValueAssertions.mapValue())
                                        .containsEntry("operatorType", Values.utf8Value("child1"))
                                        .containsEntry("args", args -> Assertions.assertThat(args)
                                                .asInstanceOf(MapValueAssertions.mapValue())
                                                .hasSize(1)
                                                .containsEntry("foo", Values.stringValue("baz")))
                                        .containsEntry("identifiers", ids -> Assertions.assertThat(ids)
                                                .asInstanceOf(ListValueAssertions.listValue())
                                                .containsOnly(Values.stringValue("foo"), Values.stringValue("baz")))
                                        .doesNotContainKey("children")
                                        .satisfies(child1Assertions),
                                Index.atIndex(0))
                        .satisfies(
                                child -> Assertions.assertThat(child)
                                        .asInstanceOf(MapValueAssertions.mapValue())
                                        .containsEntry("operatorType", Values.utf8Value("child2"))
                                        .containsEntry("args", args -> Assertions.assertThat(args)
                                                .asInstanceOf(MapValueAssertions.mapValue())
                                                .containsEntry("foo", Values.stringValue("bar")))
                                        .containsEntry("identifiers", ids -> Assertions.assertThat(ids)
                                                .asInstanceOf(ListValueAssertions.listValue())
                                                .containsOnly(Values.stringValue("foo"), Values.stringValue("bar")))
                                        .doesNotContainKey("children")
                                        .satisfies(child2Assertions),
                                Index.atIndex(1)))
                .satisfies(rootAssertions);
    }

    @Test
    void shouldApplyExecutionPlan() {
        this.handler.onExecutionPlan(consumer, this.prepareExecutionPlanDescription((root, child1, child2) -> {}));

        var captor = ArgumentCaptor.forClass(MapValue.class);
        Mockito.verify(this.consumer).onMetadata(Mockito.eq("plan"), captor.capture());

        this.verifyExecutionPlanDescriptionMetadata(
                captor.getValue(),
                root -> MapValueAssertions.assertThat(root)
                        .doesNotContainKey("dbHits")
                        .doesNotContainKey("pageCacheHits")
                        .doesNotContainKey("pageCacheMisses")
                        .doesNotContainKey("pageCacheHitRatio")
                        .doesNotContainKey("rows")
                        .doesNotContainKey("time"),
                child1 -> MapValueAssertions.assertThat(child1)
                        .doesNotContainKey("dbHits")
                        .doesNotContainKey("pageCacheHits")
                        .doesNotContainKey("pageCacheMisses")
                        .doesNotContainKey("pageCacheHitRatio")
                        .doesNotContainKey("rows")
                        .doesNotContainKey("time"),
                child2 -> MapValueAssertions.assertThat(child2)
                        .doesNotContainKey("dbHits")
                        .doesNotContainKey("pageCacheHits")
                        .doesNotContainKey("pageCacheMisses")
                        .doesNotContainKey("pageCacheHitRatio")
                        .doesNotContainKey("rows")
                        .doesNotContainKey("time"));
    }

    @Test
    void shouldApplyExecutionPlanWhenProfilerInformationIsAvailable() {
        var profiler = Mockito.mock(ExecutionPlanDescription.ProfilerStatistics.class);

        Mockito.doReturn(true).when(profiler).hasDbHits();
        Mockito.doReturn(14L).when(profiler).getDbHits();
        Mockito.doReturn(true).when(profiler).hasPageCacheStats();
        Mockito.doReturn(21L).when(profiler).getPageCacheHits();
        Mockito.doReturn(42L).when(profiler).getPageCacheMisses();
        Mockito.doReturn(84.42).when(profiler).getPageCacheHitRatio();
        Mockito.doReturn(true).when(profiler).hasRows();
        Mockito.doReturn(13L).when(profiler).getRows();
        Mockito.doReturn(true).when(profiler).hasTime();
        Mockito.doReturn(18L).when(profiler).getTime();

        this.handler.onExecutionPlan(consumer, this.prepareExecutionPlanDescription((root, child1, child2) -> {
            Mockito.doReturn(true).when(root).hasProfilerStatistics();
            Mockito.doReturn(profiler).when(root).getProfilerStatistics();

            Mockito.doReturn(true).when(child1).hasProfilerStatistics();
            Mockito.doReturn(profiler).when(child1).getProfilerStatistics();
        }));

        var captor = ArgumentCaptor.forClass(MapValue.class);
        Mockito.verify(this.consumer).onMetadata(Mockito.eq("profile"), captor.capture());

        this.verifyExecutionPlanDescriptionMetadata(
                captor.getValue(),
                root -> MapValueAssertions.assertThat(root)
                        .containsEntry("dbHits", Values.longValue(14))
                        .containsEntry("pageCacheHits", Values.longValue(21))
                        .containsEntry("pageCacheMisses", Values.longValue(42))
                        .containsEntry("pageCacheHitRatio", Values.doubleValue(84.42))
                        .containsEntry("rows", Values.longValue(13))
                        .containsEntry("time", Values.longValue(18)),
                child1 -> MapValueAssertions.assertThat(child1)
                        .containsEntry("dbHits", Values.longValue(14))
                        .containsEntry("pageCacheHits", Values.longValue(21))
                        .containsEntry("pageCacheMisses", Values.longValue(42))
                        .containsEntry("pageCacheHitRatio", Values.doubleValue(84.42))
                        .containsEntry("rows", Values.longValue(13))
                        .containsEntry("time", Values.longValue(18)),
                child2 -> MapValueAssertions.assertThat(child2)
                        .doesNotContainKey("dbHits")
                        .doesNotContainKey("pageCacheHits")
                        .doesNotContainKey("pageCacheMisses")
                        .doesNotContainKey("pageCacheHitRatio")
                        .doesNotContainKey("rows")
                        .doesNotContainKey("time"));
    }

    @Test
    void shouldApplyRemainingResults() {
        this.handler.onResultsRemaining(this.consumer, false);

        Mockito.verifyNoInteractions(this.consumer);

        this.handler.onResultsRemaining(this.consumer, true);

        Mockito.verify(this.consumer).onMetadata("has_more", BooleanValue.TRUE);
    }

    @Test
    void shouldApplyBookmark() {
        this.handler.onBookmark(this.consumer, "abcdef0123456789");

        Mockito.verify(this.consumer).onMetadata("bookmark", Values.utf8Value("abcdef0123456789"));
    }

    @Test
    void shouldApplyNotifications() {
        var notification1 = Mockito.mock(Notification.class);
        var notification2 = Mockito.mock(Notification.class);

        Mockito.doReturn(new InputPosition(5, 42, 3)).when(notification1).getPosition();
        Mockito.doReturn("Neo4j.Test.ThingsHappened").when(notification1).getCode();
        Mockito.doReturn("Something Happened").when(notification1).getTitle();
        Mockito.doReturn("Things may have happened and you have been notified")
                .when(notification1)
                .getDescription();
        Mockito.doReturn(SeverityLevel.WARNING).when(notification1).getSeverity();
        Mockito.doReturn(NotificationCategory.DEPRECATION).when(notification1).getCategory();

        Mockito.doReturn(InputPosition.empty).when(notification2).getPosition();
        Mockito.doReturn("Neo4j.Test.OtherThings").when(notification2).getCode();
        Mockito.doReturn("Something else").when(notification2).getTitle();
        Mockito.doReturn("Oh boy").when(notification2).getDescription();
        Mockito.doReturn(SeverityLevel.INFORMATION).when(notification2).getSeverity();
        Mockito.doReturn(NotificationCategory.HINT).when(notification2).getCategory();

        this.handler.onNotifications(this.consumer, List.of(notification1, notification2));

        var captor = ArgumentCaptor.forClass(ListValue.class);
        Mockito.verify(this.consumer).onMetadata(Mockito.eq("notifications"), captor.capture());

        ListValueAssertions.assertThat(captor.getValue())
                .hasSize(2)
                .satisfies(
                        notification -> Assertions.assertThat(notification)
                                .asInstanceOf(MapValueAssertions.mapValue())
                                .hasSize(6)
                                .containsEntry("code", Values.utf8Value("Neo4j.Test.ThingsHappened"))
                                .containsEntry("title", Values.utf8Value("Something Happened"))
                                .containsEntry(
                                        "description",
                                        Values.utf8Value("Things may have happened and you have been notified"))
                                .containsEntry("severity", Values.utf8Value("WARNING"))
                                .containsEntry("category", Values.utf8Value("DEPRECATION"))
                                .containsEntry("position", position -> Assertions.assertThat(position)
                                        .asInstanceOf(MapValueAssertions.mapValue())
                                        .hasSize(3)
                                        .containsEntry("offset", Values.longValue(5))
                                        .containsEntry("line", Values.longValue(42))
                                        .containsEntry("column", Values.longValue(3))),
                        Index.atIndex(0))
                .satisfies(
                        notification -> Assertions.assertThat(notification)
                                .asInstanceOf(MapValueAssertions.mapValue())
                                .hasSize(5)
                                .containsEntry("code", Values.utf8Value("Neo4j.Test.OtherThings"))
                                .containsEntry("title", Values.utf8Value("Something else"))
                                .containsEntry("description", Values.utf8Value("Oh boy"))
                                .containsEntry("severity", Values.utf8Value("INFORMATION"))
                                .containsEntry("category", Values.utf8Value("HINT"))
                                .doesNotContainKey("position"),
                        Index.atIndex(1));
    }

    @Test
    void shouldOmitNotificationsWhenEmpty() {
        this.handler.onNotifications(this.consumer, Collections.emptyList());

        Mockito.verifyNoInteractions(this.consumer);
    }
}
