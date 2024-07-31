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

import static org.neo4j.values.storable.Values.doubleValue;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.utf8Value;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import org.neo4j.bolt.protocol.common.fsm.response.MetadataConsumer;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.GqlStatusObject;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.ListValueBuilder;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;

public class DefaultMetadataHandler implements MetadataHandler {
    public static final TextValue READ_ONLY = Values.utf8Value(new byte[] {'r'});
    public static final TextValue READ_WRITE = Values.utf8Value(new byte[] {'r', 'w'});
    public static final TextValue WRITE = Values.utf8Value(new byte[] {'w'});
    public static final TextValue SCHEMA_WRITE = Values.utf8Value(new byte[] {'s'});
    public static final Map<String, Object> DEFAULT_DIAGNOSTIC_RECORD = Map.of(
            "OPERATION", "",
            "OPERATION_CODE", "0",
            "CURRENT_SCHEMA", "/",
            "_position",
                    Map.of(
                            "offset", -1,
                            "line", -1,
                            "column", -1));

    private static final DefaultMetadataHandler INSTANCE = new DefaultMetadataHandler();

    public static DefaultMetadataHandler getInstance() {
        return INSTANCE;
    }

    @Override
    public void onStatementId(MetadataConsumer consumer, long statementId) {
        // canonically identified "query id" within Bolt hence "qid" in this context
        consumer.onMetadata("qid", Values.longValue(statementId));
    }

    @Override
    public void onTimeSpentPreparingResults(MetadataConsumer handler, long spent) {
        handler.onMetadata("t_first", Values.longValue(spent));
    }

    @Override
    public void onTimeSpentStreaming(MetadataConsumer handler, long spent) {
        handler.onMetadata("t_last", Values.longValue(spent));
    }

    @Override
    public void onFieldNames(MetadataConsumer consumer, List<String> names) {
        var fieldNames = names.stream().map(Values::stringValue).collect(ListValueBuilder.collector());

        consumer.onMetadata("fields", fieldNames);
    }

    @Override
    public void onExecutionType(MetadataConsumer handler, QueryExecutionType executionType) {
        var code =
                switch (executionType.queryType()) {
                    case READ_ONLY -> READ_ONLY;
                    case READ_WRITE -> READ_WRITE;
                    case WRITE -> WRITE;
                    case SCHEMA_WRITE, DBMS -> SCHEMA_WRITE;
                };

        handler.onMetadata("type", code);
    }

    @Override
    public void onDatabase(MetadataConsumer handler, DatabaseReference reference) {
        handler.onMetadata("db", Values.stringValue(reference.alias().name()));
    }

    @Override
    public void onQueryStatistics(MetadataConsumer handler, QueryStatistics statistics) {
        // the statistics key is only present when statistics for at least one known type of update
        // are populated
        if (!statistics.containsSystemUpdates() && !statistics.containsUpdates()) {
            return;
        }

        var metadata = new MapValueBuilder();
        this.generateQueryStatistics(metadata, statistics);

        handler.onMetadata("stats", metadata.build());
    }

    @Override
    public void onExecutionPlan(MetadataConsumer consumer, ExecutionPlanDescription plan) {
        var metadata = new MapValueBuilder();
        this.generateExecutionPlan(metadata, plan);

        var fieldName = plan.hasProfilerStatistics() ? "profile" : "plan";
        consumer.onMetadata(fieldName, metadata.build());
    }

    protected void generateExecutionPlan(MapValueBuilder metadata, ExecutionPlanDescription plan) {
        var hasProfilerStatistics = plan.hasProfilerStatistics();

        metadata.add("operatorType", utf8Value(plan.getName()));
        metadata.add("args", ValueUtils.asMapValue(plan.getArguments()));
        metadata.add("identifiers", ValueUtils.asListValue(plan.getIdentifiers()));

        if (!plan.getChildren().isEmpty()) {
            var children = ListValueBuilder.newListBuilder();
            this.generateExecutionPlanChildren(children, plan);
            metadata.add("children", children.build());
        }

        if (hasProfilerStatistics) {
            var profile = plan.getProfilerStatistics();
            if (profile.hasDbHits()) {
                metadata.add("dbHits", longValue(profile.getDbHits()));
            }

            if (profile.hasPageCacheStats()) {
                metadata.add("pageCacheHits", longValue(profile.getPageCacheHits()));
                metadata.add("pageCacheMisses", longValue(profile.getPageCacheMisses()));
                metadata.add("pageCacheHitRatio", doubleValue(profile.getPageCacheHitRatio()));
            }

            if (profile.hasRows()) {
                metadata.add("rows", longValue(profile.getRows()));
            }

            if (profile.hasTime()) {
                metadata.add("time", longValue(profile.getTime()));
            }
        }
    }

    protected void generateExecutionPlanChildren(ListValueBuilder children, ExecutionPlanDescription plan) {
        for (var child : plan.getChildren()) {
            var metadata = new MapValueBuilder();
            this.generateExecutionPlan(metadata, child);

            children.add(metadata.build());
        }
    }

    @Override
    public void onResultsRemaining(MetadataConsumer handler, boolean hasRemaining) {
        if (hasRemaining) {
            handler.onMetadata("has_more", BooleanValue.TRUE);
        }
    }

    @Override
    public void onRoutingTable(MetadataConsumer consumer, String databaseName, MapValue routingTable) {
        routingTable = enrichRoutingTable(databaseName, routingTable);
        consumer.onMetadata("rt", routingTable);
    }

    protected MapValue enrichRoutingTable(String databaseName, MapValue routingTable) {
        // from 4.4 onwards, the target database is included within the routing table response to
        // facilitate resolving of the home database when connecting to a server without explicitly
        // selecting the target database
        return routingTable.updatedWith("db", Values.stringValue(databaseName));
    }

    @Override
    public void onBookmark(MetadataConsumer handler, String encodedBookmark) {
        handler.onMetadata("bookmark", Values.utf8Value(encodedBookmark));
    }

    @Override
    public void onNotifications(
            MetadataConsumer handler, Iterable<Notification> notifications, Iterable<GqlStatusObject> statuses) {
        var it = statuses.iterator();
        if (!it.hasNext()) {
            return;
        }

        var children = ListValueBuilder.newListBuilder();
        while (it.hasNext()) {
            var status = it.next();
            var diagnosticRecord =
                    mapValue(status.diagnosticRecord(), DefaultMetadataHandler::isDefaultDiagnosticRecordEntry);
            var size = 3;
            if (diagnosticRecord.isEmpty()) {
                size--;
            }

            var statusIsNotification = status instanceof Notification;
            if (statusIsNotification) {
                var notification = (Notification) status;
                if (notification.getCode() != null) {
                    size++;
                }

                if (notification.getTitle() != null) {
                    size++;
                }

                if (notification.getDescription() != null) {
                    size++;
                }
            }

            var builder = new MapValueBuilder(size);

            builder.add("gql_status", Values.utf8Value(status.gqlStatus()));
            builder.add("status_description", Values.utf8Value(status.statusDescription()));

            if (statusIsNotification) {
                var notification = (Notification) status;
                if (notification.getCode() != null) {
                    builder.add("neo4j_code", Values.utf8Value(notification.getCode()));
                }

                if (notification.getTitle() != null) {
                    builder.add("title", Values.utf8Value(notification.getTitle()));
                }

                if (notification.getDescription() != null) {
                    builder.add("description", Values.utf8Value(notification.getDescription()));
                }
            }

            if (!diagnosticRecord.isEmpty()) {
                builder.add("diagnostic_record", diagnosticRecord);
            }

            children.add(builder.build());
        }

        handler.onMetadata("statuses", children.build());
    }

    private static MapValue mapValue(Map<String, Object> map, Predicate<Map.Entry<String, Object>> skip) {
        var builder = new MapValueBuilder();
        for (var entry : map.entrySet()) {
            if (skip.test(entry)) {
                continue;
            }

            builder.add(entry.getKey(), value(entry.getValue()));
        }

        return builder.build();
    }

    private static ListValue listValue(List<?> list) {
        var builder = ListValueBuilder.newListBuilder(list.size());

        for (var item : list) {
            builder.add(value(item));
        }

        return builder.build();
    }

    @SuppressWarnings("unchecked")
    private static AnyValue value(Object value) {
        if (value instanceof Map<?, ?>) {
            return mapValue((Map<String, Object>) value, (ignored) -> false);
        } else if (value instanceof List<?>) {
            return listValue((List<?>) value);
        }

        return Values.of(value);
    }

    private static boolean isDefaultDiagnosticRecordEntry(Map.Entry<String, Object> entry) {
        return DEFAULT_DIAGNOSTIC_RECORD.containsKey(entry.getKey())
                && DEFAULT_DIAGNOSTIC_RECORD.get(entry.getKey()).equals(entry.getValue());
    }

    protected void generateQueryStatistics(MapValueBuilder metadata, QueryStatistics statistics) {
        if (statistics.containsUpdates()) {
            this.generateUpdateQueryStatistics(metadata, statistics);
        } else if (statistics.containsSystemUpdates()) {
            this.generateSystemQueryStatistics(metadata, statistics);
        }
    }

    protected void generateUpdateQueryStatistics(MapValueBuilder metadata, QueryStatistics statistics) {
        addContainsUpdates(metadata);
        addIfNonZero(metadata, "nodes-created", statistics.getNodesCreated());
        addIfNonZero(metadata, "nodes-deleted", statistics.getNodesDeleted());
        addIfNonZero(metadata, "relationships-created", statistics.getRelationshipsCreated());
        addIfNonZero(metadata, "relationships-deleted", statistics.getRelationshipsDeleted());
        addIfNonZero(metadata, "properties-set", statistics.getPropertiesSet());
        addIfNonZero(metadata, "labels-added", statistics.getLabelsAdded());
        addIfNonZero(metadata, "labels-removed", statistics.getLabelsRemoved());
        addIfNonZero(metadata, "indexes-added", statistics.getIndexesAdded());
        addIfNonZero(metadata, "indexes-removed", statistics.getIndexesRemoved());
        addIfNonZero(metadata, "constraints-added", statistics.getConstraintsAdded());
        addIfNonZero(metadata, "constraints-removed", statistics.getConstraintsRemoved());
    }

    protected void addContainsUpdates(MapValueBuilder metadata) {
        metadata.add("contains-updates", BooleanValue.TRUE);
    }

    protected void generateSystemQueryStatistics(MapValueBuilder metadata, QueryStatistics statistics) {
        addContainsSystemUpdates(metadata);
        addIfNonZero(metadata, "system-updates", statistics.getSystemUpdates());
    }

    protected void addContainsSystemUpdates(MapValueBuilder metadata) {
        metadata.add("contains-system-updates", BooleanValue.TRUE);
    }

    protected static void addIfNonZero(MapValueBuilder builder, String key, int value) {
        if (value != 0) {
            builder.add(key, Values.longValue(value));
        }
    }
}
