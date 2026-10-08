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
package org.neo4j.internal.collector;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.graph_stats_collection_divergence_threshold;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.graph_stats_collection_enabled;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.graph_stats_collection_interval;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.SettingChangeListener;
import org.neo4j.cypher.internal.planner.spi.GraphStatisticsSnapshot;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.InternalLog;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;

public class GraphStatsCollector extends LifecycleAdapter {

    private static final Set<String> VOLATILE_INDEX_FIELDS =
            Set.of("totalSize", "updatesSinceEstimation", "estimatedUniqueSize");

    private final Config config;
    private final Database database;
    private final JobScheduler scheduler;
    private final BiConsumer<String, Map<String, Object>> graphStatsSink;
    private final InternalLog log;
    private final SettingChangeListener<Boolean> enabledListener = (before, after) -> updateSchedule();
    private final SettingChangeListener<Duration> durationListener = (before, after) -> updateSchedule();

    private JobHandle<?> jobHandle;

    // Only ever read/written from within collect(), which runs on a single recurring scheduler job.
    private GraphStatisticsSnapshot retainedSnapshot;
    private Set<Map<String, Object>> retainedIndexIdentities = Set.of();
    private Set<Map<String, Object>> retainedConstraintIdentities = Set.of();

    public GraphStatsCollector(
            Config config,
            Database database,
            JobScheduler scheduler,
            BiConsumer<String, Map<String, Object>> graphStatsSink,
            InternalLog log) {
        this.config = config;
        this.database = database;
        this.scheduler = scheduler;
        this.graphStatsSink = graphStatsSink;
        this.log = log;
    }

    @Override
    public synchronized void start() {
        updateSchedule();
        config.addListener(graph_stats_collection_enabled, enabledListener);
        config.addListener(graph_stats_collection_interval, durationListener);
    }

    @Override
    public synchronized void stop() {
        config.removeListener(graph_stats_collection_enabled, enabledListener);
        config.removeListener(graph_stats_collection_interval, durationListener);
        cancelScheduledJob();
    }

    private synchronized void updateSchedule() {
        cancelScheduledJob();
        if (config.get(graph_stats_collection_enabled)) {
            long intervalMillis = config.get(graph_stats_collection_interval).toMillis();
            jobHandle =
                    scheduler.scheduleRecurring(Group.DATA_COLLECTOR, this::collect, 0, intervalMillis, MILLISECONDS);
        }
    }

    private synchronized void cancelScheduledJob() {
        if (jobHandle != null) {
            jobHandle.cancel();
            jobHandle = null;
        }
    }

    private void collect() {
        var dbName = database.getDatabaseAPI().databaseName();
        try {
            Map<String, Object> graphCounts = GraphCountsSection.retrieve(database.getKernel(), Anonymizer.PLAIN_TEXT)
                    .map(retrievedResult -> retrievedResult.data)
                    .findFirst()
                    .orElse(Map.of());
            if (shouldLog(graphCounts)) {
                graphStatsSink.accept(dbName, graphCounts);
            }
        } catch (Exception e) {
            log.warn("Failed to collect graph stats for database '%s'".formatted(dbName), e);
        }
    }

    private boolean shouldLog(Map<String, Object> graphCounts) {
        GraphStatisticsSnapshot current = countsSnapshot(graphCounts);
        Set<Map<String, Object>> currentIndexIdentities = indexIdentities(graphCounts);
        Set<Map<String, Object>> currentConstraintIdentities = constraintIdentities(graphCounts);

        boolean stale = retainedSnapshot == null
                || !retainedSnapshot
                        .statsValues()
                        .keySet()
                        .equals(current.statsValues().keySet())
                || !retainedIndexIdentities.equals(currentIndexIdentities)
                || !retainedConstraintIdentities.equals(currentConstraintIdentities)
                || retainedSnapshot
                        .diverges(current)
                        .exists(d -> d.divergence() > config.get(graph_stats_collection_divergence_threshold));

        if (stale) {
            retainedSnapshot = current;
            retainedIndexIdentities = currentIndexIdentities;
            retainedConstraintIdentities = currentConstraintIdentities;
        }
        return stale;
    }

    private static GraphStatisticsSnapshot countsSnapshot(Map<String, Object> graphCounts) {
        return GraphStatisticsSnapshotBuilder.from(
                entries(graphCounts, "nodes"), entries(graphCounts, "relationships"));
    }

    private static Set<Map<String, Object>> indexIdentities(Map<String, Object> graphCounts) {
        return entries(graphCounts, "indexes").stream()
                .map(GraphStatsCollector::indexIdentity)
                .collect(Collectors.toUnmodifiableSet());
    }

    private static Map<String, Object> indexIdentity(Map<String, Object> index) {
        Map<String, Object> identity = new HashMap<>(index);
        identity.keySet().removeAll(VOLATILE_INDEX_FIELDS);
        return identity;
    }

    private static Set<Map<String, Object>> constraintIdentities(Map<String, Object> graphCounts) {
        return Set.copyOf(entries(graphCounts, "constraints"));
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> entries(Map<String, Object> graphCounts, String key) {
        Object value = graphCounts.get(key);
        return value == null ? List.of() : (List<Map<String, Object>>) value;
    }
}
