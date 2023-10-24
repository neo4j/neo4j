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
package org.neo4j.cypher;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.cypher_min_replan_interval;
import static org.neo4j.configuration.GraphDatabaseSettings.query_statistics_divergence_threshold;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.cypher.internal.cache.CypherQueryCaches;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.query.QueryCacheStatistics;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

@ImpermanentDbmsExtension(configurationCallback = "configure")
public class QueryInvalidationIT {
    private static final int USERS = 100;
    private static final int CONNECTIONS = 100;

    @Inject
    private GraphDatabaseAPI db;

    @ExtensionCallback
    void configure(TestDatabaseManagementServiceBuilder builder) {
        builder.setConfig(query_statistics_divergence_threshold, 0.1)
                .setConfig(cypher_min_replan_interval, Duration.ofSeconds(1));
    }

    private ExecutableQueryCacheMetricsMonitor getMonitor() {
        var statistics = db.getDependencyResolver().resolveDependency(QueryCacheStatistics.class);
        var cacheMetrics = statistics.metricsPerCacheKind().get(CypherQueryCaches.ExecutableQueryCache$.MODULE$.kind());
        return (ExecutableQueryCacheMetricsMonitor) cacheMetrics;
    }

    @Test
    void shouldRePlanAfterDataChangesFromAnEmptyDatabase() throws Exception {
        // - setup schema -
        createIndex();
        // - execute the query without the existence data -
        executeDistantFriendsCountQuery(USERS, "default");

        long replanTime = System.currentTimeMillis() + 1_800;

        // - create data -
        createData(0, USERS, CONNECTIONS);

        // - after the query TTL has expired -
        while (System.currentTimeMillis() < replanTime) {
            Thread.sleep(100);
        }

        // WHEN
        var monitor = getMonitor();
        var replansBefore = monitor.numberOfReplans();
        var waitTimeBefore = monitor.replanWaitTime();

        // - execute the query again -
        executeDistantFriendsCountQuery(USERS, "default");

        // THEN
        assertThat(monitor.numberOfReplans() - replansBefore)
                .as("Query should have been replanned.")
                .isEqualTo(1);
        assertThat(monitor.replanWaitTime() - waitTimeBefore)
                .as("Replan should have occurred after TTL")
                .isGreaterThanOrEqualTo(1L);
    }

    @Test
    void shouldScheduleRePlansWithForceAndSkip() throws Exception {
        // GIVEN
        // - setup schema -
        createIndex();
        // - execute the query without the existence data -
        executeDistantFriendsCountQuery(USERS, "force");

        long replanTime = System.currentTimeMillis() + 1_800;

        // - create data -
        createData(0, USERS, CONNECTIONS);

        // - after the query TTL has expired -
        while (System.currentTimeMillis() < replanTime) {
            Thread.sleep(100);
        }

        // WHEN
        var monitor = getMonitor();

        var compiledBefore = monitor.getCompiled();
        var replansBefore = monitor.numberOfReplans();

        // - execute the query again, with replan=skip -
        executeDistantFriendsCountQuery(USERS, "skip");

        // THEN
        assertThat(monitor.getCompiled() - compiledBefore)
                .as("Query should not have been compiled.")
                .isEqualTo(0);
        assertThat(monitor.numberOfReplans() - replansBefore)
                .as("Query should not have been discarded.")
                .isEqualTo(0);

        // - execute the query again, with replan=force -
        executeDistantFriendsCountQuery(USERS, "force");

        // THEN
        assertThat(monitor.getCompiled() - compiledBefore)
                .as("Query should have been replanned.")
                .isEqualTo(1);

        compiledBefore = monitor.getCompiled();
        // - execute the query again, with replan=default -
        executeDistantFriendsCountQuery(USERS, "default");

        // THEN should use the entry cached with "replan=force" instead of replanning again
        assertThat(monitor.getCompiled() - compiledBefore)
                .as("Query should not have been compiled.")
                .isEqualTo(0);
        assertThat(monitor.numberOfReplans() - replansBefore)
                .as("Query should not have been discarded.")
                .isEqualTo(0);
    }

    @Test
    void shouldRePlanAfterDataChangesFromAPopulatedDatabase() throws Exception {
        // GIVEN
        Config config = db.getDependencyResolver().resolveDependency(Config.class);
        double divergenceThreshold = config.get(query_statistics_divergence_threshold);
        long replanInterval = config.get(cypher_min_replan_interval).toMillis();

        // - setup schema -
        createIndex();
        // create some data
        createData(0, USERS, CONNECTIONS);
        executeDistantFriendsCountQuery(USERS, "default");

        long replanTime = System.currentTimeMillis() + replanInterval;

        assertThat(divergenceThreshold)
                .as(
                        "Test does not work with edge setting for %s: %f",
                        query_statistics_divergence_threshold, divergenceThreshold)
                .isGreaterThan(0)
                .isLessThan(1);

        int usersToCreate = ((int) (Math.ceil(((double) USERS) / (1.0 - divergenceThreshold)))) - USERS + 1;

        // create more data
        createData(USERS, usersToCreate, CONNECTIONS);

        // - after the query TTL has expired -
        while (System.currentTimeMillis() <= replanTime) {
            Thread.sleep(100);
        }

        // WHEN
        var monitor = getMonitor();
        var replansBefore = monitor.numberOfReplans();
        var waitTimeBefore = monitor.replanWaitTime();

        // - execute the query again -
        executeDistantFriendsCountQuery(USERS, "default");

        // THEN
        assertThat(monitor.numberOfReplans() - replansBefore)
                .as("Query should have been replanned.")
                .isEqualTo(1);
        assertThat(monitor.replanWaitTime() - waitTimeBefore)
                .as("Replan should have occurred after TTL")
                .isGreaterThanOrEqualTo(replanInterval / 1000);
    }

    private void createIndex() {
        try (Transaction tx = db.beginTx()) {
            tx.schema().indexFor(Label.label("User")).on("userId").create();
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(1, MINUTES);
            tx.commit();
        }
    }

    private void createData(long startingUserId, int numUsers, int numConnections) {
        try (Transaction transaction = db.beginTx()) {
            for (long userId = startingUserId; userId < numUsers + startingUserId; userId++) {
                transaction.execute("CREATE (newUser:User {userId: $userId})", Map.of("userId", userId));
            }
            Map<String, Object> params = new HashMap<>();
            for (int i = 0; i < numConnections; i++) {
                long user1 = startingUserId + randomInt(numUsers);
                long user2;
                do {
                    user2 = startingUserId + randomInt(numUsers);
                } while (user1 == user2);
                params.put("user1", user1);
                params.put("user2", user2);
                transaction.execute(
                        "MATCH (user1:User { userId: $user1 }), (user2:User { userId: $user2 }) "
                                + "MERGE (user1) -[:FRIEND]- (user2)",
                        params);
            }
            transaction.commit();
        }
    }

    private void executeDistantFriendsCountQuery(int userId, String replanStrategy) {
        try (Transaction transaction = db.beginTx()) {
            Map<String, Object> params = Map.of("userId", (long) randomInt(userId));

            try (Result result = transaction.execute(
                    String.format(
                            "CYPHER replan=%s MATCH (user:User { userId: $userId } ) -[:FRIEND]- () -[:FRIEND]- (distantFriend) "
                                    + "RETURN COUNT(distinct distantFriend)",
                            replanStrategy),
                    params)) {
                while (result.hasNext()) {
                    result.next();
                }
            }
            transaction.commit();
        }
    }

    private static int randomInt(int max) {
        return ThreadLocalRandom.current().nextInt(max);
    }
}
