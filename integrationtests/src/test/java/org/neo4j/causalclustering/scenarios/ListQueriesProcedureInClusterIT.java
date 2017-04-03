/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.scenarios;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.neo4j.causalclustering.core.CoreGraphDatabase;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.readreplica.ReadReplicaGraphDatabase;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.test.causalclustering.ClusterRule;
import org.neo4j.test.rule.VerboseTimeout;
import org.neo4j.test.rule.concurrent.ThreadingRule;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class ListQueriesProcedureInClusterIT {
    private final ClusterRule clusterRule =
            new ClusterRule(getClass()).withNumberOfCoreMembers(3).withNumberOfReadReplicas(1);
    private final VerboseTimeout timeout = VerboseTimeout.builder()
            .withTimeout(1000, TimeUnit.SECONDS)
            .build();

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule(clusterRule).around(timeout);

    private Cluster cluster;

    @Rule
    public final ThreadingRule threads = new ThreadingRule();

    @Before
    public void setup() throws Exception {
        cluster = clusterRule.startCluster();
    }

    @Test
    public void shouldListQueriesOnCoreAndReplica() throws Exception {
        // given
        String CORE_QUERY = "MATCH (n) SET n.number = n.number - 1";
        String REPLICA_QUERY = "MATCH (n) RETURN n";
        BiConsumer<Transaction, Node> lock = (transaction, entity) -> transaction.acquireWriteLock(entity);
        CountDownLatch resourceLocked = new CountDownLatch(1);
        CountDownLatch listQueriesLatch = new CountDownLatch(1);

        cluster.coreTx((CoreGraphDatabase leaderDb, Transaction tx) ->
        {
            try {

                //Given
                ReadReplicaGraphDatabase replicaDb = cluster.findAnyReadReplica().database();
                Node node = leaderDb.createNode();
                tx.success();

                threads.execute(parameter ->
                {
                    cluster.coreTx((leaderDb1, secondTransaction) -> {

                        //lock node
                        lock.accept(secondTransaction, node);
                        resourceLocked.countDown();
                        try {
                            listQueriesLatch.await();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                            fail("failure in locking node");
                        }
                    });
                    return null;
                }, null);

                resourceLocked.await();

                //When
                //Execute query on Leader
                threads.execute(parameter ->
                {
                    leaderDb.execute(CORE_QUERY).close();
                    return null;
                }, null);

                //Execute query on Replica
                threads.execute(parameter ->
                {
                    replicaDb.execute(REPLICA_QUERY).close();
                    return null;
                }, null);

                //Then
                try  {
                    Map<String, Object> coreQueryListing = getQueryListing(CORE_QUERY, leaderDb).get();
                    Map<String, Object> replicaQueryListing = getQueryListing(REPLICA_QUERY, replicaDb).get();

                    assertNotNull(coreQueryListing);
                    assertNotNull(replicaQueryListing);

                    assertThat(coreQueryListing.get("activeLockCount"), is(1l));
                    assertThat(replicaQueryListing.get("activeLockCount"), is(1l));

                    assertFalse(getQueryListing(REPLICA_QUERY, leaderDb).isPresent());
                    assertFalse(getQueryListing(CORE_QUERY, replicaDb).isPresent());

                    listQueriesLatch.countDown();

                } catch (Exception e) {
                    e.printStackTrace();
                    fail("Couldn't countdown listqueries latch");
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
                fail("Should create node and list queries");
            }
        });
    }

    private Optional<Map<String, Object>> getQueryListing(String query, GraphDatabaseFacade db) {
        try (Result rows = db.execute("CALL dbms.listQueries()")) {
            while (rows.hasNext()) {
                Map<String, Object> row = rows.next();
                if (query.equals(row.get("query"))) {
                    return Optional.of(row);
                }
            }
        }
        return Optional.empty();
    }


    private static class Resource<T> implements AutoCloseable {
        private final CountDownLatch latch;
        private final T resource;

        private Resource(CountDownLatch latch, T resource) {
            this.latch = latch;
            this.resource = resource;
        }

        @Override
        public void close() {
            latch.countDown();
        }
    }
}
