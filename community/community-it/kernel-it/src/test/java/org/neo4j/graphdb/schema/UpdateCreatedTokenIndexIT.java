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
package org.neo4j.graphdb.schema;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.neo4j.test.Race.throwing;
import static org.neo4j.test.TestLabels.LABEL_ONE;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.IndexMonitor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.monitoring.Monitors;
import org.neo4j.test.Race;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomSupportExtension;

@RandomSupportExtension
@ImpermanentDbmsExtension(configurationCallback = "configure")
class UpdateCreatedTokenIndexIT {
    private static final int NODES = 100;
    private static final int SKIP_NODES = 100;

    @Inject
    private GraphDatabaseAPI db;

    @Inject
    DatabaseManagementService managementService;

    @Inject
    RandomSupport random;

    private final Monitors monitors = new Monitors();

    @ExtensionCallback
    void configure(TestDatabaseManagementServiceBuilder builder) {
        builder.setMonitors(monitors);
    }

    @BeforeEach
    void before() {
        try (var tx = db.beginTx()) {
            tx.schema().getIndexes().forEach(IndexDefinition::drop);
            tx.commit();
        }
    }

    @RepeatedTest(5)
    void shouldHandleCreateNodeConcurrentlyWithIndexCreate() throws Throwable {
        shouldHandleIndexCreateConcurentlyWithOperation((tx, nodeId) -> tx.createNode(LABEL_ONE));
    }

    @RepeatedTest(5)
    void shouldHandleRemovalOfLabelConcurrentlyWithIndexCreate() throws Throwable {
        shouldHandleIndexCreateConcurentlyWithOperation(
                (tx, nodeId) -> tx.getNodeById(nodeId).removeLabel(LABEL_ONE));
    }

    @RepeatedTest(5)
    void shouldHandleDeleteNodeConcurrentlyWithIndexCreate() throws Throwable {
        shouldHandleIndexCreateConcurentlyWithOperation(
                (tx, nodeId) -> tx.getNodeById(nodeId).delete());
    }

    @RepeatedTest(5)
    void shouldHandleNodeDetachDeleteConcurrentlyWithIndexCreate() throws Throwable {
        shouldHandleIndexCreateConcurentlyWithOperation((tx, nodeId) -> {
            ((InternalTransaction) tx).kernelTransaction().dataWrite().nodeDetachDelete(nodeId);
        });
    }

    @Test
    void shouldNotPlanTokenIndexInPopulatingState() {
        Label label = Label.label("Label");
        String indexName = "testIndex";
        CountDownLatch populationScanLatch = new CountDownLatch(1);

        monitors.addMonitorListener(new IndexMonitor.MonitorAdapter() {
            @Override
            public void indexPopulationScanStarting(IndexDescriptor[] indexDescriptors) {
                if (Arrays.stream(indexDescriptors)
                        .map(IndexDescriptor::getName)
                        .anyMatch(indexName::equals)) {
                    try {
                        populationScanLatch.await();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        });

        try (var tx = db.beginTx()) {
            for (int i = 0; i < 100; i++) {
                tx.createNode(label);
            }
            tx.commit();
        }

        try {
            try (var tx = db.beginTx()) {
                tx.schema().indexFor(AnyTokens.ANY_LABELS).withName(indexName).create();
                tx.commit();
            }

            try (var tx = db.beginTx()) {
                assertThatNoException().isThrownBy(() -> {
                    try (var result = tx.execute("MATCH (n:Label) RETURN n")) {
                        while (result.hasNext()) {
                            result.next();
                        }
                    }
                });
            }
        } finally {
            populationScanLatch.countDown();
        }
    }

    private void shouldHandleIndexCreateConcurentlyWithOperation(NodeOperation operation) throws Throwable {
        // given
        long[] nodes = createNodes();

        // when
        Race race = new Race();
        race.addContestant(
                () -> {
                    try (Transaction tx = db.beginTx()) {
                        tx.schema()
                                .indexFor(AnyTokens.ANY_LABELS)
                                .withName("myLabelIndex")
                                .create();
                        tx.commit();
                    }
                },
                1);
        for (int i = 0; i < NODES; i++) {
            final long nodeId = nodes[i];
            race.addContestant(throwing(() -> {
                try (Transaction tx = db.beginTx()) {
                    operation.run(tx, nodeId);
                    tx.commit();
                }
            }));
        }

        // then
        race.go();

        try (var tx = db.beginTx()) {
            tx.schema().awaitIndexOnline("myLabelIndex", 5, TimeUnit.MINUTES);
        }

        try (var tx = db.beginTx()) {
            var labeledNodes = Iterators.count(tx.findNodes(LABEL_ONE));
            try (Stream<Node> allNodes = tx.getAllNodes().stream()) {
                Assertions.assertThat(
                                allNodes.filter(n -> n.hasLabel(LABEL_ONE)).count())
                        .isEqualTo(labeledNodes);
            }
        }
    }

    private long[] createNodes() {
        long[] nodesToDelete = new long[SKIP_NODES];
        try (Transaction tx = db.beginTx()) {
            for (int i = 0; i < SKIP_NODES; i++) {
                nodesToDelete[i] = tx.createNode(LABEL_ONE).getId();
            }
            tx.commit();
        }

        long[] nodes = new long[NODES];
        try (Transaction tx = db.beginTx()) {
            for (int i = 0; i < NODES; i++) {
                Node node = tx.createNode(LABEL_ONE);
                nodes[i] = node.getId();
            }
            tx.commit();
        }

        try (var tx = db.beginTx()) {
            for (int i = 0; i < SKIP_NODES; i++) {
                tx.getNodeById(nodesToDelete[i]).delete();
            }
            tx.commit();
        }
        return nodes;
    }

    private interface NodeOperation {
        void run(Transaction tx, long nodeId) throws Exception;
    }
}
