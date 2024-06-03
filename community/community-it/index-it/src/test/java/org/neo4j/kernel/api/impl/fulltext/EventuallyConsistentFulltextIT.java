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
package org.neo4j.kernel.api.impl.fulltext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.api.KernelTransaction.Type.EXPLICIT;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexProceduresUtil.AWAIT_REFRESH;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.configuration.FulltextSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.util.concurrent.Futures;

@PageCacheExtension
class EventuallyConsistentFulltextIT {
    private static final String INDEX_NAME_1 = "my_ft_index_1";
    private static final String INDEX_NAME_2 = "my_ft_index_2";
    private static final Label LABEL1 = Label.label("Label1");
    private static final Label LABEL2 = Label.label("Label2");
    private static final String KEY = "key";

    @Inject
    private PageCache pageCache;

    @Inject
    private TestDirectory directory;

    private DatabaseManagementService dbms;
    private GraphDatabaseAPI db;

    private void startDbms(TestDatabaseManagementServiceBuilder builder) {
        dbms = builder.build();
        db = (GraphDatabaseAPI) dbms.database(DEFAULT_DATABASE_NAME);
    }

    @AfterEach
    void stop() {
        dbms.shutdown();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldUpdateEventuallyConsistentFulltextIndexesInParallel(boolean asyncRefresh) throws Exception {
        // given
        TestDatabaseManagementServiceBuilder builder = new TestDatabaseManagementServiceBuilder(directory.homePath())
                .setConfig(FulltextSettings.eventually_consistent, true)
                .setConfig(FulltextSettings.eventually_consistent_apply_parallelism, 4);
        if (asyncRefresh) {
            builder = builder.setConfig(FulltextSettings.eventually_consistent_refresh_interval, Duration.ofSeconds(1))
                    .setConfig(FulltextSettings.eventually_consistent_refresh_parallelism, 2);
        }
        startDbms(builder);
        createFulltextIndexes();

        // when
        int numNodes = createFulltextIndexedNodesInParallel(4, 100, 10);

        // then
        awaitIndexUpdatesUpToThisPoint();
        assertAllNodesVisibleInIndexes(numNodes);
    }

    private void awaitIndexUpdatesUpToThisPoint() {
        try (var tx = db.beginTx()) {
            tx.execute(AWAIT_REFRESH);
        }
    }

    private void assertAllNodesVisibleInIndexes(int numNodes) throws KernelException {
        try (var tx = db.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var cursor = tx.kernelTransaction().cursors().allocateNodeValueIndexCursor(NULL_CONTEXT, INSTANCE)) {
            var ktx = tx.kernelTransaction();
            for (var indexName : new String[] {INDEX_NAME_1, INDEX_NAME_2}) {
                var index = ktx.schemaRead().indexGetForName(indexName);
                var session = ktx.dataRead().indexReadSession(index);
                ktx.dataRead()
                        .nodeIndexSeek(
                                ktx.queryContext(),
                                session,
                                cursor,
                                unconstrained(),
                                PropertyIndexQuery.fulltextSearch("Marker"));
                int numIndexedNodes = 0;
                while (cursor.next()) {
                    numIndexedNodes++;
                }
                assertThat(numIndexedNodes).isEqualTo(numNodes);
            }
        }
    }

    private int createFulltextIndexedNodesInParallel(int numThreads, int numTransactionsPerThread, int txSize)
            throws Exception {
        List<Callable<Void>> tasks = new ArrayList<>();
        for (int t = 0; t < numThreads; t++) {
            tasks.add(() -> {
                for (int r = 0; r < numTransactionsPerThread; r++) {
                    try (var tx = db.beginTx()) {
                        for (int i = 0; i < txSize; i++) {
                            createFulltextIndexedNode(tx);
                        }
                        tx.commit();
                    }
                }
                return null;
            });
        }

        var executor = Executors.newFixedThreadPool(numThreads);
        try {
            Futures.getAll(executor.invokeAll(tasks));
        } finally {
            executor.shutdown();
        }
        return numThreads * numTransactionsPerThread * txSize;
    }

    private void createFulltextIndexedNode(Transaction tx) {
        tx.createNode(LABEL1, LABEL2).setProperty(KEY, "Marker " + UUID.randomUUID());
    }

    private void createFulltextIndexes() {
        try (var tx = db.beginTx()) {
            tx.schema()
                    .indexFor(LABEL1)
                    .on(KEY)
                    .withIndexType(IndexType.FULLTEXT)
                    .withName(INDEX_NAME_1)
                    .create();
            tx.schema()
                    .indexFor(LABEL2)
                    .on(KEY)
                    .withIndexType(IndexType.FULLTEXT)
                    .withName(INDEX_NAME_2)
                    .create();
            tx.commit();
        }
        try (var tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(1, TimeUnit.MINUTES);
        }
    }
}
