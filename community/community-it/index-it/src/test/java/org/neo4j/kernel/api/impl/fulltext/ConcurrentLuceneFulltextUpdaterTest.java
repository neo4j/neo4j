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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphdb.schema.IndexType.FULLTEXT;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.exceptions.KernelException;
import org.neo4j.function.ThrowingAction;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.test.Race;

/**
 * Concurrent updates and index changes should result in valid state, and not create conflicts or exceptions during commit.
 */
class ConcurrentLuceneFulltextUpdaterTest extends LuceneFulltextTestSupport {
    private static final String NODES_INDEX_NAME = "nodes";
    private static final String RELS_INDEX_NAME = "rels";
    private static final int ENTITIES_PER_THREAD = 500;
    private static final int ALICE_THREADS = 1;
    private static final int BOB_THREADS = 1;
    private Race race;
    private final CountDownLatch aliceLatch = new CountDownLatch(2);
    private final CountDownLatch bobLatch = new CountDownLatch(2);

    @BeforeEach
    public void createRace() {
        race = new Race();
    }

    private void createInitialNodeIndex() {
        try (Transaction tx = db.beginTx()) {
            tx.schema()
                    .indexFor(LABEL)
                    .on(PROP)
                    .withIndexType(FULLTEXT)
                    .withName(NODES_INDEX_NAME)
                    .create();
            tx.commit();
        }
    }

    private void createInitialRelationshipIndex() {
        try (Transaction tx = db.beginTx()) {
            tx.schema()
                    .indexFor(RELTYPE)
                    .on(PROP)
                    .withIndexType(FULLTEXT)
                    .withName(RELS_INDEX_NAME)
                    .create();
            tx.commit();
        }
    }

    private void raceContestants(Runnable aliceWork, Runnable changeConfig, Runnable bobWork) throws Throwable {
        race.addContestants(ALICE_THREADS, aliceWork);
        race.addContestant(changeConfig);
        race.addContestants(BOB_THREADS, bobWork);
        race.go();
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(1, TimeUnit.MINUTES);
        }
    }

    private void verifyForNodes() throws KernelException {
        try (Transaction tx = db.beginTx()) {
            KernelTransaction ktx = kernelTransaction(tx);
            IndexReadSession index =
                    ktx.dataRead().indexReadSession(ktx.schemaRead().indexGetForName(NODES_INDEX_NAME));
            try (NodeValueIndexCursor bobCursor =
                    ktx.cursors().allocateNodeValueIndexCursor(ktx.cursorContext(), ktx.memoryTracker())) {
                ktx.dataRead()
                        .nodeIndexSeek(
                                ktx.queryContext(),
                                index,
                                bobCursor,
                                unconstrained(),
                                PropertyIndexQuery.fulltextSearch("bob"));
                int bobCount = 0;
                while (bobCursor.next()) {
                    bobCount += 1;
                }
                assertEquals(BOB_THREADS * ENTITIES_PER_THREAD, bobCount);
            }
            try (NodeValueIndexCursor aliceCursor =
                    ktx.cursors().allocateNodeValueIndexCursor(ktx.cursorContext(), ktx.memoryTracker())) {
                ktx.dataRead()
                        .nodeIndexSeek(
                                ktx.queryContext(),
                                index,
                                aliceCursor,
                                unconstrained(),
                                PropertyIndexQuery.fulltextSearch("alice"));
                int aliceCount = 0;
                while (aliceCursor.next()) {
                    aliceCount += 1;
                }
                assertEquals(0, aliceCount);
            }
        }
    }

    private void verifyForRels() throws KernelException {
        try (Transaction tx = db.beginTx()) {
            KernelTransaction ktx = kernelTransaction(tx);
            IndexReadSession index =
                    ktx.dataRead().indexReadSession(ktx.schemaRead().indexGetForName(RELS_INDEX_NAME));
            try (var bobCursor =
                    ktx.cursors().allocateRelationshipValueIndexCursor(ktx.cursorContext(), ktx.memoryTracker())) {
                ktx.dataRead()
                        .relationshipIndexSeek(
                                ktx.queryContext(),
                                index,
                                bobCursor,
                                unconstrained(),
                                PropertyIndexQuery.fulltextSearch("bob"));
                int bobCount = 0;
                while (bobCursor.next()) {
                    bobCount += 1;
                }
                assertEquals(BOB_THREADS * ENTITIES_PER_THREAD, bobCount);
            }
            try (var aliceCursor =
                    ktx.cursors().allocateRelationshipValueIndexCursor(ktx.cursorContext(), ktx.memoryTracker())) {
                ktx.dataRead()
                        .relationshipIndexSeek(
                                ktx.queryContext(),
                                index,
                                aliceCursor,
                                unconstrained(),
                                PropertyIndexQuery.fulltextSearch("alice"));
                int aliceCount = 0;
                while (aliceCursor.next()) {
                    aliceCount += 1;
                }
                assertEquals(0, aliceCount);
            }
        }
    }

    private Runnable work(int iterations, ThrowingConsumer<Transaction, Exception> work) {
        return () -> {
            try {
                for (int i = 0; i < iterations; i++) {
                    Thread.yield();
                    try (Transaction tx = db.beginTx()) {
                        Thread.yield();
                        work.accept(tx);
                        Thread.yield();
                        tx.commit();
                    }
                }
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        };
    }

    private ThrowingAction<Exception> dropAndReCreateNodeIndex() {
        return () -> {
            aliceLatch.await();
            bobLatch.await();
            try (Transaction tx = db.beginTx()) {
                tx.schema().getIndexByName(NODES_INDEX_NAME).drop();
                tx.schema()
                        .indexFor(LABEL)
                        .on("otherProp")
                        .withIndexType(FULLTEXT)
                        .withName(NODES_INDEX_NAME)
                        .create();
                tx.commit();
            }
        };
    }

    private ThrowingAction<Exception> dropAndReCreateRelationshipIndex() {
        return () -> {
            aliceLatch.await();
            bobLatch.await();
            try (Transaction tx = db.beginTx()) {
                tx.schema().getIndexByName(RELS_INDEX_NAME).drop();
                tx.schema()
                        .indexFor(RELTYPE)
                        .on("otherProp")
                        .withIndexType(FULLTEXT)
                        .withName(RELS_INDEX_NAME)
                        .create();
                tx.commit();
            }
        };
    }

    @Test
    void labelledNodesCoreAPI() throws Throwable {
        createInitialNodeIndex();

        Runnable aliceWork = work(ENTITIES_PER_THREAD, tx -> {
            tx.getNodeById(createNodeIndexableByPropertyValue(tx, LABEL, "alice"));
            aliceLatch.countDown();
        });
        Runnable bobWork = work(ENTITIES_PER_THREAD, tx -> {
            tx.getNodeById(createNodeWithProperty(tx, LABEL, "otherProp", "bob"));
            bobLatch.countDown();
        });
        Runnable changeConfig = work(1, tx -> dropAndReCreateNodeIndex().apply());
        raceContestants(aliceWork, changeConfig, bobWork);
        verifyForNodes();
    }

    @Test
    void labelledNodesCypherCurrent() throws Throwable {
        createInitialNodeIndex();

        Runnable aliceWork = work(ENTITIES_PER_THREAD, tx -> {
            tx.execute("create (:LABEL {" + PROP + ": \"alice\"})").close();
            aliceLatch.countDown();
        });
        Runnable bobWork = work(ENTITIES_PER_THREAD, tx -> {
            tx.execute("create (:LABEL {otherProp: \"bob\"})").close();
            bobLatch.countDown();
        });
        Runnable changeConfig = work(1, tx -> dropAndReCreateNodeIndex().apply());
        raceContestants(aliceWork, changeConfig, bobWork);
        verifyForNodes();
    }

    @Test
    void relationshipsCoreAPI() throws Throwable {
        createInitialRelationshipIndex();

        Runnable aliceWork = work(ENTITIES_PER_THREAD, tx -> {
            tx.getRelationshipById(createRelationshipIndexableByPropertyValue(
                    tx, tx.createNode().getId(), tx.createNode().getId(), "alice"));
            aliceLatch.countDown();
        });
        Runnable bobWork = work(ENTITIES_PER_THREAD, tx -> {
            tx.getRelationshipById(createRelationshipWithProperty(
                    tx, tx.createNode().getId(), tx.createNode().getId(), "otherProp", "bob"));
            bobLatch.countDown();
        });
        Runnable changeConfig = work(1, tx -> dropAndReCreateRelationshipIndex().apply());
        raceContestants(aliceWork, changeConfig, bobWork);
        verifyForRels();
    }

    @Test
    void relationshipsCypherCurrent() throws Throwable {
        createInitialRelationshipIndex();

        Runnable aliceWork = work(ENTITIES_PER_THREAD, tx -> {
            tx.execute("create ()-[:type {" + PROP + ": \"alice\"}]->()").close();
            aliceLatch.countDown();
        });
        Runnable bobWork = work(ENTITIES_PER_THREAD, tx -> {
            tx.execute("create ()-[:type {otherProp: \"bob\"}]->()").close();
            bobLatch.countDown();
        });
        Runnable changeConfig = work(1, tx -> dropAndReCreateRelationshipIndex().apply());
        raceContestants(aliceWork, changeConfig, bobWork);
        verifyForRels();
    }
}
