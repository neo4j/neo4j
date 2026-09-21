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
package org.neo4j.kernel.api.impl.schema.fulltext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.kernel.api.impl.schema.fulltext.LuceneFulltextTestSupport.assertQueryFindsIds;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.eclipse.collections.api.factory.primitive.LongSets;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.internal.kernel.api.EntityFilterBuilder;
import org.neo4j.internal.kernel.api.EntityFilterIndexReader;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.PropertyIndexQuery.EntityFilterPredicate;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomSupportExtension;
import org.neo4j.test.extension.SkipOnSpd;

@SkipOnSpd
@RandomSupportExtension
class FulltextEntityFilterTest extends LuceneFulltextTestSupport {
    private static final String NODE_INDEX_NAME = "nodeFulltextEntityFilterIndex";
    private static final String REL_INDEX_NAME = "relFulltextEntityFilterIndex";
    private static final int NUM_ENTITIES = 1000;
    private static final int ITERATIONS = 100;

    @Inject
    private RandomSupport random;

    @Test
    void filterNodes() throws Exception {
        createNodeFulltextIndex(NODE_INDEX_NAME);
        long alice;
        long bob;
        long carol;
        try (Transaction tx = db.beginTx()) {
            alice = createNodeWithProperty(tx, LABEL, "name", "Alice");
            bob = createNodeWithProperty(tx, LABEL, "name", "Bob");
            carol = createNodeWithProperty(tx, LABEL, "name", "Carol");
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            KernelTransaction ktx = kernelTransaction(tx);
            assertQueryFindsIds(ktx, true, NODE_INDEX_NAME, "name", entityFilter());
            assertQueryFindsIds(ktx, true, NODE_INDEX_NAME, "name", entityFilter(alice), alice);
            assertQueryFindsIds(ktx, true, NODE_INDEX_NAME, "name", entityFilter(bob), bob);
            assertQueryFindsIds(ktx, true, NODE_INDEX_NAME, "name", entityFilter(carol), carol);
            assertQueryFindsIds(ktx, true, NODE_INDEX_NAME, "name", entityFilter(alice, bob), alice, bob);
            assertQueryFindsIds(ktx, true, NODE_INDEX_NAME, "name", entityFilter(alice, carol), alice, carol);
            assertQueryFindsIds(ktx, true, NODE_INDEX_NAME, "name", entityFilter(bob, carol), bob, carol);
            assertQueryFindsIds(ktx, true, NODE_INDEX_NAME, "name", entityFilter(alice, bob, carol), alice, bob, carol);
        }
    }

    @Test
    void filterNodesFuzzTest() throws Exception {
        createNodeFulltextIndex(NODE_INDEX_NAME);
        List<Long> allNodes = new ArrayList<>();
        try (Transaction tx = db.beginTx()) {
            for (int i = 0; i < NUM_ENTITIES; i++) {
                allNodes.add(createNodeWithProperty(tx, LABEL, PROP, "fuzzword"));
            }
            tx.commit();
        }

        KernelTransactionImplementation ktx = getKernelTransaction();
        try {
            // sanity check: the query matches all nodes
            assertQueryFindsIds(
                    ktx,
                    true,
                    NODE_INDEX_NAME,
                    "fuzzword",
                    allNodes.stream().mapToLong(Long::longValue).toArray());

            for (int i = 0; i < ITERATIONS; i++) {
                Set<Long> subset = randomSubset(allNodes);
                assertQueryFindsIds(
                        ktx,
                        true,
                        NODE_INDEX_NAME,
                        "fuzzword",
                        entityFilter(from(subset)),
                        subset.stream().mapToLong(Long::longValue).toArray());
            }
        } finally {
            ktx.close();
        }
    }

    @Test
    void filterNodesUsingPreparedEntityFilter() throws Exception {
        createNodeFulltextIndex(NODE_INDEX_NAME);
        long alice;
        long bob;
        long carol;
        try (Transaction tx = db.beginTx()) {
            alice = createNodeWithProperty(tx, LABEL, "name", "Alice");
            bob = createNodeWithProperty(tx, LABEL, "name", "Bob");
            carol = createNodeWithProperty(tx, LABEL, "name", "Carol");
            tx.commit();
        }

        KernelTransactionImplementation ktx = getKernelTransaction();
        try {
            Read read = ktx.dataRead();
            IndexDescriptor index = ktx.schemaRead().indexGetForName(NODE_INDEX_NAME);
            IndexReadSession indexSession = read.indexReadSession(index);
            assertThat(indexSession.reader()).isInstanceOf(EntityFilterIndexReader.class);
            try (EntityFilterBuilder builder = ((EntityFilterIndexReader) indexSession.reader())
                    .newEntityFilterBuilder(EmptyMemoryTracker.INSTANCE)) {
                builder.add(alice);
                builder.add(carol);
                EntityFilterPredicate preparedFilter = PropertyIndexQuery.entityFilter(builder.build());
                assertQueryFindsIds(ktx, true, NODE_INDEX_NAME, "name", preparedFilter, alice, carol);
            }
        } finally {
            ktx.close();
        }
    }

    @Test
    void filterRelationships() throws Exception {
        createRelationshipFulltextIndex(REL_INDEX_NAME);
        long node1;
        long node2;
        long alice;
        long bob;
        long carol;
        try (Transaction tx = db.beginTx()) {
            node1 = createNodeWithProperty(tx, LABEL, "name", "Node1");
            node2 = createNodeWithProperty(tx, LABEL, "name", "Node2");
            alice = createRelationshipWithProperty(tx, node1, node2, "name", "Alice");
            bob = createRelationshipWithProperty(tx, node1, node2, "name", "Bob");
            carol = createRelationshipWithProperty(tx, node1, node2, "name", "Carol");
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            KernelTransaction ktx = kernelTransaction(tx);
            assertQueryFindsIds(ktx, false, REL_INDEX_NAME, "name", entityFilter());
            assertQueryFindsIds(ktx, false, REL_INDEX_NAME, "name", entityFilter(alice), alice);
            assertQueryFindsIds(ktx, false, REL_INDEX_NAME, "name", entityFilter(bob), bob);
            assertQueryFindsIds(ktx, false, REL_INDEX_NAME, "name", entityFilter(carol), carol);
            assertQueryFindsIds(ktx, false, REL_INDEX_NAME, "name", entityFilter(alice, bob), alice, bob);
            assertQueryFindsIds(ktx, false, REL_INDEX_NAME, "name", entityFilter(alice, carol), alice, carol);
            assertQueryFindsIds(ktx, false, REL_INDEX_NAME, "name", entityFilter(bob, carol), bob, carol);
            assertQueryFindsIds(ktx, false, REL_INDEX_NAME, "name", entityFilter(alice, bob, carol), alice, bob, carol);
        }
    }

    @Test
    void filterRelationshipsFuzzTest() throws Exception {
        createRelationshipFulltextIndex(REL_INDEX_NAME);
        long firstNode;
        long secondNode;
        try (Transaction tx = db.beginTx()) {
            firstNode = createNodeWithProperty(tx, LABEL, "name", "Node1");
            secondNode = createNodeWithProperty(tx, LABEL, "name", "Node2");
            tx.commit();
        }
        List<Long> allRels = new ArrayList<>();
        try (Transaction tx = db.beginTx()) {
            for (int i = 0; i < NUM_ENTITIES; i++) {
                allRels.add(createRelationshipWithProperty(tx, firstNode, secondNode, PROP, "fuzzword"));
            }
            tx.commit();
        }

        KernelTransactionImplementation ktx = getKernelTransaction();
        try {
            // sanity check: the query matches all relationships
            assertQueryFindsIds(
                    ktx,
                    false,
                    REL_INDEX_NAME,
                    "fuzzword",
                    allRels.stream().mapToLong(Long::longValue).toArray());

            for (int i = 0; i < ITERATIONS; i++) {
                Set<Long> subset = randomSubset(allRels);
                assertQueryFindsIds(
                        ktx,
                        false,
                        REL_INDEX_NAME,
                        "fuzzword",
                        entityFilter(from(subset)),
                        subset.stream().mapToLong(Long::longValue).toArray());
            }
        } finally {
            ktx.close();
        }
    }

    private Set<Long> randomSubset(List<Long> values) {
        int n = random.nextInt(values.size());
        Set<Long> set = new HashSet<>();
        for (int i = 0; i < n; i++) {
            set.add(random.among(values));
        }
        return set;
    }

    private static long[] from(Set<Long> ids) {
        return ids.stream().mapToLong(Long::longValue).toArray();
    }

    static EntityFilterPredicate entityFilter(long... entityIds) {
        return PropertyIndexQuery.entityFilter(LongSets.mutable.of(entityIds));
    }

    private void createNodeFulltextIndex(String indexName) throws Exception {
        try (Transaction tx = db.beginTx()) {
            tx.schema()
                    .indexFor(LABEL)
                    .on(PROP)
                    .on("name")
                    .withIndexType(IndexType.FULLTEXT)
                    .withName(indexName)
                    .create();
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexOnline(indexName, 1, TimeUnit.MINUTES);
        }
    }

    private void createRelationshipFulltextIndex(String indexName) throws Exception {
        try (Transaction tx = db.beginTx()) {
            tx.schema()
                    .indexFor(RELTYPE)
                    .on(PROP)
                    .on("name")
                    .withIndexType(IndexType.FULLTEXT)
                    .withName(indexName)
                    .create();
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexOnline(indexName, 1, TimeUnit.MINUTES);
        }
    }
}
