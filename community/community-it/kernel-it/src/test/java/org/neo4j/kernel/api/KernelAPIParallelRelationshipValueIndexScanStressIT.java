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
package org.neo4j.kernel.api;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unorderedValues;
import static org.neo4j.kernel.api.KernelTransaction.Type.EXPLICIT;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.impl.coreapi.schema.IndexDefinitionImpl;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@DbmsExtension
@ExtendWith(RandomExtension.class)
class KernelAPIParallelRelationshipValueIndexScanStressIT {
    private static final int N_THREADS = 10;
    private static final int N_RELS = 10_000;

    @Inject
    private GraphDatabaseAPI db;

    @Inject
    private Kernel kernel;

    @Inject
    private RandomSupport random;

    @Test
    void shouldDoParallelIndexScans() throws Throwable {
        // Given
        try (org.neo4j.graphdb.Transaction tx = db.beginTx()) {
            createRelationships(tx, N_RELS, "TYPE1", "prop");
            createRelationships(tx, N_RELS, "TYPE2", "prop");
            createRelationships(tx, N_RELS, "TYPE3", "prop");
            tx.commit();
        }

        IndexDescriptor index1;
        IndexDescriptor index2;
        IndexDescriptor index3;
        try (org.neo4j.graphdb.Transaction tx = db.beginTx()) {
            index1 = unwrap(tx.schema()
                    .indexFor(RelationshipType.withName("TYPE1"))
                    .on("prop")
                    .create());
            index2 = unwrap(tx.schema()
                    .indexFor(RelationshipType.withName("TYPE2"))
                    .on("prop")
                    .create());
            index3 = unwrap(tx.schema()
                    .indexFor(RelationshipType.withName("TYPE3"))
                    .on("prop")
                    .create());
            tx.commit();
        }

        try (org.neo4j.graphdb.Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(10, MINUTES);
            tx.commit();
        }

        // when & then
        IndexReadSession[] indexes = new IndexReadSession[3];
        try (KernelTransaction tx = kernel.beginTransaction(EXPLICIT, LoginContext.AUTH_DISABLED)) {
            indexes[0] = indexReadSession(tx, index1);
            indexes[1] = indexReadSession(tx, index2);
            indexes[2] = indexReadSession(tx, index3);
            tx.commit();
        }

        KernelAPIParallelStress.parallelStressInTx(
                kernel,
                N_THREADS,
                tx -> {
                    var statement = tx.acquireStatement();
                    var executionContext = tx.createExecutionContext();
                    var cursor = tx.cursors()
                            .allocateRelationshipValueIndexCursor(
                                    executionContext.cursorContext(), EmptyMemoryTracker.INSTANCE);
                    return new WorkerContext<>(cursor, executionContext, tx, statement);
                },
                (read, workerContext) -> indexSeek(
                        read,
                        new WorkerQueryContext(
                                workerContext.getTransaction().queryContext(),
                                workerContext.getContext().cursorContext()),
                        workerContext,
                        indexes[random.nextInt(indexes.length)]));
    }

    private static IndexDescriptor unwrap(IndexDefinition indexDefinition) {
        return ((IndexDefinitionImpl) indexDefinition).getIndexReference();
    }

    private static IndexReadSession indexReadSession(KernelTransaction tx, IndexDescriptor index)
            throws IndexNotFoundKernelException {
        return tx.dataRead().indexReadSession(index);
    }

    private static void createRelationships(
            org.neo4j.graphdb.Transaction tx, int count, String labelName, String propKey) {
        RelationshipType type = RelationshipType.withName(labelName);
        for (int i = 0; i < count; i++) {
            Node from = tx.createNode();
            Node to = tx.createNode();
            var rel = from.createRelationshipTo(to, type);
            rel.setProperty(propKey, i);
        }
    }

    private static Runnable indexSeek(
            Read read,
            QueryContext queryContext,
            WorkerContext<RelationshipValueIndexCursor> workerContext,
            IndexReadSession index) {
        return () -> {
            try {
                var query = PropertyIndexQuery.exists(index.reference().schema().getPropertyIds()[0]);
                var cursor = workerContext.getCursor();
                read.relationshipIndexSeek(queryContext, index, cursor, unorderedValues(), query);
                int n = 0;
                while (cursor.next()) {
                    n++;
                }
                assertThat(n).as("correct number of relationships").isEqualTo(N_RELS);
            } catch (KernelException e) {
                throw new RuntimeException(e);
            } finally {
                workerContext.complete();
            }
        };
    }
}
