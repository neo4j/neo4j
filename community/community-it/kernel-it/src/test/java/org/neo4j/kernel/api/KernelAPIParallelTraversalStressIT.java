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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.neo4j.io.IOUtils.closeAllUnchecked;
import static org.neo4j.kernel.api.KernelAPIParallelStress.parallelStressInTx;
import static org.neo4j.kernel.api.KernelTransaction.Type.EXPLICIT;
import static org.neo4j.storageengine.api.RelationshipSelection.ALL_RELATIONSHIPS;

import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.impl.coreapi.TransactionImpl;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.query.TransactionalContextFactory;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.token.TokenHolders;
import org.neo4j.values.ElementIdMapper;

@DbmsExtension
class KernelAPIParallelTraversalStressIT {
    private static final int N_THREADS = 10;
    private static final int N_NODES = 10_000;
    private static final int N_RELATIONSHIPS = 4 * N_NODES;

    // the following static fields are needed to create a fake internal transaction
    private static final TokenHolders tokenHolders = mock(TokenHolders.class);
    private static final QueryExecutionEngine engine = mock(QueryExecutionEngine.class);
    private static final TransactionalContextFactory contextFactory = mock(TransactionalContextFactory.class);
    private static final DatabaseAvailabilityGuard availabilityGuard = mock(DatabaseAvailabilityGuard.class);
    private static final ElementIdMapper elementIdMapper = mock(ElementIdMapper.class);

    @Inject
    private GraphDatabaseAPI db;

    @Inject
    private Kernel kernel;

    @Test
    void shouldScanNodesAndTraverseInParallel() throws Throwable {
        createRandomGraph(kernel);

        parallelStressInTx(
                kernel,
                N_THREADS,
                tx -> new NodeAndTraverseCursors(tx, kernel),
                KernelAPIParallelTraversalStressIT::scanAndTraverse);
    }

    private static void createRandomGraph(Kernel kernel) throws Exception {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        long[] nodes = new long[N_NODES];
        KernelTransaction setup = kernel.beginTransaction(EXPLICIT, LoginContext.AUTH_DISABLED);

        int relationshipType = setup.token().relationshipTypeCreateForName("R", false);
        for (int i = 0; i < N_NODES; i++) {
            nodes[i] = setup.dataWrite().nodeCreate();
            if ((i + 1) % 10000 == 0) {
                setup.commit();
                setup = kernel.beginTransaction(EXPLICIT, LoginContext.AUTH_DISABLED);
                new TransactionImpl(tokenHolders, contextFactory, availabilityGuard, engine, setup, elementIdMapper);
            }
        }

        for (int i = 0; i < N_RELATIONSHIPS; i++) {
            int n1 = random.nextInt(N_NODES);
            int n2 = random.nextInt(N_NODES);
            while (n2 == n1) {
                n2 = random.nextInt(N_NODES);
            }
            setup.dataWrite().relationshipCreate(nodes[n1], relationshipType, nodes[n2]);
            if ((i + 1) % 10000 == 0) {
                setup.commit();
                setup = kernel.beginTransaction(EXPLICIT, LoginContext.AUTH_DISABLED);
                new TransactionImpl(tokenHolders, contextFactory, availabilityGuard, engine, setup, elementIdMapper);
            }
        }

        setup.commit();
    }

    private static Runnable scanAndTraverse(Read read, NodeAndTraverseCursors cursors) {
        return () -> {
            try {
                read.allNodesScan(cursors.nodeCursor);
                int n = 0;
                int r = 0;
                while (cursors.nodeCursor.next()) {
                    cursors.nodeCursor.relationships(cursors.traversalCursor, ALL_RELATIONSHIPS);
                    while (cursors.traversalCursor.next()) {
                        r++;
                    }
                    n++;
                }
                assertEquals(N_NODES, n, "correct number of nodes");
                assertEquals(2 * N_RELATIONSHIPS, r, "correct number of traversals");
            } finally {
                cursors.complete();
            }
        };
    }

    static class NodeAndTraverseCursors implements AutoCloseable {
        final NodeCursor nodeCursor;
        final RelationshipTraversalCursor traversalCursor;
        final Statement statement;
        private final ExecutionContext executionContext;

        NodeAndTraverseCursors(KernelTransaction tx, Kernel kernel) {
            statement = tx.acquireStatement();
            executionContext = tx.createExecutionContext();
            nodeCursor = kernel.cursors().allocateNodeCursor(executionContext.cursorContext());
            traversalCursor = kernel.cursors().allocateRelationshipTraversalCursor(executionContext.cursorContext());
        }

        @Override
        public void close() throws Exception {
            closeAllUnchecked(statement, executionContext);
        }

        public void complete() {
            closeAllUnchecked(nodeCursor, traversalCursor);
            executionContext.complete();
        }
    }
}
