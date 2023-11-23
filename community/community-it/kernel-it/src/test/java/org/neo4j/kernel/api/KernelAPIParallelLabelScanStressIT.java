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
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;
import static org.neo4j.kernel.api.KernelTransaction.Type.EXPLICIT;
import static org.neo4j.test.Race.throwing;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.TokenPredicate;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@DbmsExtension
@ExtendWith(RandomExtension.class)
class KernelAPIParallelLabelScanStressIT {
    private static final int N_THREADS = 10;
    private static final int N_NODES = 10_000;

    @Inject
    private GraphDatabaseAPI db;

    @Inject
    private RandomSupport random;

    @Inject
    private Kernel kernel;

    @Test
    void shouldDoParallelLabelScans() throws Throwable {
        int[] labels = new int[3];

        // Create nodes with labels
        try (KernelTransaction tx = kernel.beginTransaction(EXPLICIT, LoginContext.AUTH_DISABLED)) {
            labels[0] = createLabeledNodes(tx, N_NODES, "LABEL1");
            labels[1] = createLabeledNodes(tx, N_NODES, "LABEL2");
            labels[2] = createLabeledNodes(tx, N_NODES, "LABEL3");
            tx.commit();
        }

        IndexDescriptor nodeLabelIndex;
        try (KernelTransaction tx = kernel.beginTransaction(EXPLICIT, LoginContext.AUTH_DISABLED)) {
            nodeLabelIndex = tx.schemaRead()
                    .index(SchemaDescriptors.ANY_TOKEN_NODE_SCHEMA_DESCRIPTOR)
                    .next();
            tx.commit();
        }

        KernelAPIParallelStress.parallelStressInTx(
                kernel,
                N_THREADS,
                tx -> {
                    var statement = tx.acquireStatement();
                    var executionContext = tx.createExecutionContext();
                    var cursor = tx.cursors().allocateNodeLabelIndexCursor(executionContext.cursorContext());
                    return new WorkerContext<>(cursor, executionContext, tx, statement);
                },
                (read, workerContext) ->
                        labelScan(read, workerContext, nodeLabelIndex, labels[random.nextInt(labels.length)]));
    }

    private static int createLabeledNodes(KernelTransaction tx, int nNodes, String labelName) throws KernelException {
        int label = tx.tokenWrite().labelGetOrCreateForName(labelName);
        for (int i = 0; i < nNodes; i++) {
            long n = tx.dataWrite().nodeCreate();
            tx.dataWrite().nodeAddLabel(n, label);
        }
        return label;
    }

    private static Runnable labelScan(
            Read read, WorkerContext<NodeLabelIndexCursor> workerContext, IndexDescriptor index, int label) {
        return throwing(() -> {
            try {
                var tokenReadSession = read.tokenReadSession(index);
                var cursor = workerContext.getCursor();
                var cursorContext = workerContext.getContext().cursorContext();
                read.nodeLabelScan(tokenReadSession, cursor, unconstrained(), new TokenPredicate(label), cursorContext);

                int n = 0;
                while (cursor.next()) {
                    n++;
                }
                assertEquals(N_NODES, n, "correct number of nodes");
            } finally {
                workerContext.complete();
            }
        });
    }
}
