/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.newapi.parallel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.graphdb.RelationshipType.withName;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.api.parallel.ExecutionContextProcedureTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

@DbmsExtension
class ExecutionContextProcedureTransactionIT {

    @Inject
    private GraphDatabaseAPI db;

    @Test
    void testGettingAllNodes() {
        List<String> nodeIds = new ArrayList<>();
        try (var tx = db.beginTx()) {
            nodeIds.add(tx.createNode().getElementId());
            nodeIds.add(tx.createNode().getElementId());
            nodeIds.add(tx.createNode().getElementId());
            tx.commit();
        }

        try (var tx = db.beginTx()) {
            var retrievedIds = executionContextTransaction(tx).getAllNodes().stream()
                    .map(Entity::getElementId)
                    .collect(Collectors.toList());
            assertThat(retrievedIds).containsExactlyInAnyOrderElementsOf(nodeIds);
        }
    }

    @Test
    void testGettingAllRelationships() {
        List<String> relIds = new ArrayList<>();

        try (var tx = db.beginTx()) {
            var node = tx.createNode();
            relIds.add(node.createRelationshipTo(node, withName("T1")).getElementId());
            relIds.add(node.createRelationshipTo(node, withName("T2")).getElementId());
            relIds.add(node.createRelationshipTo(node, withName("T3")).getElementId());
            tx.commit();
        }

        try (var tx = db.beginTx()) {
            var retrievedIds = executionContextTransaction(tx).getAllRelationships().stream()
                    .map(Entity::getElementId)
                    .collect(Collectors.toList());
            assertThat(retrievedIds).containsExactlyInAnyOrderElementsOf(relIds);
        }
    }

    private static Transaction executionContextTransaction(Transaction tx) {
        var internalTx = ((InternalTransaction) tx);
        var ktx = internalTx.kernelTransaction();
        internalTx.registerCloseableResource(ktx.acquireStatement());
        var executionContext = ktx.createExecutionContext();
        internalTx.registerCloseableResource(() -> {
            executionContext.complete();
            executionContext.close();
        });
        return new ExecutionContextProcedureTransaction(executionContext);
    }
}
