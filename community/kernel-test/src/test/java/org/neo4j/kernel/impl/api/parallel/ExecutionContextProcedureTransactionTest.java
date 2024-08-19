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
package org.neo4j.kernel.impl.api.parallel;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.kernel.api.ExecutionContext;
import org.neo4j.kernel.api.KernelTransaction;

public class ExecutionContextProcedureTransactionTest {

    @Test
    void getAllNodesShouldRegisterAndUnregisterAsResource() {
        ExecutionContext executionContext = mock(ExecutionContext.class);
        var tx = new ExecutionContextProcedureTransaction(
                new ExecutionContextProcedureKernelTransaction(mock(KernelTransaction.class), executionContext), null);
        ResourceIterable<Node> nodes = tx.getAllNodes();

        verify(executionContext, times(1)).registerCloseableResource(eq(nodes));
        verify(executionContext, never()).unregisterCloseableResource(any());

        clearInvocations(executionContext);

        nodes.close();
        verify(executionContext, never()).registerCloseableResource(eq(nodes));
        verify(executionContext, times(1)).unregisterCloseableResource(eq(nodes));
    }

    @Test
    void getAllRelationshipsShouldRegisterAndUnregisterAsResource() {
        ExecutionContext executionContext = mock(ExecutionContext.class);
        var tx = new ExecutionContextProcedureTransaction(
                new ExecutionContextProcedureKernelTransaction(mock(KernelTransaction.class), executionContext), null);
        ResourceIterable<Relationship> relationships = tx.getAllRelationships();

        verify(executionContext, times(1)).registerCloseableResource(eq(relationships));
        verify(executionContext, never()).unregisterCloseableResource(any());

        clearInvocations(executionContext);

        relationships.close();
        verify(executionContext, never()).registerCloseableResource(eq(relationships));
        verify(executionContext, times(1)).unregisterCloseableResource(eq(relationships));
    }
}
