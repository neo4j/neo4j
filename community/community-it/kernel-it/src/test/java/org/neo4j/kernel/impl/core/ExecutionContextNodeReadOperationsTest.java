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
package org.neo4j.kernel.impl.core;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.IOUtils;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.ExecutionContext;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.impl.api.parallel.ExecutionContextNode;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;

class ExecutionContextNodeReadOperationsTest extends NodeReadOperationsTest {

    private final Map<Transaction, ExecutionContext> executionContexts = new HashMap<>();

    @AfterEach
    void afterEach() {
        executionContexts.values().forEach(ExecutionContext::complete);
        IOUtils.closeAllUnchecked(executionContexts.values());
    }

    @Override
    PageCursorTracer reportCursorEventsAndGetTracer(Transaction tx) {
        var executionContext = getExecutionContext(tx);
        executionContext.report();
        assertZeroTracer(executionContext.cursorContext());
        return executionContext.cursorContext().getCursorTracer();
    }

    @Override
    protected Node lookupNode(Transaction tx, String id) {
        var executionContext = getExecutionContext(tx);
        var internalId = executionContext.elementIdMapper().nodeId(id);
        return new ExecutionContextNode(internalId, executionContext);
    }

    private ExecutionContext getExecutionContext(Transaction tx) {
        return executionContexts.computeIfAbsent(tx, t -> {
            var ktx = ((InternalTransaction) t).kernelTransaction();
            try (Statement statement = ktx.acquireStatement()) {
                return ktx.createExecutionContext();
            }
        });
    }
}
