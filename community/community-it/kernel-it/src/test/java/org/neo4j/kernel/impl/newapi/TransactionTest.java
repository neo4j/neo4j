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
package org.neo4j.kernel.impl.newapi;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;

import org.junit.jupiter.api.Test;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.KernelTransaction;

class TransactionTest extends KernelAPIWriteTestBase<WriteTestSupport> {
    @Test
    void shouldRollbackWhenTxIsNotSuccess() throws Exception {
        // GIVEN
        long nodeId;
        int labelId;
        try (KernelTransaction tx = beginTransaction()) {
            // WHEN
            nodeId = tx.dataWrite().nodeCreate();
            labelId = tx.tokenWrite().labelGetOrCreateForName("labello");
            tx.dataWrite().nodeAddLabel(nodeId, labelId);

            // OBS: not marked as tx.success();
        }

        // THEN
        assertNoNode(nodeId);
    }

    @Test
    void shouldRollbackWhenTxIsFailed() throws Exception {
        // GIVEN
        long nodeId;
        int labelId;
        try (KernelTransaction tx = beginTransaction()) {
            // WHEN
            nodeId = tx.dataWrite().nodeCreate();
            labelId = tx.tokenWrite().labelGetOrCreateForName("labello");
            tx.dataWrite().nodeAddLabel(nodeId, labelId);

            tx.rollback();
        }

        // THEN
        assertNoNode(nodeId);
    }

    // HELPERS

    private void assertNoNode(long nodeId) throws TransactionFailureException {
        try (KernelTransaction tx = beginTransaction();
                NodeCursor cursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT)) {
            tx.dataRead().singleNode(nodeId, cursor);
            assertFalse(cursor.next());
        }
    }

    @Override
    public WriteTestSupport newTestSupport() {
        return new WriteTestSupport();
    }
}
