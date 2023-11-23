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

import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.TokenPredicate;
import org.neo4j.internal.kernel.api.TokenReadSession;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.kernel.api.KernelTransaction;

public class NodeLabelIndexOrderTest extends TokenIndexOrderTestBase<NodeLabelIndexCursor> {

    @Override
    protected NodeLabelIndexCursor getIndexCursor(KernelTransaction tx) {
        return tx.cursors().allocateNodeLabelIndexCursor(tx.cursorContext());
    }

    @Override
    protected long entityReference(NodeLabelIndexCursor cursor) {
        return cursor.nodeReference();
    }

    @Override
    protected void tokenScan(IndexOrder indexOrder, KernelTransaction tx, int label, NodeLabelIndexCursor cursor)
            throws KernelException {
        IndexDescriptor index = tx.schemaRead()
                .index(SchemaDescriptors.ANY_TOKEN_NODE_SCHEMA_DESCRIPTOR)
                .next();
        TokenReadSession tokenReadSession = tx.dataRead().tokenReadSession(index);
        tx.dataRead()
                .nodeLabelScan(
                        tokenReadSession,
                        cursor,
                        IndexQueryConstraints.ordered(indexOrder),
                        new TokenPredicate(label),
                        tx.cursorContext());
    }

    @Override
    protected int tokenByName(KernelTransaction tx, String name) {
        return tx.tokenRead().nodeLabel(name);
    }

    @Override
    protected long entityWithToken(KernelTransaction tx, String name) throws Exception {
        Write write = tx.dataWrite();
        long node = write.nodeCreate();
        write.nodeAddLabel(node, tx.tokenWrite().labelGetOrCreateForName(name));
        return node;
    }
}
