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

import static org.neo4j.kernel.impl.newapi.IndexReadAsserts.assertNodeCount;
import static org.neo4j.kernel.impl.newapi.IndexReadAsserts.assertNodes;

import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.TokenPredicate;
import org.neo4j.internal.kernel.api.TokenReadSession;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.KernelTransaction;

public class NodeLabelTokenIndexCursorTest extends KernelAPIWriteTestBase<WriteTestSupport> {

    private static final String labelOneName = "Label1";
    private static final String labelTwoName = "Label2";
    private static final String labelThreeName = "Label3";
    private static final String labelFirstName = "Label4";

    @Override
    public WriteTestSupport newTestSupport() {
        return new WriteTestSupport();
    }

    private int labelOne;
    private int labelTwo;
    private int labelThree;
    private int labelFirst;

    @BeforeAll
    void setupClass() throws Exception {
        try (KernelTransaction tx = beginTransaction()) {
            TokenWrite tokenWrite = tx.tokenWrite();
            labelOne = tokenWrite.labelGetOrCreateForName(labelOneName);
            labelTwo = tokenWrite.labelGetOrCreateForName(labelTwoName);
            labelThree = tokenWrite.labelGetOrCreateForName(labelThreeName);
            labelFirst = tokenWrite.labelGetOrCreateForName(labelFirstName);
        }
    }

    @Test
    void shouldFindNodesByLabel() throws Exception {
        long toDelete;
        try (KernelTransaction tx = beginTransaction()) {
            createNode(tx.dataWrite(), labelOne, labelFirst);
            createNode(tx.dataWrite(), labelTwo, labelFirst);
            createNode(tx.dataWrite(), labelThree, labelFirst);
            toDelete = createNode(tx.dataWrite(), labelOne);
            createNode(tx.dataWrite(), labelTwo);
            createNode(tx.dataWrite(), labelThree);
            createNode(tx.dataWrite(), labelThree);
            tx.commit();
        }

        try (KernelTransaction tx = beginTransaction()) {
            tx.dataWrite().nodeDelete(toDelete);
            tx.commit();
        }

        try (KernelTransaction tx = beginTransaction()) {
            org.neo4j.internal.kernel.api.Read read = tx.dataRead();

            var session = getTokenReadSession(tx);

            CursorContext cursorContext = tx.cursorContext();
            try (NodeLabelIndexCursor cursor = tx.cursors().allocateNodeLabelIndexCursor(cursorContext)) {
                MutableLongSet uniqueIds = new LongHashSet();

                // WHEN
                read.nodeLabelScan(
                        session,
                        cursor,
                        IndexQueryConstraints.unconstrained(),
                        new TokenPredicate(labelOne),
                        cursorContext);

                // THEN
                assertNodeCount(cursor, 1, uniqueIds);

                // WHEN
                read.nodeLabelScan(
                        session,
                        cursor,
                        IndexQueryConstraints.unconstrained(),
                        new TokenPredicate(labelTwo),
                        cursorContext);

                // THEN
                assertNodeCount(cursor, 2, uniqueIds);

                // WHEN
                read.nodeLabelScan(
                        session,
                        cursor,
                        IndexQueryConstraints.unconstrained(),
                        new TokenPredicate(labelThree),
                        cursorContext);

                // THEN
                assertNodeCount(cursor, 3, uniqueIds);

                // WHEN
                uniqueIds.clear();
                read.nodeLabelScan(
                        session,
                        cursor,
                        IndexQueryConstraints.unconstrained(),
                        new TokenPredicate(labelFirst),
                        cursorContext);

                // THEN
                assertNodeCount(cursor, 3, uniqueIds);
            }
        }
    }

    private static TokenReadSession getTokenReadSession(KernelTransaction tx) throws IndexNotFoundKernelException {
        var indexes = tx.schemaRead().index(SchemaDescriptors.ANY_TOKEN_NODE_SCHEMA_DESCRIPTOR);
        return tx.dataRead().tokenReadSession(indexes.next());
    }

    @Test
    void shouldFindNodesByLabelInTx() throws Exception {
        long inStore;
        long deletedInTx;
        long createdInTx;

        try (KernelTransaction tx = beginTransaction()) {
            inStore = createNode(tx.dataWrite(), labelOne);
            createNode(tx.dataWrite(), labelTwo);
            deletedInTx = createNode(tx.dataWrite(), labelOne);
            tx.commit();
        }

        try (KernelTransaction tx = beginTransaction()) {
            tx.dataWrite().nodeDelete(deletedInTx);
            createdInTx = createNode(tx.dataWrite(), labelOne);

            createNode(tx.dataWrite(), labelTwo);

            Read read = tx.dataRead();

            var session = getTokenReadSession(tx);

            CursorContext cursorContext = tx.cursorContext();
            try (NodeLabelIndexCursor cursor = tx.cursors().allocateNodeLabelIndexCursor(cursorContext)) {
                MutableLongSet uniqueIds = new LongHashSet();

                // when
                read.nodeLabelScan(
                        session,
                        cursor,
                        IndexQueryConstraints.unconstrained(),
                        new TokenPredicate(labelOne),
                        cursorContext);

                // then
                assertNodes(cursor, uniqueIds, inStore, createdInTx);
            }
        }
    }

    private static long createNode(Write write, int... labels) throws KernelException {
        long nodeId = write.nodeCreate();
        for (int label : labels) {
            write.nodeAddLabel(nodeId, label);
        }
        return nodeId;
    }
}
