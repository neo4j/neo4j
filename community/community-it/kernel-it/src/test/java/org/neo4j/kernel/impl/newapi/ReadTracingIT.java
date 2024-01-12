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

import static java.util.concurrent.TimeUnit.HOURS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;
import static org.neo4j.internal.kernel.api.PropertyIndexQuery.exact;
import static org.neo4j.internal.kernel.api.PropertyIndexQuery.fulltextSearch;
import static org.neo4j.token.api.TokenConstants.ANY_LABEL;
import static org.neo4j.token.api.TokenConstants.ANY_RELATIONSHIP_TYPE;
import static org.neo4j.values.storable.Values.stringValue;

import org.junit.jupiter.api.Test;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.Cursor;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

@DbmsExtension
class ReadTracingIT {
    @Inject
    private GraphDatabaseAPI database;

    private final Label label = Label.label("marker");
    private final String property = "property";
    private final String testPropertyValue = "abc";
    private final String indexName = "indexName";
    private final RelationshipType type = RelationshipType.withName("type");

    @Test
    void tracePageCacheAccessOnNodeIndexSeek() throws KernelException {
        createNodeConstraint();
        createMatchingNode();

        try (InternalTransaction transaction = (InternalTransaction) database.beginTx()) {
            var kernelTransaction = transaction.kernelTransaction();
            var dataRead = kernelTransaction.dataRead();
            var indexDescriptor = kernelTransaction.schemaRead().indexGetForName(indexName);
            var cursorContext = kernelTransaction.cursorContext();
            int propertyId = kernelTransaction.tokenRead().propertyKey(property);

            assertZeroCursor(cursorContext);

            var indexSession = dataRead.indexReadSession(indexDescriptor);
            try (var cursor = kernelTransaction
                    .cursors()
                    .allocateNodeValueIndexCursor(
                            kernelTransaction.cursorContext(), kernelTransaction.memoryTracker())) {
                dataRead.nodeIndexSeek(
                        kernelTransaction.queryContext(),
                        indexSession,
                        cursor,
                        unconstrained(),
                        exact(propertyId, stringValue(testPropertyValue)));

                consumeCursor(cursor);
            }

            assertOneCursor(cursorContext);
            assertThat(cursorContext.getCursorTracer().faults()).isZero();
        }
    }

    @Test
    void noPageCacheTracingAvailableOnRelationshipIndexSeek() throws KernelException {
        createRelationshipIndex();
        try (Transaction tx = database.beginTx()) {
            var source = tx.createNode(label);
            var target = tx.createNode(label);
            var relationship = source.createRelationshipTo(target, type);
            relationship.setProperty(property, testPropertyValue);
            tx.commit();
        }

        try (InternalTransaction transaction = (InternalTransaction) database.beginTx()) {
            var kernelTransaction = transaction.kernelTransaction();
            var dataRead = kernelTransaction.dataRead();
            var indexDescriptor = kernelTransaction.schemaRead().indexGetForName(indexName);
            IndexReadSession indexReadSession = kernelTransaction.dataRead().indexReadSession(indexDescriptor);
            var cursorContext = kernelTransaction.cursorContext();

            assertZeroCursor(cursorContext);

            try (var cursor = kernelTransaction
                    .cursors()
                    .allocateRelationshipValueIndexCursor(
                            kernelTransaction.cursorContext(), kernelTransaction.memoryTracker())) {
                dataRead.relationshipIndexSeek(
                        kernelTransaction.queryContext(),
                        indexReadSession,
                        cursor,
                        unconstrained(),
                        fulltextSearch(testPropertyValue));

                consumeCursor(cursor);
            }

            assertZeroCursor(cursorContext);
        }
    }

    @Test
    void tracePageCacheAccessOnNodeIndexScan() throws KernelException {
        createNodeConstraint();
        createMatchingNode();

        try (InternalTransaction transaction = (InternalTransaction) database.beginTx()) {
            var kernelTransaction = transaction.kernelTransaction();
            var dataRead = kernelTransaction.dataRead();
            var indexDescriptor = kernelTransaction.schemaRead().indexGetForName(indexName);
            var cursorContext = kernelTransaction.cursorContext();

            assertZeroCursor(cursorContext);

            var indexSession = dataRead.indexReadSession(indexDescriptor);
            try (var cursor = kernelTransaction
                    .cursors()
                    .allocateNodeValueIndexCursor(
                            kernelTransaction.cursorContext(), kernelTransaction.memoryTracker())) {
                dataRead.nodeIndexScan(indexSession, cursor, unconstrained());

                consumeCursor(cursor);
            }

            assertOneCursor(cursorContext);
            assertThat(cursorContext.getCursorTracer().faults()).isZero();
        }
    }

    @Test
    void tracePageCacheAccessOnNodeCountByLabel() {
        try (InternalTransaction transaction = (InternalTransaction) database.beginTx()) {
            var kernelTransaction = transaction.kernelTransaction();
            var cursorContext = kernelTransaction.cursorContext();
            var dataRead = kernelTransaction.dataRead();

            assertZeroCursor(cursorContext);

            dataRead.countsForNode(0);

            assertOneCursor(cursorContext);
        }
    }

    @Test
    void tracePageCacheAccessOnNodeCount() {
        try (InternalTransaction transaction = (InternalTransaction) database.beginTx()) {
            var kernelTransaction = transaction.kernelTransaction();
            var cursorContext = kernelTransaction.cursorContext();
            var dataRead = kernelTransaction.dataRead();

            assertZeroCursor(cursorContext);

            assertEquals(0, dataRead.nodesGetCount());

            assertOneCursor(cursorContext);
        }
    }

    @Test
    void tracePageCacheAccessOnRelationshipCount() {
        try (InternalTransaction transaction = (InternalTransaction) database.beginTx()) {
            var kernelTransaction = transaction.kernelTransaction();
            var cursorContext = kernelTransaction.cursorContext();
            var dataRead = kernelTransaction.dataRead();

            assertZeroCursor(cursorContext);

            dataRead.countsForRelationship(ANY_LABEL, ANY_RELATIONSHIP_TYPE, ANY_LABEL);

            assertOneCursor(cursorContext);
        }
    }

    private static void consumeCursor(Cursor cursor) {
        while (cursor.next()) {
            // consume
        }
    }

    private void createMatchingNode() {
        try (Transaction tx = database.beginTx()) {
            var node = tx.createNode(label);
            node.setProperty(property, testPropertyValue);
            tx.commit();
        }
    }

    private void createNodeConstraint() {
        try (Transaction tx = database.beginTx()) {
            tx.schema()
                    .constraintFor(label)
                    .assertPropertyIsUnique(property)
                    .withName(indexName)
                    .create();
            tx.commit();
        }
    }

    private void createRelationshipIndex() {
        database.executeTransactionally("CREATE FULLTEXT INDEX " + indexName + "  FOR ()-[r:" + type.name()
                + "]-() ON EACH [r." + property + "]");

        try (Transaction tx = database.beginTx()) {
            tx.schema().awaitIndexesOnline(1, HOURS);
        }
    }

    private static void assertOneCursor(CursorContext cursorContext) {
        PageCursorTracer cursorTracer = cursorContext.getCursorTracer();
        assertThat(cursorTracer.pins()).isOne();
        assertThat(cursorTracer.unpins()).isOne();
        assertThat(cursorTracer.hits()).isOne();
    }

    private static void assertZeroCursor(CursorContext cursorContext) {
        PageCursorTracer cursorTracer = cursorContext.getCursorTracer();
        assertThat(cursorTracer.pins()).isZero();
        assertThat(cursorTracer.unpins()).isZero();
        assertThat(cursorTracer.hits()).isZero();
        assertThat(cursorTracer.faults()).isZero();
    }
}
