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
package org.neo4j.kernel.impl.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.schema.IndexDefinitionImpl;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.values.storable.TextValue;

/**
 * Does test having multiple iterators open on the same index
 * <ul>
 * <li>Exhaust variations:</li>
 * <ul>
 * <li>Exhaust iterators one by one</li>
 * <li>Nesting</li>
 * <li>Interleaved</li>
 * </ul>
 * <li>Happy case for schema index iterators on static db for:</li>
 * <ul>
 * <li>Single property number index</li>
 * <li>Single property string index</li>
 * <li>Composite property number index</li>
 * <li>Composite property string index</li>
 * </ul>
 * <li>For index queries:</li>
 * <ul>
 * <li>{@link PropertyIndexQuery#exists(int)}</li>
 * <li>{@link PropertyIndexQuery#exact(int, Object)}</li>
 * <li>{@link PropertyIndexQuery#range(int, Number, boolean, Number, boolean)}</li>
 * <li>{@link PropertyIndexQuery#range(int, String, boolean, String, boolean)}</li>
 * </ul>
 * </ul>
 * Does NOT test
 * <ul>
 * <li>Single property unique number index</li>
 * <li>Single property unique string index</li>
 * <li>Composite property mixed index</li>
 * <li>{@link PropertyIndexQuery#stringPrefix(int, TextValue)}</li>
 * <li>{@link PropertyIndexQuery#stringSuffix(int, TextValue)}</li>
 * <li>{@link PropertyIndexQuery#stringContains(int, TextValue)}</li>
 * <li>Composite property node key index (due to it being enterprise feature)</li>
 * <li>Label index iterators</li>
 * <li>Concurrency</li>
 * <li>Locking</li>
 * <li>Cluster</li>
 * <li>Index creation</li>
 * </ul>
 * Code navigation:
 */
@DbmsExtension
@ExtendWith(RandomExtension.class)
class MultipleOpenCursorsTest {
    private static final Label indexLabel = Label.label("IndexLabel");
    private static final String numberProp1 = "numberProp1";
    private static final String numberProp2 = "numberProp2";
    private static final String stringProp1 = "stringProp1";
    private static final String stringProp2 = "stringProp2";

    @Inject
    private GraphDatabaseAPI db;

    @Inject
    private RandomSupport rnd;

    public static Stream<Arguments> params() {
        return Stream.of(
                Arguments.of(
                        new NumberIndexCoordinator(indexLabel, numberProp1, numberProp2, stringProp1, stringProp2)),
                Arguments.of(
                        new StringIndexCoordinator(indexLabel, numberProp1, numberProp2, stringProp1, stringProp2)),
                Arguments.of(new NumberCompositeIndexCoordinator(
                        indexLabel, numberProp1, numberProp2, stringProp1, stringProp2)),
                Arguments.of(new StringCompositeIndexCoordinator(
                        indexLabel, numberProp1, numberProp2, stringProp1, stringProp2)));
    }

    public String name;

    @ParameterizedTest
    @MethodSource(value = "params")
    void multipleCursorsNotNestedExists(IndexCoordinator indexCoordinator) throws Exception {
        indexCoordinator.init(db);
        try (Transaction tx = db.beginTx()) {
            KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
            // when
            try (NodeValueIndexCursor cursor1 = indexCoordinator.queryExists(ktx);
                    NodeValueIndexCursor cursor2 = indexCoordinator.queryExists(ktx)) {
                List<Long> actual1 = asList(cursor1);
                List<Long> actual2 = asList(cursor2);

                // then
                indexCoordinator.assertExistsResult(actual1);
                indexCoordinator.assertExistsResult(actual2);
            }

            tx.commit();
        }
    }

    @ParameterizedTest
    @MethodSource(value = "params")
    void multipleCursorsNotNestedExact(IndexCoordinator indexCoordinator) throws Exception {
        indexCoordinator.init(db);
        try (Transaction tx = db.beginTx()) {
            // when
            KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
            try (NodeValueIndexCursor cursor1 = indexCoordinator.queryExact(ktx);
                    NodeValueIndexCursor cursor2 = indexCoordinator.queryExact(ktx)) {
                List<Long> actual1 = asList(cursor1);
                List<Long> actual2 = asList(cursor2);

                // then
                indexCoordinator.assertExactResult(actual1);
                indexCoordinator.assertExactResult(actual2);
            }
            tx.commit();
        }
    }

    @ParameterizedTest
    @MethodSource(value = "params")
    void multipleIteratorsNotNestedRange(IndexCoordinator indexCoordinator) throws KernelException {
        assumeTrue(indexCoordinator.supportRangeQuery());
        indexCoordinator.init(db);
        try (Transaction tx = db.beginTx()) {
            // when
            KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
            try (NodeValueIndexCursor cursor1 = indexCoordinator.queryRange(ktx);
                    NodeValueIndexCursor cursor2 = indexCoordinator.queryRange(ktx)) {
                List<Long> actual1 = asList(cursor1);
                List<Long> actual2 = asList(cursor2);

                // then
                indexCoordinator.assertRangeResult(actual1);
                indexCoordinator.assertRangeResult(actual2);
            }
            tx.commit();
        }
    }

    @ParameterizedTest
    @MethodSource(value = "params")
    void multipleIteratorsNestedInnerNewExists(IndexCoordinator indexCoordinator) throws Exception {
        indexCoordinator.init(db);
        try (Transaction tx = db.beginTx()) {
            // when
            KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
            try (NodeValueIndexCursor cursor1 = indexCoordinator.queryExists(ktx)) {
                List<Long> actual1 = new ArrayList<>();
                while (cursor1.next()) {
                    actual1.add(cursor1.nodeReference());

                    try (NodeValueIndexCursor cursor2 = indexCoordinator.queryExists(ktx)) {
                        List<Long> actual2 = asList(cursor2);
                        indexCoordinator.assertExistsResult(actual2);
                    }
                }
                // then
                indexCoordinator.assertExistsResult(actual1);
            }
            tx.commit();
        }
    }

    @ParameterizedTest
    @MethodSource(value = "params")
    void multipleIteratorsNestedInnerNewExact(IndexCoordinator indexCoordinator) throws Exception {
        indexCoordinator.init(db);
        try (Transaction tx = db.beginTx()) {
            // when
            KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
            try (NodeValueIndexCursor cursor1 = indexCoordinator.queryExact(ktx)) {
                List<Long> actual1 = new ArrayList<>();
                while (cursor1.next()) {
                    actual1.add(cursor1.nodeReference());
                    try (NodeValueIndexCursor cursor2 = indexCoordinator.queryExact(ktx)) {
                        List<Long> actual2 = asList(cursor2);
                        indexCoordinator.assertExactResult(actual2);
                    }
                }
                // then
                indexCoordinator.assertExactResult(actual1);
            }
            tx.commit();
        }
    }

    @ParameterizedTest
    @MethodSource(value = "params")
    void multipleIteratorsNestedInnerNewRange(IndexCoordinator indexCoordinator) throws Exception {
        assumeTrue(indexCoordinator.supportRangeQuery());
        indexCoordinator.init(db);
        try (Transaction tx = db.beginTx()) {
            // when
            KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
            try (NodeValueIndexCursor cursor1 = indexCoordinator.queryRange(ktx)) {
                List<Long> actual1 = new ArrayList<>();
                while (cursor1.next()) {
                    actual1.add(cursor1.nodeReference());
                    try (NodeValueIndexCursor cursor2 = indexCoordinator.queryRange(ktx)) {
                        List<Long> actual2 = asList(cursor2);
                        indexCoordinator.assertRangeResult(actual2);
                    }
                }
                // then
                indexCoordinator.assertRangeResult(actual1);
            }
            tx.commit();
        }
    }

    @ParameterizedTest
    @MethodSource(value = "params")
    void multipleIteratorsNestedInterleavedExists(IndexCoordinator indexCoordinator) throws Exception {
        indexCoordinator.init(db);
        try (Transaction tx = db.beginTx()) {
            // when
            KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
            try (NodeValueIndexCursor cursor1 = indexCoordinator.queryExists(ktx)) {
                List<Long> actual1 = new ArrayList<>();
                try (NodeValueIndexCursor cursor2 = indexCoordinator.queryExists(ktx)) {
                    List<Long> actual2 = new ArrayList<>();

                    // Interleave
                    exhaustInterleaved(cursor1, actual1, cursor2, actual2);

                    // then
                    indexCoordinator.assertExistsResult(actual1);
                    indexCoordinator.assertExistsResult(actual2);
                }
            }
            tx.commit();
        }
    }

    @ParameterizedTest
    @MethodSource(value = "params")
    void multipleIteratorsNestedInterleavedExact(IndexCoordinator indexCoordinator) throws Exception {
        indexCoordinator.init(db);
        try (Transaction tx = db.beginTx()) {
            // when
            KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
            try (NodeValueIndexCursor cursor1 = indexCoordinator.queryExact(ktx)) {
                List<Long> actual1 = new ArrayList<>();
                try (NodeValueIndexCursor cursor2 = indexCoordinator.queryExact(ktx)) {
                    List<Long> actual2 = new ArrayList<>();

                    // Interleave
                    exhaustInterleaved(cursor1, actual1, cursor2, actual2);

                    // then
                    indexCoordinator.assertExactResult(actual1);
                    indexCoordinator.assertExactResult(actual2);
                }
            }
            tx.commit();
        }
    }

    @ParameterizedTest
    @MethodSource(value = "params")
    void multipleIteratorsNestedInterleavedRange(IndexCoordinator indexCoordinator) throws Exception {
        assumeTrue(indexCoordinator.supportRangeQuery());
        indexCoordinator.init(db);
        try (Transaction tx = db.beginTx()) {
            // when
            KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
            try (NodeValueIndexCursor cursor1 = indexCoordinator.queryRange(ktx);
                    NodeValueIndexCursor cursor2 = indexCoordinator.queryRange(ktx)) {
                List<Long> actual1 = new ArrayList<>();

                List<Long> actual2 = new ArrayList<>();

                // Interleave
                exhaustInterleaved(cursor1, actual1, cursor2, actual2);

                // then
                indexCoordinator.assertRangeResult(actual1);
                indexCoordinator.assertRangeResult(actual2);
            }
            tx.commit();
        }
    }

    private static List<Long> asList(NodeValueIndexCursor cursor) {
        List<Long> list = new ArrayList<>();
        while (cursor.next()) {
            list.add(cursor.nodeReference());
        }
        return list;
    }

    private void exhaustInterleaved(
            NodeValueIndexCursor source1, List<Long> target1, NodeValueIndexCursor source2, List<Long> target2) {
        boolean source1HasNext = true;
        boolean source2HasNext = true;
        while (source1HasNext && source2HasNext) {
            if (rnd.nextBoolean()) {
                source1HasNext = source1.next();
                if (source1HasNext) {
                    target1.add(source1.nodeReference());
                }
            } else {
                source2HasNext = source2.next();
                if (source2HasNext) {
                    target2.add(source2.nodeReference());
                }
            }
        }

        // Empty the rest
        while (source1.next()) {
            target1.add(source1.nodeReference());
        }
        while (source2.next()) {
            target2.add(source2.nodeReference());
        }
    }

    private static class StringCompositeIndexCoordinator extends IndexCoordinator {
        StringCompositeIndexCoordinator(
                Label indexLabel, String numberProp1, String numberProp2, String stringProp1, String stringProp2) {
            super(indexLabel, numberProp1, numberProp2, stringProp1, stringProp2);
        }

        @Override
        boolean supportRangeQuery() {
            return false;
        }

        @Override
        NodeValueIndexCursor queryRange(KernelTransaction ktx) {
            throw new UnsupportedOperationException();
        }

        @Override
        NodeValueIndexCursor queryExists(KernelTransaction ktx) throws KernelException {
            return indexQuery(
                    ktx,
                    indexDescriptor,
                    PropertyIndexQuery.exists(stringPropId1),
                    PropertyIndexQuery.exists(stringPropId2));
        }

        @Override
        NodeValueIndexCursor queryExact(KernelTransaction ktx) throws KernelException {
            return indexQuery(
                    ktx,
                    indexDescriptor,
                    PropertyIndexQuery.exact(stringPropId1, stringProp1Values[0]),
                    PropertyIndexQuery.exact(stringPropId2, stringProp2Values[0]));
        }

        @Override
        void assertRangeResult(List<Long> result) {
            throw new UnsupportedOperationException();
        }

        @Override
        void assertExactResult(List<Long> actual) {
            List<Long> expected = new ArrayList<>();
            expected.add(0L);
            assertSameContent(actual, expected);
        }

        @Override
        IndexDefinition doCreateIndex(Transaction tx) {
            return tx.schema()
                    .indexFor(indexLabel)
                    .on(stringProp1)
                    .on(stringProp2)
                    .create();
        }

        @Override
        public String toString() {
            return "Composite string non unique";
        }
    }

    private static class NumberCompositeIndexCoordinator extends IndexCoordinator {
        NumberCompositeIndexCoordinator(
                Label indexLabel, String numberProp1, String numberProp2, String stringProp1, String stringProp2) {
            super(indexLabel, numberProp1, numberProp2, stringProp1, stringProp2);
        }

        @Override
        boolean supportRangeQuery() {
            return false;
        }

        @Override
        NodeValueIndexCursor queryRange(KernelTransaction ktx) {
            throw new UnsupportedOperationException();
        }

        @Override
        NodeValueIndexCursor queryExists(KernelTransaction ktx) throws KernelException {
            return indexQuery(
                    ktx,
                    indexDescriptor,
                    PropertyIndexQuery.exists(numberPropId1),
                    PropertyIndexQuery.exists(numberPropId2));
        }

        @Override
        NodeValueIndexCursor queryExact(KernelTransaction ktx) throws KernelException {
            return indexQuery(
                    ktx,
                    indexDescriptor,
                    PropertyIndexQuery.exact(numberPropId1, numberProp1Values[0]),
                    PropertyIndexQuery.exact(numberPropId2, numberProp2Values[0]));
        }

        @Override
        void assertRangeResult(List<Long> actual) {
            throw new UnsupportedOperationException();
        }

        @Override
        void assertExactResult(List<Long> actual) {
            List<Long> expected = new ArrayList<>();
            expected.add(0L);
            assertSameContent(actual, expected);
        }

        @Override
        IndexDefinition doCreateIndex(Transaction tx) {
            return tx.schema()
                    .indexFor(indexLabel)
                    .on(numberProp1)
                    .on(numberProp2)
                    .create();
        }

        @Override
        public String toString() {
            return "Composite number non unique";
        }
    }

    private static class StringIndexCoordinator extends IndexCoordinator {
        StringIndexCoordinator(
                Label indexLabel, String numberProp1, String numberProp2, String stringProp1, String stringProp2) {
            super(indexLabel, numberProp1, numberProp2, stringProp1, stringProp2);
        }

        @Override
        boolean supportRangeQuery() {
            return true;
        }

        @Override
        NodeValueIndexCursor queryRange(KernelTransaction ktx) throws KernelException {
            // query for half the range
            return indexQuery(
                    ktx,
                    indexDescriptor,
                    PropertyIndexQuery.range(
                            numberPropId1, stringProp1Values[0], true, stringProp1Values[numberOfNodes / 2], false));
        }

        @Override
        NodeValueIndexCursor queryExists(KernelTransaction ktx) throws KernelException {
            return indexQuery(ktx, indexDescriptor, PropertyIndexQuery.exists(stringPropId1));
        }

        @Override
        NodeValueIndexCursor queryExact(KernelTransaction ktx) throws KernelException {
            return indexQuery(ktx, indexDescriptor, PropertyIndexQuery.exact(stringPropId1, stringProp1Values[0]));
        }

        @Override
        void assertRangeResult(List<Long> actual) {
            List<Long> expected = new ArrayList<>();
            for (long i = 0; i < numberOfNodes / 2; i++) {
                expected.add(i);
            }
            assertSameContent(actual, expected);
        }

        @Override
        void assertExactResult(List<Long> actual) {
            List<Long> expected = new ArrayList<>();
            expected.add(0L);
            assertSameContent(actual, expected);
        }

        @Override
        IndexDefinition doCreateIndex(Transaction tx) {
            return tx.schema().indexFor(indexLabel).on(stringProp1).create();
        }

        @Override
        public String toString() {
            return "Single string non unique";
        }
    }

    private static class NumberIndexCoordinator extends IndexCoordinator {
        NumberIndexCoordinator(
                Label indexLabel, String numberProp1, String numberProp2, String stringProp1, String stringProp2) {
            super(indexLabel, numberProp1, numberProp2, stringProp1, stringProp2);
        }

        @Override
        boolean supportRangeQuery() {
            return true;
        }

        @Override
        NodeValueIndexCursor queryRange(KernelTransaction ktx) throws KernelException {
            // query for half the range
            return indexQuery(
                    ktx,
                    indexDescriptor,
                    PropertyIndexQuery.range(
                            numberPropId1, numberProp1Values[0], true, numberProp1Values[numberOfNodes / 2], false));
        }

        @Override
        NodeValueIndexCursor queryExists(KernelTransaction ktx) throws KernelException {
            return indexQuery(ktx, indexDescriptor, PropertyIndexQuery.exists(numberPropId1));
        }

        @Override
        NodeValueIndexCursor queryExact(KernelTransaction ktx) throws KernelException {
            return indexQuery(ktx, indexDescriptor, PropertyIndexQuery.exact(numberPropId1, numberProp1Values[0]));
        }

        @Override
        void assertRangeResult(List<Long> actual) {
            List<Long> expected = new ArrayList<>();
            for (long i = 0; i < numberOfNodes / 2; i++) {
                expected.add(i);
            }
            assertSameContent(actual, expected);
        }

        @Override
        void assertExactResult(List<Long> actual) {
            List<Long> expected = new ArrayList<>();
            expected.add(0L);
            assertSameContent(actual, expected);
        }

        @Override
        IndexDefinition doCreateIndex(Transaction tx) {
            return tx.schema().indexFor(indexLabel).on(numberProp1).create();
        }

        @Override
        public String toString() {
            return "Single number non unique";
        }
    }

    private abstract static class IndexCoordinator {
        final int numberOfNodes = 100;

        final Label indexLabel;
        final String numberProp1;
        final String numberProp2;
        final String stringProp1;
        final String stringProp2;

        Number[] numberProp1Values;
        Number[] numberProp2Values;
        String[] stringProp1Values;
        String[] stringProp2Values;

        int indexedLabelId;
        int numberPropId1;
        int numberPropId2;
        int stringPropId1;
        int stringPropId2;
        IndexDescriptor indexDescriptor;

        IndexCoordinator(
                Label indexLabel, String numberProp1, String numberProp2, String stringProp1, String stringProp2) {
            this.indexLabel = indexLabel;
            this.numberProp1 = numberProp1;
            this.numberProp2 = numberProp2;
            this.stringProp1 = stringProp1;
            this.stringProp2 = stringProp2;

            this.numberProp1Values = new Number[numberOfNodes];
            this.numberProp2Values = new Number[numberOfNodes];
            this.stringProp1Values = new String[numberOfNodes];
            this.stringProp2Values = new String[numberOfNodes];

            // EXISTING DATA:
            // 100 nodes with properties:
            // numberProp1: 0-99
            // numberProp2: 0-99
            // stringProp1: "string-0"-"string-99"
            // stringProp2: "string-0"-"string-99"
            for (int i = 0; i < numberOfNodes; i++) {
                numberProp1Values[i] = i;
                numberProp2Values[i] = i;
                stringProp1Values[i] = "string-" + String.format("%02d", i);
                stringProp2Values[i] = "string-" + String.format("%02d", i);
            }
        }

        void init(GraphDatabaseAPI db) {
            try (Transaction tx = db.beginTx()) {
                for (int i = 0; i < numberOfNodes; i++) {
                    Node node = tx.createNode(indexLabel);
                    node.setProperty(numberProp1, numberProp1Values[i]);
                    node.setProperty(numberProp2, numberProp2Values[i]);
                    node.setProperty(stringProp1, stringProp1Values[i]);
                    node.setProperty(stringProp2, stringProp2Values[i]);
                }
                tx.commit();
            }

            try (Transaction tx = db.beginTx()) {
                TokenRead tokenRead =
                        ((InternalTransaction) tx).kernelTransaction().tokenRead();
                indexedLabelId = tokenRead.nodeLabel(indexLabel.name());
                numberPropId1 = tokenRead.propertyKey(numberProp1);
                numberPropId2 = tokenRead.propertyKey(numberProp2);
                stringPropId1 = tokenRead.propertyKey(stringProp1);
                stringPropId2 = tokenRead.propertyKey(stringProp2);
                tx.commit();
            }

            createIndex(db);
        }

        private void createIndex(GraphDatabaseAPI db) {
            try (Transaction tx = db.beginTx()) {
                IndexDefinitionImpl indexDefinition = (IndexDefinitionImpl) doCreateIndex(tx);
                indexDescriptor = indexDefinition.getIndexReference();
                tx.commit();
            }
            try (Transaction tx = db.beginTx()) {
                tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);
                tx.commit();
            }
        }

        abstract boolean supportRangeQuery();

        abstract NodeValueIndexCursor queryRange(KernelTransaction ktx) throws KernelException;

        abstract NodeValueIndexCursor queryExists(KernelTransaction ktx) throws KernelException;

        abstract NodeValueIndexCursor queryExact(KernelTransaction ktx) throws KernelException;

        abstract void assertRangeResult(List<Long> result);

        void assertExistsResult(List<Long> actual) {
            List<Long> expected = new ArrayList<>();
            for (long i = 0; i < numberOfNodes; i++) {
                expected.add(i);
            }
            assertSameContent(actual, expected);
        }

        static void assertSameContent(List<Long> actual, List<Long> expected) {
            assertThat(actual).containsAll(expected);
        }

        abstract void assertExactResult(List<Long> result);

        abstract IndexDefinition doCreateIndex(Transaction tx);

        static NodeValueIndexCursor indexQuery(
                KernelTransaction ktx, IndexDescriptor indexDescriptor, PropertyIndexQuery... indexQueries)
                throws KernelException {
            NodeValueIndexCursor cursor =
                    ktx.cursors().allocateNodeValueIndexCursor(ktx.cursorContext(), ktx.memoryTracker());
            IndexReadSession index = ktx.dataRead().indexReadSession(indexDescriptor);
            ktx.dataRead().nodeIndexSeek(ktx.queryContext(), index, cursor, unconstrained(), indexQueries);
            return cursor;
        }
    }
}
