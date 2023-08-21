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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unordered;
import static org.neo4j.values.storable.ValueType.GEOGRAPHIC_POINT;
import static org.neo4j.values.storable.ValueType.GEOGRAPHIC_POINT_ARRAY;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.impl.coreapi.TransactionImpl;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueType;
import org.neo4j.values.storable.Values;

@ExtendWith(RandomExtension.class)
@ImpermanentDbmsExtension
class IncomparableValuesIndexRangeQueryTest {
    private static String INDEX_NAME = "TestIndex";
    private static String LABEL = "TestLabel";
    private static String PROPERTY = "testProp";

    @Inject
    private GraphDatabaseService db;

    @Inject
    RandomSupport random;

    @BeforeEach
    void setupRandomConfig() {
        random.reset();
        createIndex();
    }

    @MethodSource("incomparableValueTypes")
    @ParameterizedTest
    void testSeek(ValueType valueType) throws KernelException {
        Range range = prepareData(valueType);

        try (var tx = db.beginTx()) {
            var kernelTx = ((TransactionImpl) tx).kernelTransaction();

            IndexDescriptor index = kernelTx.schemaRead().indexGetForName(INDEX_NAME);
            IndexReadSession indexSession = kernelTx.dataRead().indexReadSession(index);
            int prop = kernelTx.tokenRead().propertyKey(PROPERTY);

            // This is what we test ...
            try (NodeValueIndexCursor cursor = kernelTx.cursors()
                    .allocateNodeValueIndexCursor(kernelTx.cursorContext(), kernelTx.memoryTracker())) {
                var query = PropertyIndexQuery.range(prop, range.from, true, range.to, true);

                kernelTx.dataRead()
                        .nodeIndexSeek(kernelTx.queryContext(), indexSession, cursor, unordered(true), query);
                assertFalse(cursor.next());
            }

            // and this is a sanity check that we got the setup right and the index actually contains the data
            try (NodeValueIndexCursor cursor = kernelTx.cursors()
                    .allocateNodeValueIndexCursor(kernelTx.cursorContext(), kernelTx.memoryTracker())) {
                kernelTx.dataRead()
                        .nodeIndexSeek(
                                kernelTx.queryContext(),
                                indexSession,
                                cursor,
                                unordered(true),
                                PropertyIndexQuery.exists(prop));
                assertTrue(cursor.next());
            }
        }
    }

    @MethodSource("incomparableValueTypes")
    @ParameterizedTest
    void testPartitionedScan(ValueType valueType) throws KernelException {
        // We don't support partitioned scans for geographic values
        assumeTrue(valueType != GEOGRAPHIC_POINT && valueType != GEOGRAPHIC_POINT_ARRAY);
        Range range = prepareData(valueType);

        try (var tx = db.beginTx()) {
            var kernelTx = ((TransactionImpl) tx).kernelTransaction();

            IndexDescriptor index = kernelTx.schemaRead().indexGetForName(INDEX_NAME);
            IndexReadSession indexSession = kernelTx.dataRead().indexReadSession(index);
            int prop = kernelTx.tokenRead().propertyKey(PROPERTY);

            var query = PropertyIndexQuery.range(prop, range.from, true, range.to, true);
            var partitionedScan = kernelTx.dataRead().nodeIndexSeek(indexSession, 2, kernelTx.queryContext(), query);

            try (var statement = kernelTx.acquireStatement();
                    var executionContext = kernelTx.createExecutionContext()) {
                for (int i = 0; i < 2; i++) {
                    try (NodeValueIndexCursor cursor = executionContext
                            .cursors()
                            .allocateNodeValueIndexCursor(kernelTx.cursorContext(), kernelTx.memoryTracker())) {
                        partitionedScan.reservePartition(cursor, executionContext);
                        assertFalse(cursor.next());
                    }
                }

                executionContext.complete();
            }
        }
    }

    private Range prepareData(ValueType valueType) throws KernelException {
        Value[] values = random.randomValues().nextValuesOfTypes(10, valueType);
        Arrays.sort(values, Values.COMPARATOR);
        try (var tx = db.beginTx()) {
            for (Value value : values) {
                var kernelTx = ((TransactionImpl) tx).kernelTransaction();
                var node = kernelTx.dataWrite().nodeCreate();
                int label = kernelTx.tokenRead().nodeLabel(LABEL);
                kernelTx.dataWrite().nodeAddLabel(node, label);
                int prop = kernelTx.tokenRead().propertyKey(PROPERTY);
                kernelTx.dataWrite().nodeSetProperty(node, prop, value);
            }

            tx.commit();
        }

        return new Range(values[0], values[values.length - 1]);
    }

    private void createIndex() {
        try (var tx = db.beginTx()) {
            tx.schema()
                    .indexFor(Label.label(LABEL))
                    .on(PROPERTY)
                    .withIndexType(IndexType.RANGE)
                    .withName(INDEX_NAME)
                    .create();

            tx.commit();
        }

        try (var tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(5, TimeUnit.MINUTES);
        }
    }

    private static Stream<ValueType> incomparableValueTypes() {
        return Stream.of(ValueType.DURATION, ValueType.DURATION_ARRAY, GEOGRAPHIC_POINT, GEOGRAPHIC_POINT_ARRAY);
    }

    record Range(Value from, Value to) {}
}
