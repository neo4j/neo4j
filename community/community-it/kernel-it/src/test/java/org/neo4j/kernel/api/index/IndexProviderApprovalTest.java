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
package org.neo4j.kernel.api.index;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_BYTE_ARRAY;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_DOUBLE_ARRAY;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_INT_ARRAY;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_LONG_ARRAY;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_STRING_ARRAY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexCreator;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.internal.helpers.Strings;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Values;

/*
 * The purpose of this test class is to make sure all index providers produce the same results.
 *
 * Indexes should always produce the same result as scanning all nodes and checking properties. By extending this
 * class in the index provider module, all value types will be checked against the index provider.
 */
public abstract class IndexProviderApprovalTest {
    // These are the values that will be checked.
    public enum TestValue {
        BOOLEAN_TRUE(true),
        BOOLEAN_FALSE(false),
        STRING_TRUE("true"),
        STRING_FALSE("false"),
        STRING_UPPER_A("A"),
        STRING_LOWER_A("a"),
        CHAR_UPPER_A('A'),
        CHAR_LOWER_A('a'),
        INT_42(42),
        LONG_42((long) 42),
        LARGE_LONG_1(4611686018427387905L),
        LARGE_LONG_2(4611686018427387907L),
        BYTE_42((byte) 42),
        DOUBLE_42((double) 42),
        DOUBLE_42andAHalf(42.5d),
        SHORT_42((short) 42),
        FLOAT_42((float) 42),
        FLOAT_42andAHalf(42.5f),
        POINT_123456_GPS(Values.pointValue(CoordinateReferenceSystem.WGS_84, 12.3, 45.6)),
        POINT_123456_CAR(Values.pointValue(CoordinateReferenceSystem.CARTESIAN, 123, 456)),
        POINT_123456_GPS_3D(Values.pointValue(CoordinateReferenceSystem.WGS_84_3D, 12.3, 45.6, 78.9)),
        POINT_123456_CAR_3D(Values.pointValue(CoordinateReferenceSystem.CARTESIAN_3D, 123, 456, 789)),
        ARRAY_OF_INTS(new int[] {1, 2, 3}),
        ARRAY_OF_LONGS(new long[] {1, 2, 3}),
        ARRAY_OF_LARGE_LONGS_1(new long[] {4611686018427387905L}),
        ARRAY_OF_LARGE_LONGS_2(new long[] {4611686018427387906L}),
        ARRAY_OF_LARGE_LONGS_3(new Long[] {4611686018425387907L}),
        ARRAY_OF_LARGE_LONGS_4(new Long[] {4611686018425387908L}),
        ARRAY_OF_BOOL_LIKE_STRING(new String[] {"true", "false", "true"}),
        ARRAY_OF_BOOLS(new boolean[] {true, false, true}),
        ARRAY_OF_DOUBLES(new double[] {1, 2, 3}),
        ARRAY_OF_STRING(new String[] {"1", "2", "3"}),
        EMPTY_ARRAY_OF_INTS(EMPTY_INT_ARRAY),
        EMPTY_ARRAY_OF_LONGS(EMPTY_LONG_ARRAY),
        EMPTY_ARRAY_OF_BOOLS(EMPTY_BYTE_ARRAY),
        EMPTY_ARRAY_OF_DOUBLES(EMPTY_DOUBLE_ARRAY),
        EMPTY_ARRAY_OF_STRING(EMPTY_STRING_ARRAY),
        ONE(new String[] {"", "||"}),
        OTHER(new String[] {"||", ""}),
        ANOTHER_ARRAY_OF_STRING(new String[] {"1|2|3"}),
        ARRAY_OF_CHAR(new char[] {'1', '2', '3'}),
        ARRAY_OF_POINTS_GPS(new PointValue[] {Values.pointValue(CoordinateReferenceSystem.WGS_84, 12.3, 45.6)}),
        ARRAY_OF_POINTS_CAR(new PointValue[] {Values.pointValue(CoordinateReferenceSystem.CARTESIAN, 123, 456)}),
        ARRAY_OF_POINTS_GPS_3D(
                new PointValue[] {Values.pointValue(CoordinateReferenceSystem.WGS_84_3D, 12.3, 45.6, 78.9)}),
        ARRAY_OF_POINTS_CAR_3D(
                new PointValue[] {Values.pointValue(CoordinateReferenceSystem.CARTESIAN_3D, 123, 456, 789)});

        private final Object value;

        TestValue(Object value) {
            this.value = value;
        }
    }

    private static Map<TestValue, Set<Object>> noIndexRun;
    private static Map<TestValue, Set<Object>> indexRun;

    public static void setupBeforeAllTests(DatabaseManagementService managementService, IndexType indexType) {
        GraphDatabaseService db = managementService.database(DEFAULT_DATABASE_NAME);
        for (TestValue value : TestValue.values()) {
            createNode(db, PROPERTY_KEY, value.value);
        }

        noIndexRun = runFindByLabelAndProperty(db);
        createIndex(db, indexType, label(LABEL), PROPERTY_KEY);
        indexRun = runFindByLabelAndProperty(db);
        managementService.shutdown();
    }

    public static final String LABEL = "Person";
    private static final String PROPERTY_KEY = "name";
    private static final Function<Node, Object> PROPERTY_EXTRACTOR = node -> {
        Object value = node.getProperty(PROPERTY_KEY);
        if (value.getClass().isArray()) {
            return new ArrayEqualityObject(value);
        }
        return value;
    };

    @ParameterizedTest
    @EnumSource(TestValue.class)
    public void test(TestValue currentValue) {
        Set<Object> noIndexResult = Iterables.asSet(noIndexRun.get(currentValue));
        Set<Object> indexResult = Iterables.asSet(indexRun.get(currentValue));

        String errorMessage = currentValue.toString();

        if (!noIndexResult.equals(indexResult)) {
            System.out.println("");
        }
        assertEquals(noIndexResult, indexResult, errorMessage);
    }

    private static Map<TestValue, Set<Object>> runFindByLabelAndProperty(GraphDatabaseService db) {
        Map<TestValue, Set<Object>> results = new HashMap<>();
        try (Transaction tx = db.beginTx()) {
            for (TestValue value : TestValue.values()) {
                addToResults(tx, results, value);
            }
            tx.commit();
        }
        return results;
    }

    private static void createNode(GraphDatabaseService db, String propertyKey, Object value) {
        try (Transaction tx = db.beginTx()) {
            Node node = tx.createNode(label(LABEL));
            node.setProperty(propertyKey, value);
            tx.commit();
        }
    }

    private static void addToResults(Transaction tx, Map<TestValue, Set<Object>> results, TestValue value) {
        try (ResourceIterator<Node> foundNodes = tx.findNodes(label(LABEL), PROPERTY_KEY, value.value)) {
            results.put(value, asSet(Iterators.map(PROPERTY_EXTRACTOR, foundNodes)));
        }
    }

    private static class ArrayEqualityObject {
        private final Object array;

        ArrayEqualityObject(Object array) {
            this.array = array;
        }

        @Override
        public int hashCode() {
            return ArrayUtils.hashCode(array);
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof ArrayEqualityObject && Objects.deepEquals(array, ((ArrayEqualityObject) obj).array);
        }

        @Override
        public String toString() {
            return Strings.prettyPrint(array);
        }
    }

    private static void createIndex(GraphDatabaseService db, IndexType indexType, Label label, String... properties) {
        IndexDefinition indexDef;
        try (Transaction tx = db.beginTx()) {
            IndexCreator indexCreator = tx.schema().indexFor(label).withIndexType(indexType);
            for (String property : properties) {
                indexCreator = indexCreator.on(property);
            }
            indexDef = indexCreator.create();
            tx.commit();
        }

        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexOnline(indexDef, 1, MINUTES);
        }
    }
}
