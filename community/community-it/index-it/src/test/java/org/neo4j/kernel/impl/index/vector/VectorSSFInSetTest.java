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
package org.neo4j.kernel.impl.index.vector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.tuple;
import static org.neo4j.internal.kernel.api.PropertyIndexQuery.nearestNeighbors;
import static org.neo4j.kernel.impl.index.vector.VectorSSFQueryResult.extractor;
import static org.neo4j.kernel.impl.index.vector.VectorSSFQueryResult.field;
import static org.neo4j.values.storable.DurationValue.duration;
import static org.neo4j.values.storable.ValueType.BOOLEAN;
import static org.neo4j.values.storable.ValueType.DATE;
import static org.neo4j.values.storable.ValueType.DATE_TIME;
import static org.neo4j.values.storable.ValueType.DOUBLE;
import static org.neo4j.values.storable.ValueType.DURATION;
import static org.neo4j.values.storable.ValueType.FLOAT;
import static org.neo4j.values.storable.ValueType.INT;
import static org.neo4j.values.storable.ValueType.LOCAL_DATE_TIME;
import static org.neo4j.values.storable.ValueType.LOCAL_TIME;
import static org.neo4j.values.storable.ValueType.LONG;
import static org.neo4j.values.storable.ValueType.STRING;
import static org.neo4j.values.storable.ValueType.TIME;

import java.time.ZoneId;
import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.Token;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.index.vector.VectorSSFQueryResult.ResultList;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomSupportExtension;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.FloatArray;
import org.neo4j.values.storable.TemporalValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueType;
import org.neo4j.values.storable.Values;

@RandomSupportExtension
public class VectorSSFInSetTest extends VectorSSFTestBase {

    @Inject
    RandomSupport random;

    @Test
    void singleIntegerPropertyInSetForNodes() throws Exception {
        singleIntegerPropertyInSet(new NodeVectorIndexMethods());
    }

    @Test
    void singleIntegerPropertyInSetForRelationships() throws Exception {
        singleIntegerPropertyInSet(new RelationshipVectorIndexMethods());
    }

    static void singleIntegerPropertyInSet(VectorIndexMethods indexMethods) throws Exception {
        indexMethods.createTestIndex(VECTOR_INDEX_NAME, EMBEDDINGS.dimensions(), EMBEDDING_NAME, "id");
        indexMethods.createTestEntity(Map.of("id", 10, "name", "Alice", EMBEDDING_NAME, EMBEDDINGS.get(1)));
        indexMethods.createTestEntity(Map.of("id", 20, "name", "Bob", EMBEDDING_NAME, EMBEDDINGS.get(2)));
        indexMethods.createTestEntity(Map.of("id", 30, "name", "Carol", EMBEDDING_NAME, EMBEDDINGS.get(3)));
        indexMethods.createTestEntity(Map.of("id", 40, "name", "Ted", EMBEDDING_NAME, EMBEDDINGS.get(4)));
        indexMethods.createTestEntity(Map.of("id", 50, "name", "Bob", EMBEDDING_NAME, EMBEDDINGS.get(5)));

        assertThat(indexMethods.queryTestIndex(inSetQuery("id", of(10, 20))))
                .hasSize(2)
                .extracting(extractor("name"), extractor("id"))
                .containsExactlyInAnyOrder(tuple("Alice", 10), tuple("Bob", 20));

        assertThat(indexMethods.queryTestIndex(inSetQuery("id", of(10))))
                .singleElement()
                .has(field("name", "Alice"));

        assertThat(indexMethods.queryTestIndex(inSetQuery("id", of()))).isEmpty();

        assertThat(indexMethods.queryTestIndex(inSetQuery("id", of(8)))).isEmpty();

        assertThat(indexMethods.queryTestIndex(inSetQuery("id", of(8, 12, 15, 16, 17, 18, 500))))
                .isEmpty();

        assertThat(indexMethods.queryTestIndex(inSetQuery("id", of(8, 10, 12, 15, 16, 17, 18, 500))))
                .singleElement()
                .has(field("name", "Alice"));

        assertThat(indexMethods.queryTestIndex(inSetQuery("id", of(10, 8, 12, 15, 16, 17, 18, 500))))
                .singleElement()
                .has(field("name", "Alice"));

        assertThat(indexMethods.queryTestIndex(inSetQuery("id", of(8, 12, 15, 16, 17, 18, 10, 500))))
                .singleElement()
                .has(field("name", "Alice"));

        assertThat(indexMethods.queryTestIndex(inSetQuery("id", of(10, 8, 12, 10, 15, 16, 17, 18, 10, 500))))
                .singleElement()
                .has(field("name", "Alice"));
    }

    private static Value[] of(Object... objects) {
        Value[] values = new Value[objects.length];
        for (int i = 0; i < objects.length; i++) {
            values[i] = Values.of(objects[i]);
        }
        return values;
    }

    @Test
    void stringAndIntegerAndFloatForNodes() throws Exception {
        stringAndIntegerAndFloat(new NodeVectorIndexMethods());
    }

    @Test
    void stringAndIntegerAndFloatForRelationships() throws Exception {
        stringAndIntegerAndFloat(new RelationshipVectorIndexMethods());
    }

    static void stringAndIntegerAndFloat(VectorIndexMethods indexMethods) throws Exception {

        // in order to break the index id ordering being 1,2,3
        indexMethods.createTestEntity(Map.of("id", 103, "priority", 15, "story", "Once upon a time", "age", 128));
        indexMethods.createTestEntity(Map.of("id", 104, "shoesize", 11.5, "story", "In a galaxy far, far away"));

        indexMethods.createTestIndex(
                VECTOR_INDEX_NAME, EMBEDDINGS.dimensions(), EMBEDDING_NAME, "name", "age", "shoesize");
        indexMethods.createTestEntity(
                Map.of("id", 10, "name", "Alice", "age", 23, "shoesize", 8.5, EMBEDDING_NAME, EMBEDDINGS.get(1)));
        indexMethods.createTestEntity(
                Map.of("id", 20, "name", "Bob", "age", 18, "shoesize", 8.5, EMBEDDING_NAME, EMBEDDINGS.get(2)));
        indexMethods.createTestEntity(
                Map.of("id", 30, "name", "Carol", "age", 45, "shoesize", 8.5, EMBEDDING_NAME, EMBEDDINGS.get(3)));
        indexMethods.createTestEntity(
                Map.of("id", 40, "name", "Ted", "age", 23, "shoesize", 8.5, EMBEDDING_NAME, EMBEDDINGS.get(4)));
        indexMethods.createTestEntity(
                Map.of("id", 50, "name", "Bob", "age", 45, "shoesize", 8.5, EMBEDDING_NAME, EMBEDDINGS.get(5)));

        // order of exact queries consistent with the index
        assertThat(indexMethods.queryTestIndex(
                        inSetQuery("name", of("Bob", "Alice")),
                        exactQuery("age", Values.of(45)),
                        exactQuery("shoesize", Values.of(8.5))))
                .singleElement()
                .has(field("name", "Bob"))
                .has(field("age", 45));

        assertThat(indexMethods.queryTestIndex(
                        inSetQuery("name", of("Bob", "Alice")),
                        inSetQuery("age", of(45, 24)),
                        exactQuery("shoesize", Values.of(8.5))))
                .singleElement()
                .has(field("name", "Bob"))
                .has(field("age", 45));

        assertThat(indexMethods.queryTestIndex(
                        inSetQuery("name", of("Bob", "Alice")),
                        inSetQuery("age", of(45, 24)),
                        inSetQuery("shoesize", of(8.5f, 11.5f))))
                .singleElement()
                .has(field("name", "Bob"))
                .has(field("age", 45));

        assertThat(indexMethods.queryTestIndex(
                        inSetQuery("name", of("Ted", "Carol")), existsQuery("age"), existsQuery("shoesize")))
                .hasSize(2)
                .extracting(extractor("id"))
                .containsExactlyInAnyOrder(30, 40);
    }

    @Test
    void mixedValueInSetForNodes() throws Exception {
        mixedValueInSet(new NodeVectorIndexMethods());
    }

    @Test
    void mixedValueInSetForRelationships() throws Exception {
        mixedValueInSet(new RelationshipVectorIndexMethods());
    }

    static void mixedValueInSet(VectorIndexMethods indexMethods) throws Exception {
        indexMethods.createTestIndex(VECTOR_INDEX_NAME, EMBEDDINGS.dimensions(), EMBEDDING_NAME, "id", "name");
        indexMethods.createTestEntity(Map.of("id", 10, "name", "Alice", EMBEDDING_NAME, EMBEDDINGS.get(1)));
        indexMethods.createTestEntity(Map.of("id", 20, "name", "Bob", EMBEDDING_NAME, EMBEDDINGS.get(2)));
        indexMethods.createTestEntity(Map.of("id", 30, "name", "Carol", EMBEDDING_NAME, EMBEDDINGS.get(3)));
        indexMethods.createTestEntity(Map.of("id", 40, "name", "Ted", EMBEDDING_NAME, EMBEDDINGS.get(4)));
        indexMethods.createTestEntity(Map.of("id", 50, "name", "Bob", EMBEDDING_NAME, EMBEDDINGS.get(5)));

        assertThat(indexMethods.queryTestIndex(
                        genericInSetQuery("id", Values.of(10), Values.of("hello"), Values.of(3.141)), allQuery("name")))
                .extracting(extractor("name"))
                .containsExactlyInAnyOrder("Alice");

        assertThat(indexMethods.queryTestIndex(
                        allQuery("id"), genericInSetQuery("name", Values.of(15), Values.of("Carol"), Values.of(3.141))))
                .extracting(extractor("id"))
                .containsExactlyInAnyOrder(30);
    }

    @Test
    void emptyValueListForNodes() throws Exception {
        emptyValueList(new NodeVectorIndexMethods());
    }

    @Test
    void emptyValueListForRelationships() throws Exception {
        emptyValueList(new RelationshipVectorIndexMethods());
    }

    static void emptyValueList(VectorIndexMethods indexMethods) throws Exception {
        indexMethods.createTestIndex(VECTOR_INDEX_NAME, EMBEDDINGS.dimensions(), EMBEDDING_NAME, "id", "name");
        indexMethods.createTestEntity(Map.of("id", 10, "name", "Alice", EMBEDDING_NAME, EMBEDDINGS.get(1)));
        indexMethods.createTestEntity(Map.of("id", 20, "name", "Bob", EMBEDDING_NAME, EMBEDDINGS.get(2)));
        indexMethods.createTestEntity(Map.of("id", 30, "name", "Carol", EMBEDDING_NAME, EMBEDDINGS.get(3)));
        indexMethods.createTestEntity(Map.of("id", 40, "name", "Ted", EMBEDDING_NAME, EMBEDDINGS.get(4)));
        indexMethods.createTestEntity(Map.of("id", 50, "name", "Bob", EMBEDDING_NAME, EMBEDDINGS.get(5)));

        assertThat(indexMethods.queryTestIndex(genericInSetQuery("id", Values.of(10)), allQuery("name")))
                .extracting(extractor("name"))
                .containsExactlyInAnyOrder("Alice");

        assertThat(indexMethods.queryTestIndex(genericInSetQuery("id"), allQuery("name")))
                .isEmpty();
    }

    @Test
    void singleFloatValuesForNodes() throws Exception {
        singleFloatValues(new NodeVectorIndexMethods());
    }

    @Test
    void singleFloatValuesForRelationships() throws Exception {
        singleFloatValues(new RelationshipVectorIndexMethods());
    }

    static void singleFloatValues(VectorIndexMethods indexMethods) throws Exception {
        indexMethods.createTestIndex(VECTOR_INDEX_NAME, EMBEDDINGS.dimensions(), EMBEDDING_NAME, "age");
        indexMethods.createTestEntity(Map.of("id", 5, "age", 74.0f, EMBEDDING_NAME, EMBEDDINGS.get(1)));
        indexMethods.createTestEntity(Map.of("id", 10, "age", 75.0f, EMBEDDING_NAME, EMBEDDINGS.get(1)));
        indexMethods.createTestEntity(Map.of("id", 20, "age", 75, EMBEDDING_NAME, EMBEDDINGS.get(2)));
        indexMethods.createTestEntity(Map.of("id", 30, "age", 75.9f, EMBEDDING_NAME, EMBEDDINGS.get(3)));
        indexMethods.createTestEntity(Map.of("id", 40, "age", 76.1f, EMBEDDING_NAME, EMBEDDINGS.get(4)));
        indexMethods.createTestEntity(Map.of("id", 50, "age", 76, EMBEDDING_NAME, EMBEDDINGS.get(5)));

        assertThat(indexMethods.queryTestIndex(genericInSetQuery("age", Values.of(74.0f))))
                .singleElement()
                .has(field("id", 5));
        assertThat(indexMethods.queryTestIndex(genericInSetQuery("age", Values.of(74))))
                .singleElement()
                .has(field("id", 5));
        assertThat(indexMethods.queryTestIndex(genericInSetQuery("age", Values.of(74), Values.of(74.0f))))
                .singleElement()
                .has(field("id", 5));

        assertThat(indexMethods.queryTestIndex(genericInSetQuery("age", Values.of(75))))
                .hasSize(2)
                .extracting(extractor("id"))
                .containsExactlyInAnyOrder(10, 20);
        assertThat(indexMethods.queryTestIndex(genericInSetQuery("age", Values.of(75.0f))))
                .hasSize(2)
                .extracting(extractor("id"))
                .containsExactlyInAnyOrder(10, 20);
        assertThat(indexMethods.queryTestIndex(genericInSetQuery("age", Values.of(75.0f), Values.of(75))))
                .hasSize(2)
                .extracting(extractor("id"))
                .containsExactlyInAnyOrder(10, 20);

        assertThat(indexMethods.queryTestIndex(genericInSetQuery("age", Values.of(75.9f))))
                .singleElement()
                .has(field("id", 30));

        assertThat(indexMethods.queryTestIndex(genericInSetQuery("age", Values.of(76))))
                .singleElement()
                .has(field("id", 50));
        assertThat(indexMethods.queryTestIndex(genericInSetQuery("age", Values.of(76.0f))))
                .singleElement()
                .has(field("id", 50));
        assertThat(indexMethods.queryTestIndex(genericInSetQuery("age", Values.of(76.0f), Values.of(76))))
                .singleElement()
                .has(field("id", 50));
    }

    @Test
    void extremeFloatValuesForNodes() throws Exception {
        extremeFloatValues(new NodeVectorIndexMethods());
    }

    @Test
    void extremeFloatValuesForRelationships() throws Exception {
        extremeFloatValues(new RelationshipVectorIndexMethods());
    }

    static void extremeFloatValues(VectorIndexMethods indexMethods) throws Exception {
        indexMethods.createTestIndex(VECTOR_INDEX_NAME, EMBEDDINGS.dimensions(), EMBEDDING_NAME, "age");
        indexMethods.createTestEntity(Map.of("id", 1, "age", 75, EMBEDDING_NAME, EMBEDDINGS.get(1)));

        assertThat(indexMethods.queryTestIndex(genericInSetQuery("age", Values.of(75))))
                .singleElement()
                .has(field("id", 1));
        assertThat(indexMethods.queryTestIndex(genericInSetQuery(
                        "age",
                        Values.of(Integer.MAX_VALUE),
                        Values.of(Integer.MIN_VALUE),
                        Values.of(Float.NaN),
                        Values.of(Float.POSITIVE_INFINITY),
                        Values.of(Float.NEGATIVE_INFINITY))))
                .isEmpty();
        assertThat(indexMethods.queryTestIndex(genericInSetQuery("age", Values.of(Integer.MAX_VALUE))))
                .isEmpty();
        assertThat(indexMethods.queryTestIndex(genericInSetQuery("age", Values.of(Integer.MIN_VALUE))))
                .isEmpty();
        assertThat(indexMethods.queryTestIndex(genericInSetQuery("age", Values.of(Float.NaN))))
                .isEmpty();
        assertThat(indexMethods.queryTestIndex(genericInSetQuery("age", Values.of(Float.NEGATIVE_INFINITY))))
                .isEmpty();
        assertThat(indexMethods.queryTestIndex(genericInSetQuery("age", Values.of(Float.POSITIVE_INFINITY))))
                .isEmpty();
    }

    @Test
    void durationsForNodes() throws Exception {
        durations(new NodeVectorIndexMethods());
    }

    @Test
    void durationsForRelationships() throws Exception {
        durations(new RelationshipVectorIndexMethods());
    }

    static void durations(VectorIndexMethods indexMethods) throws Exception {
        indexMethods.createTestIndex(VECTOR_INDEX_NAME, EMBEDDINGS.dimensions(), EMBEDDING_NAME, "duration");
        indexMethods.createTestEntity(
                Map.of("id", 10, "duration", duration(0, 0, 0, 1000_000), EMBEDDING_NAME, EMBEDDINGS.get(1)));
        indexMethods.createTestEntity(
                Map.of("id", 20, "duration", duration(0, 0, 1, 0), EMBEDDING_NAME, EMBEDDINGS.get(2)));
        indexMethods.createTestEntity(
                Map.of("id", 30, "duration", duration(0, 1, 0, 0), EMBEDDING_NAME, EMBEDDINGS.get(3)));
        indexMethods.createTestEntity(
                Map.of("id", 40, "duration", duration(0, 1, 1, 0), EMBEDDING_NAME, EMBEDDINGS.get(4)));
        indexMethods.createTestEntity(
                Map.of("id", 50, "duration", duration(1, 1, 1, 1), EMBEDDING_NAME, EMBEDDINGS.get(5)));

        assertThat(indexMethods.queryTestIndex(genericInSetQuery("duration", duration(0, 0, 0, 1000_000))))
                .singleElement()
                .has(field("id", 10));
        assertThat(indexMethods.queryTestIndex(genericInSetQuery("duration", duration(0, 0, 1, 0))))
                .singleElement()
                .has(field("id", 20));
        assertThat(indexMethods.queryTestIndex(genericInSetQuery("duration", duration(0, 1, 1, 0))))
                .singleElement()
                .has(field("id", 40));
        assertThat(indexMethods.queryTestIndex(genericInSetQuery("duration", duration(1, 1, 1, 1))))
                .singleElement()
                .has(field("id", 50));
        assertThat(indexMethods.queryTestIndex(genericInSetQuery("duration", duration(1, 1, 1, 0))))
                .isEmpty();
        assertThat(indexMethods.queryTestIndex(genericInSetQuery("duration", duration(1, 1, 0, 0))))
                .isEmpty();
        assertThat(indexMethods.queryTestIndex(genericInSetQuery("duration", duration(1, 0, 0, 0))))
                .isEmpty();

        assertThat(indexMethods.queryTestIndex(
                        genericInSetQuery("duration", duration(0, 0, 0, 1000_000), duration(0, 0, 1, 0))))
                .hasSize(2)
                .extracting(extractor("id"))
                .containsExactlyInAnyOrder(10, 20);

        assertThat(indexMethods.queryTestIndex(genericInSetQuery(
                        "duration",
                        duration(0, 0, 0, 1000_000),
                        duration(0, 0, 1, 0),
                        duration(1, 1, 1, 0),
                        duration(1, 1, 0, 0),
                        duration(1, 0, 0, 0))))
                .hasSize(2)
                .extracting(extractor("id"))
                .containsExactlyInAnyOrder(10, 20);

        assertThat(indexMethods.queryTestIndex(genericInSetQuery(
                        "duration", duration(1, 1, 1, 0), duration(1, 1, 0, 0), duration(1, 0, 0, 0))))
                .isEmpty();
    }

    @Test
    void booleanInSetForNodes() throws Exception {
        booleanInSet(new NodeVectorIndexMethods());
    }

    @Test
    void booleanInSetForRelationships() throws Exception {
        booleanInSet(new RelationshipVectorIndexMethods());
    }

    static void booleanInSet(VectorIndexMethods indexMethods) throws Exception {
        indexMethods.createTestIndex(VECTOR_INDEX_NAME, EMBEDDINGS.dimensions(), EMBEDDING_NAME, "authorized");
        indexMethods.createTestEntity(Map.of("id", 10, "authorized", true, EMBEDDING_NAME, EMBEDDINGS.get(1)));
        indexMethods.createTestEntity(Map.of("id", 20, "authorized", false, EMBEDDING_NAME, EMBEDDINGS.get(2)));
        indexMethods.createTestEntity(Map.of("id", 30, "authorized", true, EMBEDDING_NAME, EMBEDDINGS.get(2)));
        indexMethods.createTestEntity(Map.of("id", 40, "authorized", false, EMBEDDING_NAME, EMBEDDINGS.get(2)));

        assertThat(indexMethods.queryTestIndex(genericInSetQuery("authorized", BooleanValue.TRUE)))
                .hasSize(2)
                .extracting(extractor("id"))
                .containsExactlyInAnyOrder(10, 30);
        assertThat(indexMethods.queryTestIndex(genericInSetQuery("authorized", BooleanValue.FALSE)))
                .hasSize(2)
                .extracting(extractor("id"))
                .containsExactlyInAnyOrder(20, 40);
        assertThat(indexMethods.queryTestIndex(genericInSetQuery("authorized", BooleanValue.FALSE, BooleanValue.TRUE)))
                .hasSize(4)
                .extracting(extractor("id"))
                .containsExactlyInAnyOrder(10, 20, 30, 40);
    }

    private static final List<String> dateTimeValues = List.of(
            "2019-06-02T21:00:00.000[Atlantic/Cape_Verde]",
            "2019-06-02T21:00:00.000[Etc/GMT+1]",
            "2019-06-02T22:00:00.000+0000",
            "2019-06-02T22:00:00.000[Africa/Abidjan]",
            "2019-06-02T22:00:00.000[Africa/Accra]",
            "2019-06-02T22:00:00.000[Africa/Bamako]");

    private static DateTimeValue dateTimeValue(int i) {
        return DateTimeValue.parse(dateTimeValues.get(i), ZoneId::systemDefault);
    }

    private static DateTimeValue lastDateTimeValue() {
        return DateTimeValue.parse(dateTimeValues.getLast(), ZoneId::systemDefault);
    }

    @Test
    void temporalInSetForNodes() throws Exception {
        temporalInSetForIndexMethods(new NodeVectorIndexMethods());
    }

    @Test
    void temporalInSetForRelationships() throws Exception {
        temporalInSetForIndexMethods(new RelationshipVectorIndexMethods());
    }

    static void temporalInSetForIndexMethods(VectorIndexMethods indexMethods) throws Exception {

        indexMethods.createTestIndex(VECTOR_INDEX_NAME, EMBEDDINGS.dimensions(), EMBEDDING_NAME, "time");
        for (int i = 0; i < dateTimeValues.size() - 1; i++) {
            indexMethods.createTestEntity(
                    Map.of("id", i * 10 + 10, "time", dateTimeValue(i), EMBEDDING_NAME, EMBEDDINGS.get(1)));
        }

        assertThat(indexMethods.queryTestIndex(genericInSetQuery("time", dateTimeValue(0))))
                .singleElement()
                .has(field("id", 10));

        assertThat(indexMethods.queryTestIndex(genericInSetQuery("time", lastDateTimeValue())))
                .isEmpty();

        assertThat(indexMethods.queryTestIndex(genericInSetQuery("time", dateTimeValue(0), dateTimeValue(1))))
                .hasSize(2)
                .extracting(extractor("id"))
                .containsExactlyInAnyOrder(10, 20);

        assertThat(indexMethods.queryTestIndex(
                        genericInSetQuery("time", dateTimeValue(0), dateTimeValue(1), dateTimeValue(4))))
                .hasSize(3)
                .extracting(extractor("id"))
                .containsExactlyInAnyOrder(10, 20, 50);

        assertThat(indexMethods.queryTestIndex(genericInSetQuery(
                        "time", dateTimeValue(0), lastDateTimeValue(), dateTimeValue(1), dateTimeValue(4))))
                .hasSize(3)
                .extracting(extractor("id"))
                .containsExactlyInAnyOrder(10, 20, 50);
    }

    private static final int ITERATIONS = 100;

    @Test
    void dateTimeValueInSetFuzzTestForNodes() throws Exception {
        temporalInSetFuzzTest(
                new NodeVectorIndexMethods(),
                VectorSSFTemporalTestHelper::generateTestZonedDateTimeStrings,
                DateTimeValue::parse);
    }

    @Test
    void timeValueInSetFuzzTestForNodes() throws Exception {
        temporalInSetFuzzTest(
                new NodeVectorIndexMethods(),
                VectorSSFTemporalTestHelper::generateTestZonedTimeStrings,
                TimeValue::parse);
    }

    @Test
    void dateTimeValueInSetFuzzTestForRelationships() throws Exception {
        temporalInSetFuzzTest(
                new RelationshipVectorIndexMethods(),
                VectorSSFTemporalTestHelper::generateTestZonedDateTimeStrings,
                DateTimeValue::parse);
    }

    @Test
    void timeValueInSetFuzzTestForRelationships() throws Exception {
        temporalInSetFuzzTest(
                new RelationshipVectorIndexMethods(),
                VectorSSFTemporalTestHelper::generateTestZonedTimeStrings,
                TimeValue::parse);
    }

    <TEMPORAL extends Temporal, TValue extends TemporalValue<TEMPORAL, TValue>> void temporalInSetFuzzTest(
            VectorIndexMethods indexMethods,
            Supplier<List<String>> generator,
            BiFunction<String, Supplier<ZoneId>, TValue> parser)
            throws Exception {

        indexMethods.createTestIndex(VECTOR_INDEX_NAME, EMBEDDINGS.dimensions(), EMBEDDING_NAME, "time");

        Map<Integer, TValue> map = new HashMap<>();
        Map<Integer, TValue> present = new HashMap<>();
        int id = 1;
        // single transaction to create all the nodes
        try (Transaction tx = db.beginTx()) {
            for (String timeString : generator.get()) {
                TValue dateTimeValue = parser.apply(timeString, ZoneId::systemDefault);
                if (random.intBetween(0, 100) < 80) {
                    // 80% of entries are added
                    int embedding = random.nextInt(1, EMBEDDINGS.count());
                    indexMethods.createTestEntity(
                            tx, Map.of("id", id, "time", dateTimeValue, EMBEDDING_NAME, EMBEDDINGS.get(embedding)));
                    present.put(id, dateTimeValue);
                }
                map.put(id, dateTimeValue);
                id++;
            }
            tx.commit();
        }
        int limit = id;

        for (int j = 0; j < ITERATIONS; j++) {
            int listSize = random.among(new int[] {2, 2, 2, 2, 3, 3, 3, 5, 5, 7, 10});
            List<Integer> list = new ArrayList<>(listSize);
            for (int k = 0; k < listSize; k++) {
                list.add(random.nextInt(1, limit));
            }
            Set<Integer> expected = new HashSet<>();
            for (int key : list) {
                if (present.containsKey(key)) {
                    expected.add(key);
                }
            }
            Value[] valuesToQuery = new Value[list.size()];
            for (int i = 0; i < list.size(); i++) {
                valuesToQuery[i] = map.get(list.get(i));
            }
            ResultList result = indexMethods.queryTestIndex(inSetQuery("time", valuesToQuery));
            assertThat(result)
                    .hasSize(expected.size())
                    .extracting(extractor("id"))
                    .containsExactlyInAnyOrderElementsOf(expected);
        }
    }

    private FloatArray randomVector(int dimensions) {
        float[] vector = new float[dimensions];
        for (int i = 0; i < vector.length; i++) {
            vector[i] = random.nextFloat();
        }
        // ensure not at exact origin (all zeros)
        int index = random.nextInt(dimensions);
        if (vector[index] == 0.0f) {
            vector[index] = (random.nextBoolean()) ? Math.nextUp(vector[index]) : Math.nextDown(vector[index]);
        }
        return Values.floatArray(vector);
    }

    @Test
    void mixedSetsWithSimpleTypesFuzzTests() throws Exception {
        NodeVectorIndexMethods indexMethods = new NodeVectorIndexMethods();
        int dimension = 128;
        int size = 10000;
        int iterations = 10;
        ValueType[] simpleTypes = {INT, LONG, FLOAT, DOUBLE, STRING};
        indexMethods.createTestIndex(VECTOR_INDEX_NAME, dimension, "embedding", "value");
        Value[] allValues = new Value[size];
        int embeddingToken, valueToken;
        try (InternalTransaction tx = db.beginTransaction()) {
            KernelTransaction ktx = tx.kernelTransaction();
            TokenWrite token = ktx.tokenWrite();
            int[] label = new int[] {token.labelGetOrCreateForName(LABEL_NODE_1.name())};
            embeddingToken = token.propertyKeyGetOrCreateForName("embedding");
            valueToken = token.propertyKeyGetOrCreateForName("value");
            for (int i = 0; i < size; i++) {
                Value value = random.randomValues().nextValueOfTypes(simpleTypes);
                allValues[i] = value;
                Write write = ktx.dataWrite();
                long node = write.nodeCreateWithLabels(label);
                write.nodeSetProperty(node, embeddingToken, randomVector(dimension));
                write.nodeSetProperty(node, valueToken, value);
            }
            tx.commit();
        }

        for (int i = 0; i < iterations; i++) {
            int n = random.intBetween(0, size);
            Value[] searchValues = random.selection(allValues, n, n, false);
            ResultList result = queryNodeIndex(
                    VECTOR_INDEX_NAME,
                    nearestNeighbors(Integer.MAX_VALUE, randomVector(dimension).asObject()),
                    PropertyIndexQuery.matchAllEntityFilter(),
                    inSetQuery("value", searchValues));
            assertThat(result.stream().map(v -> v.getValue("value")).toList())
                    .hasSameElementsAs(Arrays.asList(searchValues));
        }
    }

    @Test
    void mixedSetsWithComplexTypesFuzzTests() throws Exception {
        NodeVectorIndexMethods indexMethods = new NodeVectorIndexMethods();
        int dimension = 128;
        int size = 10000;
        int iterations = 10;
        indexMethods.createTestIndex(VECTOR_INDEX_NAME, dimension, "embedding", "value");
        Value[] allValues = new Value[size];
        ValueType[] allTypes = {
            LONG, DOUBLE, STRING, BOOLEAN, DURATION, TIME, LOCAL_TIME, DATE_TIME, LOCAL_DATE_TIME, DATE
        };
        int embeddingToken, valueToken;
        try (InternalTransaction tx = db.beginTransaction()) {
            KernelTransaction ktx = tx.kernelTransaction();
            Token token = ktx.token();
            int[] label = new int[] {token.labelGetOrCreateForName(LABEL_NODE_1.name())};
            embeddingToken = token.propertyKeyGetOrCreateForName("embedding");
            valueToken = token.propertyKeyGetOrCreateForName("value");
            for (int i = 0; i < size; i++) {
                Value value = random.randomValues().nextValueOfTypes(allTypes);
                allValues[i] = value;
                Write write = ktx.dataWrite();
                long node = write.nodeCreateWithLabels(label);
                write.nodeSetProperty(node, embeddingToken, randomVector(dimension));
                write.nodeSetProperty(node, valueToken, value);
            }
            tx.commit();
        }

        for (int i = 0; i < iterations; i++) {
            // for "complex types" we only guarantee handling up to 256 items, worst case
            // is when all items are duration which adds depth 4 level nesting, leading to
            // exceeding the total nesting depth of 1024 set by lucene.
            int n = random.intBetween(0, 256);
            Value[] searchValues = random.selection(allValues, n, n, false);
            ResultList result = queryNodeIndex(
                    VECTOR_INDEX_NAME,
                    nearestNeighbors(Integer.MAX_VALUE, randomVector(dimension).asObject()),
                    PropertyIndexQuery.matchAllEntityFilter(),
                    inSetQuery("value", searchValues));
            assertThat(result.stream().map(v -> v.getValue("value")).toList())
                    .hasSameElementsAs(Arrays.asList(searchValues));
        }
    }

    interface VectorIndexMethods {
        void createTestIndex(String name, int vectorDimension, String... onProperties);

        long createTestEntity(Map<String, Object> properties);

        long createTestEntity(Transaction tx, Map<String, Object> properties);

        ResultList queryTestIndex(Function<TokenRead, PropertyIndexQuery>... queryFilters) throws Exception;
    }

    private final class NodeVectorIndexMethods implements VectorIndexMethods {

        @Override
        public void createTestIndex(String name, int vectorDimension, String... onProperties) {
            createNodeVectorIndex(name, vectorDimension, onProperties);
        }

        @Override
        public long createTestEntity(Map<String, Object> properties) {
            return createTestNode(properties);
        }

        @Override
        public long createTestEntity(Transaction tx, Map<String, Object> properties) {
            return createTestNode(tx, properties);
        }

        @Override
        @SafeVarargs
        public final ResultList queryTestIndex(Function<TokenRead, PropertyIndexQuery>... queryFilters)
                throws Exception {
            return queryNodeIndex(queryFilters);
        }
    }

    private final class RelationshipVectorIndexMethods implements VectorIndexMethods {

        @Override
        public void createTestIndex(String name, int vectorDimension, String... onProperties) {
            createRelationshipVectorIndex(name, vectorDimension, onProperties);
        }

        @Override
        public long createTestEntity(Map<String, Object> properties) {
            return createTestRelationship(properties);
        }

        @Override
        public long createTestEntity(Transaction tx, Map<String, Object> properties) {
            return createTestRelationship(tx, properties);
        }

        @Override
        @SafeVarargs
        public final ResultList queryTestIndex(Function<TokenRead, PropertyIndexQuery>... queryFilters)
                throws Exception {
            return queryRelationshipIndex(queryFilters);
        }
    }
}
