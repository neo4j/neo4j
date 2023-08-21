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
package org.neo4j.kernel.impl.newapi.index;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.constrained;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unordered;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unorderedValues;
import static org.neo4j.values.storable.CoordinateReferenceSystem.CARTESIAN;
import static org.neo4j.values.storable.CoordinateReferenceSystem.CARTESIAN_3D;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS_84;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS_84_3D;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.stringValue;

import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.exceptions.KernelException;
import org.neo4j.gis.spatial.index.curves.SpaceFillingCurve;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.internal.kernel.api.Cursor;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.ValueIndexCursor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.index.schema.config.ConfiguredSpaceFillingCurveSettingsCache;
import org.neo4j.kernel.impl.index.schema.config.SpaceFillingCurveSettings;
import org.neo4j.kernel.impl.newapi.KernelAPIReadTestBase;
import org.neo4j.kernel.impl.newapi.ReadTestSupport;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public abstract class EntityValueIndexCursorTestBase<ENTITY_VALUE_INDEX_CURSOR extends Cursor & ValueIndexCursor>
        extends KernelAPIReadTestBase<ReadTestSupport> {
    private static final int TOTAL_ENTITY_COUNT = 37;
    private static final String COMPOSITE_INDEX_NAME = "compositeIndex";
    protected static final String PROP_INDEX_NAME = "nodeProp";
    private static final String PROP_2_INDEX_NAME = "nodeProp2";
    private static final String PROP_3_INDEX_NAME = "nodeProp3";
    private static final String WHAT_EVER_INDEX_NAME = "whatEver";
    public static final String DEFAULT_ENTITY_TOKEN = "Token";
    public static final String PROP_NAME = "prop";
    public static final String PROP_2_NAME = "prop2";
    public static final String PROP_3_NAME = "prop3";
    public static final String FIRSTNAME_PROP_NAME = "firstname";
    public static final String SURNAME_PROP_NAME = "surname";
    public static final String PERSON_TOKEN = "Person";
    public static final String WHAT_TOKEN = "What";
    public static final String EVER_PROP_NAME = "ever";
    protected static long strOne;
    protected static long strTwo1;
    protected static long strTwo2;
    protected static long strThree1;
    protected static long strThree2;
    protected static long strThree3;
    private static long boolTrue, num5, num6, num12a, num12b;
    private static long strOneNoLabel;
    private static long joeDalton, williamDalton, jackDalton, averellDalton;
    private static long date891, date892, date86;
    private static long[] entitiesOfAllPropertyTypes;
    private static long whateverPoint;
    private static Value whateverPointValue;

    private static final PointValue POINT_1 =
            PointValue.parse("{latitude: 40.7128, longitude: -74.0060, crs: 'wgs-84'}");
    private static final PointValue POINT_2 =
            PointValue.parse("{latitude: 40.7128, longitude: -74.006000001, crs: 'wgs-84'}");

    protected final EntityParams<ENTITY_VALUE_INDEX_CURSOR> entityParams = getEntityParams();
    protected final IndexParams indexParams = getIndexParams();

    protected abstract EntityParams<ENTITY_VALUE_INDEX_CURSOR> getEntityParams();

    protected abstract IndexType getIndexType();

    @Override
    public ReadTestSupport newTestSupport() {
        return new ReadTestSupport();
    }

    @Override
    public void createTestGraph(GraphDatabaseService graphDb) {
        try (Transaction tx = graphDb.beginTx()) {
            entityParams.createEntityIndex(tx, DEFAULT_ENTITY_TOKEN, PROP_NAME, PROP_INDEX_NAME, getIndexType());
            entityParams.createEntityIndex(tx, DEFAULT_ENTITY_TOKEN, PROP_2_NAME, PROP_2_INDEX_NAME, getIndexType());
            entityParams.createEntityIndex(tx, DEFAULT_ENTITY_TOKEN, PROP_3_NAME, PROP_3_INDEX_NAME, getIndexType());
            tx.commit();
        }
        try (Transaction tx = graphDb.beginTx()) {
            entityParams.createEntityIndex(tx, WHAT_TOKEN, EVER_PROP_NAME, WHAT_EVER_INDEX_NAME, getIndexType());
            tx.commit();
        }
        try (Transaction tx = graphDb.beginTx()) {
            entityParams.createCompositeEntityIndex(
                    tx, PERSON_TOKEN, FIRSTNAME_PROP_NAME, SURNAME_PROP_NAME, COMPOSITE_INDEX_NAME, getIndexType());
            tx.commit();
        } catch (Exception e) {
            throw new AssertionError(e);
        }
        try (Transaction tx = graphDb.beginTx()) {
            tx.schema().awaitIndexesOnline(5, MINUTES);
            tx.commit();
        }
        try (Transaction tx = graphDb.beginTx()) {
            strOne = entityWithProp(tx, "one");
            strTwo1 = entityWithProp(tx, "two");
            strTwo2 = entityWithProp(tx, "two");
            strThree1 = entityWithProp(tx, "three");
            strThree2 = entityWithProp(tx, "three");
            strThree3 = entityWithProp(tx, "three");
            entityWithProp(tx, false);
            boolTrue = entityWithProp(tx, true);
            entityWithProp(tx, 3); // Purposely mix ordering
            entityWithProp(tx, 3);
            entityWithProp(tx, 3);
            entityWithProp(tx, 2);
            entityWithProp(tx, 2);
            entityWithProp(tx, 1);
            entityWithProp(tx, 4);
            num5 = entityWithProp(tx, 5);
            num6 = entityWithProp(tx, 6);
            num12a = entityWithProp(tx, 12.0);
            num12b = entityWithProp(tx, 12.0);
            entityWithProp(tx, 18);
            entityWithProp(tx, 24);
            entityWithProp(tx, 30);
            entityWithProp(tx, 36);
            entityWithProp(tx, 42);

            if (entityParams.tokenlessEntitySupported()) {
                strOneNoLabel = entityWithNoLabel(tx, "one");
            } else {
                strOneNoLabel = -1;
            }

            joeDalton = person(tx, "Joe", "Dalton");
            williamDalton = person(tx, "William", "Dalton");
            jackDalton = person(tx, "Jack", "Dalton");
            averellDalton = person(tx, "Averell", "Dalton");
            entityWithProp(tx, Values.pointValue(CARTESIAN, 1, 0)); // Purposely mix order
            entityWithProp(tx, Values.pointValue(CARTESIAN, 0, 0));
            entityWithProp(tx, Values.pointValue(CARTESIAN, 0, 0));
            entityWithProp(tx, Values.pointValue(CARTESIAN, 0, 0));
            entityWithProp(tx, Values.pointValue(CARTESIAN, 0, 1));
            entityWithProp(tx, Values.pointValue(CARTESIAN_3D, 0, 0, 0));
            entityWithProp(tx, Values.pointValue(WGS_84, 0, 0));
            entityWithProp(tx, Values.pointValue(WGS_84_3D, 0, 0, 0));
            date891 = entityWithProp(tx, DateValue.date(1989, 3, 24)); // Purposely mix order
            date86 = entityWithProp(tx, DateValue.date(1986, 11, 18));
            date892 = entityWithProp(tx, DateValue.date(1989, 3, 24));
            entityWithProp(tx, new String[] {"first", "second", "third"});
            entityWithProp(tx, new String[] {"fourth", "fifth", "sixth", "seventh"});

            MutableLongList listOfIds = LongLists.mutable.empty();
            listOfIds.add(entityWithWhatever(tx, "string"));
            listOfIds.add(entityWithWhatever(tx, false));
            listOfIds.add(entityWithWhatever(tx, 3));
            listOfIds.add(entityWithWhatever(tx, 13.0));
            whateverPointValue = Values.pointValue(CARTESIAN, 1, 0);
            whateverPoint = entityWithWhatever(tx, whateverPointValue);
            listOfIds.add(whateverPoint);
            listOfIds.add(entityWithWhatever(tx, DateValue.date(1989, 3, 24)));
            listOfIds.add(entityWithWhatever(tx, new String[] {"first", "second", "third"}));

            entitiesOfAllPropertyTypes = listOfIds.toArray();

            assertSameDerivedValue(POINT_1, POINT_2);
            entityWithProp(tx, "prop3", POINT_1.asObjectCopy());
            entityWithProp(tx, "prop3", POINT_2.asObjectCopy());
            entityWithProp(tx, "prop3", POINT_2.asObjectCopy());

            tx.commit();
        }
    }

    protected abstract IndexParams getIndexParams();

    protected static void assertSameDerivedValue(PointValue p1, PointValue p2) {
        ConfiguredSpaceFillingCurveSettingsCache settingsFactory =
                new ConfiguredSpaceFillingCurveSettingsCache(Config.defaults());
        SpaceFillingCurveSettings spaceFillingCurveSettings = settingsFactory.forCRS(CoordinateReferenceSystem.WGS_84);
        SpaceFillingCurve curve = spaceFillingCurveSettings.curve();
        assertEquals(curve.derivedValueFor(p1.coordinate()), curve.derivedValueFor(p2.coordinate()));
    }

    @Test
    void shouldPerformExactLookup() throws Exception {
        // given
        int prop = token.propertyKey(PROP_NAME);
        IndexReadSession index = tx.dataRead().indexReadSession(tx.schemaRead().indexGetForName(PROP_INDEX_NAME));
        try (var cursor = entityParams.allocateEntityValueIndexCursor(tx, cursors)) {
            MutableLongSet uniqueIds = new LongHashSet();

            // when
            entityParams.entityIndexSeek(tx, index, cursor, unconstrained(), PropertyIndexQuery.exact(prop, "zero"));

            // then
            assertFoundEntitiesAndNoValue(cursor, uniqueIds);

            // when
            entityParams.entityIndexSeek(tx, index, cursor, unconstrained(), PropertyIndexQuery.exact(prop, "one"));

            // then
            assertFoundEntitiesAndNoValue(cursor, uniqueIds, strOne);

            // when
            entityParams.entityIndexSeek(tx, index, cursor, unconstrained(), PropertyIndexQuery.exact(prop, "two"));

            // then
            assertFoundEntitiesAndNoValue(cursor, uniqueIds, strTwo1, strTwo2);

            // when
            entityParams.entityIndexSeek(tx, index, cursor, unconstrained(), PropertyIndexQuery.exact(prop, "three"));

            // then
            assertFoundEntitiesAndNoValue(cursor, uniqueIds, strThree1, strThree2, strThree3);

            // when
            entityParams.entityIndexSeek(tx, index, cursor, unconstrained(), PropertyIndexQuery.exact(prop, 1));

            // then
            assertFoundEntitiesAndNoValue(cursor, 1, uniqueIds);

            // when
            entityParams.entityIndexSeek(tx, index, cursor, unconstrained(), PropertyIndexQuery.exact(prop, 2));

            // then
            assertFoundEntitiesAndNoValue(cursor, 2, uniqueIds);

            // when
            entityParams.entityIndexSeek(tx, index, cursor, unconstrained(), PropertyIndexQuery.exact(prop, 3));

            // then
            assertFoundEntitiesAndNoValue(cursor, 3, uniqueIds);

            // when
            entityParams.entityIndexSeek(tx, index, cursor, unconstrained(), PropertyIndexQuery.exact(prop, 6));

            // then
            assertFoundEntitiesAndNoValue(cursor, uniqueIds, num6);

            // when
            entityParams.entityIndexSeek(tx, index, cursor, unconstrained(), PropertyIndexQuery.exact(prop, 12.0));

            // then
            assertFoundEntitiesAndNoValue(cursor, uniqueIds, num12a, num12b);

            // when
            entityParams.entityIndexSeek(tx, index, cursor, unconstrained(), PropertyIndexQuery.exact(prop, true));

            // then
            assertFoundEntitiesAndNoValue(cursor, uniqueIds, boolTrue);

            // when
            entityParams.entityIndexSeek(
                    tx,
                    index,
                    cursor,
                    unconstrained(),
                    PropertyIndexQuery.exact(prop, Values.pointValue(CARTESIAN, 0, 0)));

            // then
            assertFoundEntitiesAndNoValue(cursor, 3, uniqueIds);

            // when
            entityParams.entityIndexSeek(
                    tx,
                    index,
                    cursor,
                    unconstrained(),
                    PropertyIndexQuery.exact(prop, Values.pointValue(CARTESIAN_3D, 0, 0, 0)));

            // then
            assertFoundEntitiesAndNoValue(cursor, 1, uniqueIds);

            // when
            entityParams.entityIndexSeek(
                    tx,
                    index,
                    cursor,
                    unconstrained(),
                    PropertyIndexQuery.exact(prop, Values.pointValue(WGS_84, 0, 0)));

            // then
            assertFoundEntitiesAndNoValue(cursor, 1, uniqueIds);

            // when
            entityParams.entityIndexSeek(
                    tx,
                    index,
                    cursor,
                    unconstrained(),
                    PropertyIndexQuery.exact(prop, Values.pointValue(WGS_84_3D, 0, 0, 0)));

            // then
            assertFoundEntitiesAndNoValue(cursor, 1, uniqueIds);

            // when
            entityParams.entityIndexSeek(
                    tx, index, cursor, unconstrained(), PropertyIndexQuery.exact(prop, DateValue.date(1989, 3, 24)));

            // then
            assertFoundEntitiesAndNoValue(cursor, 2, uniqueIds);

            // when
            entityParams.entityIndexSeek(
                    tx, index, cursor, unconstrained(), PropertyIndexQuery.exact(prop, DateValue.date(1986, 11, 18)));

            // then
            assertFoundEntitiesAndNoValue(cursor, 1, uniqueIds);
        }
    }

    @Test
    void shouldPerformExactLookupInCompositeIndex() throws Exception {
        // given
        int firstName = token.propertyKey(FIRSTNAME_PROP_NAME);
        int surname = token.propertyKey(SURNAME_PROP_NAME);
        IndexReadSession index = read.indexReadSession(schemaRead.indexGetForName(COMPOSITE_INDEX_NAME));
        try (var cursor = entityParams.allocateEntityValueIndexCursor(tx, cursors)) {
            MutableLongSet uniqueIds = new LongHashSet();

            // when
            entityParams.entityIndexSeek(
                    tx,
                    index,
                    cursor,
                    unconstrained(),
                    PropertyIndexQuery.exact(firstName, "Joe"),
                    PropertyIndexQuery.exact(surname, "Dalton"));

            // then
            assertThat(cursor.numberOfProperties()).isEqualTo(2);
            assertFoundEntitiesAndNoValue(cursor, 1, uniqueIds);
        }
    }

    @Test
    void shouldPerformStringPrefixSearch() throws Exception {
        // given
        boolean needsValues = indexParams.indexProvidesStringValues();
        int prop = token.propertyKey(PROP_NAME);
        IndexReadSession index = read.indexReadSession(schemaRead.indexGetForName(PROP_INDEX_NAME));
        try (var cursor = entityParams.allocateEntityValueIndexCursor(tx, cursors)) {
            MutableLongSet uniqueIds = new LongHashSet();

            // when
            entityParams.entityIndexSeek(
                    tx, index, cursor, unordered(needsValues), PropertyIndexQuery.stringPrefix(prop, stringValue("t")));

            // then
            assertThat(cursor.numberOfProperties()).isEqualTo(1);
            assertFoundEntitiesAndValue(
                    cursor,
                    uniqueIds,
                    index.reference().getCapability().supportsReturningValues(),
                    needsValues,
                    strTwo1,
                    strTwo2,
                    strThree1,
                    strThree2,
                    strThree3);
        }
    }

    @Test
    void shouldPerformStringSuffixSearch() throws Exception {
        assumeTrue(indexParams.indexSupportsStringSuffixAndContains());

        // given
        boolean needsValues = indexParams.indexProvidesStringValues();
        int prop = token.propertyKey(PROP_NAME);
        IndexReadSession index = read.indexReadSession(schemaRead.indexGetForName(PROP_INDEX_NAME));
        try (var cursor = entityParams.allocateEntityValueIndexCursor(tx, cursors)) {
            MutableLongSet uniqueIds = new LongHashSet();

            // when
            entityParams.entityIndexSeek(
                    tx, index, cursor, unordered(needsValues), PropertyIndexQuery.stringSuffix(prop, stringValue("e")));

            // then
            assertThat(cursor.numberOfProperties()).isEqualTo(1);
            assertFoundEntitiesAndValue(
                    cursor,
                    uniqueIds,
                    index.reference().getCapability().supportsReturningValues(),
                    needsValues,
                    strOne,
                    strThree1,
                    strThree2,
                    strThree3);
        }
    }

    @Test
    void shouldPerformStringContainmentSearch() throws Exception {
        assumeTrue(indexParams.indexSupportsStringSuffixAndContains());

        // given
        boolean needsValues = indexParams.indexProvidesStringValues();
        int prop = token.propertyKey(PROP_NAME);
        IndexReadSession index = read.indexReadSession(schemaRead.indexGetForName(PROP_INDEX_NAME));
        try (var cursor = entityParams.allocateEntityValueIndexCursor(tx, cursors)) {
            MutableLongSet uniqueIds = new LongHashSet();

            // when
            entityParams.entityIndexSeek(
                    tx,
                    index,
                    cursor,
                    unordered(needsValues),
                    PropertyIndexQuery.stringContains(prop, stringValue("o")));

            // then
            assertThat(cursor.numberOfProperties()).isEqualTo(1);
            assertFoundEntitiesAndValue(
                    cursor,
                    uniqueIds,
                    index.reference().getCapability().supportsReturningValues(),
                    needsValues,
                    strOne,
                    strTwo1,
                    strTwo2);
        }
    }

    @Test
    void shouldPerformStringRangeSearch() throws Exception {
        // given
        boolean needsValues = indexParams.indexProvidesStringValues();
        IndexQueryConstraints constraints = unordered(needsValues);
        int prop = token.propertyKey(PROP_NAME);
        IndexReadSession index = read.indexReadSession(schemaRead.indexGetForName(PROP_INDEX_NAME));
        boolean supportsValues = index.reference().getCapability().supportsReturningValues();
        try (var cursor = entityParams.allocateEntityValueIndexCursor(tx, cursors)) {
            MutableLongSet uniqueIds = new LongHashSet();

            // when
            entityParams.entityIndexSeek(
                    tx, index, cursor, constraints, PropertyIndexQuery.range(prop, "one", true, "three", true));

            // then

            assertFoundEntitiesAndValue(
                    cursor, uniqueIds, supportsValues, needsValues, strOne, strThree1, strThree2, strThree3);

            // when
            entityParams.entityIndexSeek(
                    tx, index, cursor, constraints, PropertyIndexQuery.range(prop, "one", true, "three", false));

            // then
            assertFoundEntitiesAndValue(cursor, uniqueIds, supportsValues, needsValues, strOne);

            // when
            entityParams.entityIndexSeek(
                    tx, index, cursor, constraints, PropertyIndexQuery.range(prop, "one", false, "three", true));

            // then
            assertFoundEntitiesAndValue(
                    cursor, uniqueIds, supportsValues, needsValues, strThree1, strThree2, strThree3);

            // when
            entityParams.entityIndexSeek(
                    tx, index, cursor, constraints, PropertyIndexQuery.range(prop, "one", false, "two", false));

            // then
            assertFoundEntitiesAndValue(
                    cursor, uniqueIds, supportsValues, needsValues, strThree1, strThree2, strThree3);

            // when
            entityParams.entityIndexSeek(
                    tx, index, cursor, constraints, PropertyIndexQuery.range(prop, "one", true, "two", true));

            // then
            assertFoundEntitiesAndValue(
                    cursor,
                    uniqueIds,
                    supportsValues,
                    needsValues,
                    strOne,
                    strThree1,
                    strThree2,
                    strThree3,
                    strTwo1,
                    strTwo2);
        }
    }

    @Test
    void shouldPerformNumericRangeSearch() throws Exception {
        // given
        boolean needsValues = indexParams.indexProvidesNumericValues();
        IndexQueryConstraints constraints = unordered(needsValues);
        int prop = token.propertyKey(PROP_NAME);
        IndexReadSession index = read.indexReadSession(schemaRead.indexGetForName(PROP_INDEX_NAME));
        boolean supportsValues = index.reference().getCapability().supportsReturningValues();
        try (var cursor = entityParams.allocateEntityValueIndexCursor(tx, cursors)) {
            MutableLongSet uniqueIds = new LongHashSet();

            // when
            entityParams.entityIndexSeek(
                    tx, index, cursor, constraints, PropertyIndexQuery.range(prop, 5, true, 12, true));

            // then
            assertFoundEntitiesAndValue(cursor, uniqueIds, supportsValues, needsValues, num5, num6, num12a, num12b);

            // when
            entityParams.entityIndexSeek(
                    tx, index, cursor, constraints, PropertyIndexQuery.range(prop, 5, true, 12, false));

            // then
            assertFoundEntitiesAndValue(cursor, uniqueIds, supportsValues, needsValues, num5, num6);

            // when
            entityParams.entityIndexSeek(
                    tx, index, cursor, constraints, PropertyIndexQuery.range(prop, 5, false, 12, true));

            // then
            assertFoundEntitiesAndValue(cursor, uniqueIds, supportsValues, needsValues, num6, num12a, num12b);

            // when
            entityParams.entityIndexSeek(
                    tx, index, cursor, constraints, PropertyIndexQuery.range(prop, 5, false, 12, false));

            // then
            assertFoundEntitiesAndValue(cursor, uniqueIds, supportsValues, needsValues, num6);
        }
    }

    @Test
    void shouldPerformTemporalRangeSearch() throws KernelException {
        // given
        boolean needsValues = indexParams.indexProvidesTemporalValues();
        IndexQueryConstraints constraints = unordered(needsValues);
        int prop = token.propertyKey(PROP_NAME);
        IndexReadSession index = read.indexReadSession(schemaRead.indexGetForName(PROP_INDEX_NAME));
        boolean supportsValues = index.reference().getCapability().supportsReturningValues();
        try (var cursor = entityParams.allocateEntityValueIndexCursor(tx, cursors)) {
            MutableLongSet uniqueIds = new LongHashSet();

            // when
            entityParams.entityIndexSeek(
                    tx,
                    index,
                    cursor,
                    constraints,
                    PropertyIndexQuery.range(
                            prop, DateValue.date(1986, 11, 18), true, DateValue.date(1989, 3, 24), true));

            // then
            assertFoundEntitiesAndValue(cursor, uniqueIds, supportsValues, needsValues, date86, date891, date892);

            // when
            entityParams.entityIndexSeek(
                    tx,
                    index,
                    cursor,
                    constraints,
                    PropertyIndexQuery.range(
                            prop, DateValue.date(1986, 11, 18), true, DateValue.date(1989, 3, 24), false));

            // then
            assertFoundEntitiesAndValue(cursor, uniqueIds, supportsValues, needsValues, date86);

            // when
            entityParams.entityIndexSeek(
                    tx,
                    index,
                    cursor,
                    constraints,
                    PropertyIndexQuery.range(
                            prop, DateValue.date(1986, 11, 18), false, DateValue.date(1989, 3, 24), true));

            // then
            assertFoundEntitiesAndValue(cursor, uniqueIds, supportsValues, needsValues, date891, date892);

            // when
            entityParams.entityIndexSeek(
                    tx,
                    index,
                    cursor,
                    constraints,
                    PropertyIndexQuery.range(
                            prop, DateValue.date(1986, 11, 18), false, DateValue.date(1989, 3, 24), false));

            // then
            assertFoundEntitiesAndValue(cursor, uniqueIds, supportsValues, needsValues);
        }
    }

    @Test
    void shouldPerformBooleanSearch() throws KernelException {
        // given
        boolean needsValues = indexParams.indexProvidesBooleanValues();
        IndexQueryConstraints constraints = unordered(needsValues);
        int prop = token.propertyKey(PROP_NAME);
        IndexReadSession index = read.indexReadSession(schemaRead.indexGetForName(PROP_INDEX_NAME));
        boolean supportsValues = index.reference().getCapability().supportsReturningValues();
        try (var cursor = entityParams.allocateEntityValueIndexCursor(tx, cursors)) {
            MutableLongSet uniqueIds = new LongHashSet();

            // when
            entityParams.entityIndexSeek(tx, index, cursor, constraints, PropertyIndexQuery.exact(prop, false));

            // then
            assertFoundEntitiesAndValue(cursor, 1, uniqueIds, supportsValues, needsValues);

            // when
            entityParams.entityIndexSeek(tx, index, cursor, constraints, PropertyIndexQuery.exact(prop, true));

            // then
            assertFoundEntitiesAndValue(cursor, 1, uniqueIds, supportsValues, needsValues);
        }
    }

    @Test
    void shouldPerformTextArraySearch() throws KernelException {
        // given
        boolean needsValues = indexParams.indexProvidesArrayValues();
        IndexQueryConstraints constraints = unordered(needsValues);
        int prop = token.propertyKey(PROP_NAME);
        IndexReadSession index = read.indexReadSession(schemaRead.indexGetForName(PROP_INDEX_NAME));
        boolean supportsValues = index.reference().getCapability().supportsReturningValues();
        try (var cursor = entityParams.allocateEntityValueIndexCursor(tx, cursors)) {
            MutableLongSet uniqueIds = new LongHashSet();

            // when
            entityParams.entityIndexSeek(tx, index, cursor, constraints, PropertyIndexQuery.exact(prop, new String[] {
                "first", "second", "third"
            }));

            // then
            assertFoundEntitiesAndValue(cursor, 1, uniqueIds, supportsValues, needsValues);

            // when
            entityParams.entityIndexSeek(tx, index, cursor, constraints, PropertyIndexQuery.exact(prop, new String[] {
                "fourth", "fifth", "sixth", "seventh"
            }));

            // then
            assertFoundEntitiesAndValue(cursor, 1, uniqueIds, supportsValues, needsValues);
        }
    }

    @Test
    void shouldPerformIndexScan() throws Exception {
        // given
        IndexReadSession index = read.indexReadSession(schemaRead.indexGetForName(PROP_INDEX_NAME));
        try (var cursor = entityParams.allocateEntityValueIndexCursor(tx, cursors)) {
            MutableLongSet uniqueIds = new LongHashSet();

            // when
            entityParams.entityIndexScan(tx, index, cursor, unordered(indexParams.indexProvidesAllValues()));

            // then
            assertThat(cursor.numberOfProperties()).isEqualTo(1);
            assertFoundEntitiesAndValue(
                    cursor,
                    TOTAL_ENTITY_COUNT,
                    uniqueIds,
                    index.reference().getCapability().supportsReturningValues(),
                    indexParams.indexProvidesAllValues());
        }
    }

    @Test
    void shouldRespectOrderCapabilitiesForNumbers() throws Exception {
        // given
        boolean needsValues = indexParams.indexProvidesNumericValues();
        int prop = token.propertyKey(PROP_NAME);
        IndexReadSession index = read.indexReadSession(schemaRead.indexGetForName(PROP_INDEX_NAME));

        try (var cursor = entityParams.allocateEntityValueIndexCursor(tx, cursors)) {
            if (index.reference().getCapability().supportsOrdering()) {
                // when
                entityParams.entityIndexSeek(
                        tx,
                        index,
                        cursor,
                        constrained(IndexOrder.ASCENDING, needsValues),
                        PropertyIndexQuery.range(prop, 1, true, 42, true));

                // then
                assertFoundEntitiesInOrder(cursor, IndexOrder.ASCENDING);

                // when
                entityParams.entityIndexSeek(
                        tx,
                        index,
                        cursor,
                        constrained(IndexOrder.DESCENDING, needsValues),
                        PropertyIndexQuery.range(prop, 1, true, 42, true));

                // then
                assertFoundEntitiesInOrder(cursor, IndexOrder.DESCENDING);
            }
        }
    }

    @Test
    void shouldRespectOrderCapabilitiesForStrings() throws Exception {
        // given
        boolean needsValues = indexParams.indexProvidesStringValues();
        int prop = token.propertyKey(PROP_NAME);
        IndexReadSession index = read.indexReadSession(schemaRead.indexGetForName(PROP_INDEX_NAME));

        try (var cursor = entityParams.allocateEntityValueIndexCursor(tx, cursors)) {
            if (index.reference().getCapability().supportsOrdering()) {
                // when
                entityParams.entityIndexSeek(
                        tx,
                        index,
                        cursor,
                        constrained(IndexOrder.ASCENDING, needsValues),
                        PropertyIndexQuery.range(prop, "one", true, "two", true));

                // then
                assertFoundEntitiesInOrder(cursor, IndexOrder.ASCENDING);

                // when
                entityParams.entityIndexSeek(
                        tx,
                        index,
                        cursor,
                        constrained(IndexOrder.DESCENDING, needsValues),
                        PropertyIndexQuery.range(prop, "one", true, "two", true));

                // then
                assertFoundEntitiesInOrder(cursor, IndexOrder.DESCENDING);
            }
        }
    }

    @Test
    void shouldRespectOrderCapabilitiesForTemporal() throws KernelException {
        // given
        boolean needsValues = indexParams.indexProvidesTemporalValues();
        int prop = token.propertyKey(PROP_NAME);
        IndexReadSession index = read.indexReadSession(schemaRead.indexGetForName(PROP_INDEX_NAME));

        try (var cursor = entityParams.allocateEntityValueIndexCursor(tx, cursors)) {
            if (index.reference().getCapability().supportsOrdering()) {
                // when
                entityParams.entityIndexSeek(
                        tx,
                        index,
                        cursor,
                        constrained(IndexOrder.ASCENDING, needsValues),
                        PropertyIndexQuery.range(
                                prop, DateValue.date(1986, 11, 18), true, DateValue.date(1989, 3, 24), true));

                // then
                assertFoundEntitiesInOrder(cursor, IndexOrder.ASCENDING);

                // when
                entityParams.entityIndexSeek(
                        tx,
                        index,
                        cursor,
                        constrained(IndexOrder.DESCENDING, needsValues),
                        PropertyIndexQuery.range(
                                prop, DateValue.date(1986, 11, 18), true, DateValue.date(1989, 3, 24), true));

                // then
                assertFoundEntitiesInOrder(cursor, IndexOrder.DESCENDING);
            }
        }
    }

    @Test
    void shouldRespectOrderCapabilitiesForStringArray() throws KernelException {
        // given
        boolean needsValues = indexParams.indexProvidesSpatialValues();
        int prop = token.propertyKey(PROP_NAME);
        IndexReadSession index = read.indexReadSession(schemaRead.indexGetForName(PROP_INDEX_NAME));

        try (var cursor = entityParams.allocateEntityValueIndexCursor(tx, cursors)) {
            if (index.reference().getCapability().supportsOrdering()) {
                // when
                entityParams.entityIndexSeek(
                        tx,
                        index,
                        cursor,
                        constrained(IndexOrder.ASCENDING, needsValues),
                        PropertyIndexQuery.range(
                                prop,
                                Values.of(new String[] {"first", "second", "third"}),
                                true,
                                Values.of(new String[] {"fourth", "fifth", "sixth", "seventh"}),
                                true));

                // then
                assertFoundEntitiesInOrder(cursor, IndexOrder.ASCENDING);

                // when
                entityParams.entityIndexSeek(
                        tx,
                        index,
                        cursor,
                        constrained(IndexOrder.DESCENDING, needsValues),
                        PropertyIndexQuery.range(
                                prop,
                                Values.of(new String[] {"first", "second", "third"}),
                                true,
                                Values.of(new String[] {"fourth", "fifth", "sixth", "seventh"}),
                                true));

                // then
                assertFoundEntitiesInOrder(cursor, IndexOrder.DESCENDING);
            }
        }
    }

    @Test
    void shouldRespectOrderCapabilitiesForWildcard() throws Exception {
        // given
        boolean needsValues = false;
        int prop = token.propertyKey(PROP_NAME);
        IndexReadSession index = read.indexReadSession(schemaRead.indexGetForName(PROP_INDEX_NAME));

        try (var cursor = entityParams.allocateEntityValueIndexCursor(tx, cursors)) {
            if (index.reference().getCapability().supportsOrdering()) {
                // when
                entityParams.entityIndexSeek(
                        tx,
                        index,
                        cursor,
                        constrained(IndexOrder.ASCENDING, needsValues),
                        PropertyIndexQuery.exists(prop));

                // then
                assertFoundEntitiesInOrder(cursor, IndexOrder.ASCENDING);

                // when
                entityParams.entityIndexSeek(
                        tx,
                        index,
                        cursor,
                        constrained(IndexOrder.DESCENDING, needsValues),
                        PropertyIndexQuery.exists(prop));

                // then
                assertFoundEntitiesInOrder(cursor, IndexOrder.DESCENDING);
            }
        }
    }

    @Test
    void shouldProvideValuesForPoints() throws Exception {
        // given
        assumeTrue(indexParams.indexProvidesSpatialValues());

        int prop = token.propertyKey(EVER_PROP_NAME);
        IndexReadSession index = read.indexReadSession(schemaRead.indexGetForName(WHAT_EVER_INDEX_NAME));
        assertTrue(index.reference().getCapability().supportsReturningValues());

        try (var cursor = entityParams.allocateEntityValueIndexCursor(tx, cursors)) {
            MutableLongSet uniqueIds = new LongHashSet();

            // when
            entityParams.entityIndexSeek(
                    tx, index, cursor, unorderedValues(), PropertyIndexQuery.exact(prop, whateverPointValue));

            // then
            assertFoundEntitiesAndValue(
                    cursor,
                    uniqueIds,
                    index.reference().getCapability().supportsReturningValues(),
                    true,
                    whateverPoint);
        }
    }

    @Test
    void shouldProvideValuesForAllTypes() throws Exception {
        // given
        assumeTrue(indexParams.indexProvidesAllValues());

        IndexReadSession index = read.indexReadSession(schemaRead.indexGetForName(WHAT_EVER_INDEX_NAME));
        boolean supportsValues = index.reference().getCapability().supportsReturningValues();
        assertTrue(supportsValues);

        try (var cursor = entityParams.allocateEntityValueIndexCursor(tx, cursors)) {
            MutableLongSet uniqueIds = new LongHashSet();

            // when
            entityParams.entityIndexSeek(tx, index, cursor, unorderedValues(), PropertyIndexQuery.allEntries());

            // then
            assertFoundEntitiesAndValue(cursor, uniqueIds, supportsValues, true, entitiesOfAllPropertyTypes);
        }
    }

    @Test
    void shouldProvideValuesForAllTypesOnPropKey() throws Exception {
        // given
        assumeTrue(indexParams.indexProvidesAllValues());

        int prop = token.propertyKey(EVER_PROP_NAME);
        IndexReadSession index = read.indexReadSession(schemaRead.indexGetForName(WHAT_EVER_INDEX_NAME));
        boolean supportsValues = index.reference().getCapability().supportsReturningValues();
        assertTrue(supportsValues);

        try (var cursor = entityParams.allocateEntityValueIndexCursor(tx, cursors)) {
            MutableLongSet uniqueIds = new LongHashSet();

            // when
            entityParams.entityIndexSeek(tx, index, cursor, unorderedValues(), PropertyIndexQuery.exists(prop));

            // then
            assertFoundEntitiesAndValue(cursor, uniqueIds, supportsValues, true, entitiesOfAllPropertyTypes);
        }
    }

    private void assertFoundEntitiesInOrder(ENTITY_VALUE_INDEX_CURSOR cursor, IndexOrder indexOrder) {
        Value currentValue = null;
        while (cursor.next()) {
            long reference = entityParams.entityReference(cursor);
            Value storedValue = entityParams.getPropertyValueFromStore(tx, cursors, reference);
            // NOTE: Points are not stored in the natural order in the index.
            //      Sort order for points is explicitly handled by the client, see DefaultNodeValueIndexCursor
            if (storedValue instanceof PointValue) {
                continue;
            }
            if (currentValue != null) {
                switch (indexOrder) {
                    case ASCENDING:
                        assertTrue(
                                Values.COMPARATOR.compare(currentValue, storedValue) <= 0,
                                "Requested ordering " + indexOrder + " was not respected.");
                        break;
                    case DESCENDING:
                        assertTrue(
                                Values.COMPARATOR.compare(currentValue, storedValue) >= 0,
                                "Requested ordering " + indexOrder + " was not respected.");
                        break;
                    case NONE:
                        // Don't verify
                        break;
                    default:
                        throw new UnsupportedOperationException("Can not verify ordering for " + indexOrder);
                }
            }
            currentValue = storedValue;
        }
    }

    private void assertFoundEntitiesAndValue(
            ENTITY_VALUE_INDEX_CURSOR cursor,
            int expectedCount,
            MutableLongSet uniqueIds,
            boolean expectValue,
            boolean indexProvidesValues) {
        uniqueIds.clear();
        for (int i = 0; i < expectedCount; i++) {
            assertTrue(cursor.next(), "at least " + expectedCount + " entities, was " + uniqueIds.size());
            long reference = entityParams.entityReference(cursor);
            assertTrue(uniqueIds.add(reference), "all entities are unique");

            // Assert has value capability
            if (expectValue) {
                assertTrue(
                        cursor.hasValue(),
                        "Value capability said index would have value for " + expectValue + ", but didn't");
            }

            // Assert has correct value
            if (indexProvidesValues) {
                assertTrue(cursor.hasValue(), "Index did not provide values");
                Value storedValue = entityParams.getPropertyValueFromStore(tx, cursors, reference);
                assertThat(cursor.propertyValue(0)).as("has correct value").isEqualTo(storedValue);
            }
        }

        assertFalse(cursor.next(), "no more than " + expectedCount + " entities");
    }

    private void assertFoundEntitiesAndNoValue(
            ENTITY_VALUE_INDEX_CURSOR cursor, int expectedCount, MutableLongSet uniqueIds) {
        uniqueIds.clear();
        for (int i = 0; i < expectedCount; i++) {
            assertTrue(cursor.next(), "at least " + expectedCount + " entities, was " + uniqueIds.size());
            long reference = entityParams.entityReference(cursor);
            assertTrue(uniqueIds.add(reference), "all entities are unique");

            // We can't quite assert !node.hasValue() because even tho pure SpatialIndexReader is guaranteed to not
            // return any values,
            // where null could be used, the generic native index, especially when having composite keys including
            // spatial values it's
            // more of a gray area and some keys may be spatial, some not and therefore a proper Value[] will be
            // extracted
            // potentially containing some NO_VALUE values.
        }

        assertFalse(cursor.next(), "no more than " + expectedCount + " entities");
    }

    protected void assertFoundEntitiesAndValue(
            ENTITY_VALUE_INDEX_CURSOR cursor,
            MutableLongSet uniqueIds,
            boolean expectValue,
            boolean indexProvidesValues,
            long... expected) {
        assertFoundEntitiesAndValue(cursor, expected.length, uniqueIds, expectValue, indexProvidesValues);

        for (long expectedEntity : expected) {
            assertTrue(uniqueIds.contains(expectedEntity), "expected entity " + expectedEntity);
        }
    }

    private void assertFoundEntitiesAndNoValue(
            ENTITY_VALUE_INDEX_CURSOR cursor, MutableLongSet uniqueIds, long... expected) {
        assertFoundEntitiesAndNoValue(cursor, expected.length, uniqueIds);

        for (long expectedEntity : expected) {
            assertTrue(uniqueIds.contains(expectedEntity), "expected entity " + expectedEntity);
        }
    }

    @Test
    void shouldGetNoIndexForMissingTokens() {
        int tokenId = entityParams.entityTokenId(tx, DEFAULT_ENTITY_TOKEN);
        int prop = token.propertyKey(PROP_NAME);
        int badTokenId = tokenId + 1000;
        int badProp = prop + 1000;

        assertFalse(
                schemaRead
                        .index(entityParams.schemaDescriptor(badTokenId, prop))
                        .hasNext(),
                "bad tokenId");
        assertFalse(
                schemaRead
                        .index(entityParams.schemaDescriptor(tokenId, badProp))
                        .hasNext(),
                "bad prop");
        assertFalse(
                schemaRead
                        .index(entityParams.schemaDescriptor(badTokenId, badProp))
                        .hasNext(),
                "just bad");
    }

    @Test
    void shouldGetNoIndexForUnknownTokens() {
        int label = entityParams.entityTokenId(tx, DEFAULT_ENTITY_TOKEN);
        int prop = token.propertyKey(PROP_NAME);
        int badLabel = Integer.MAX_VALUE;
        int badProp = Integer.MAX_VALUE;

        assertFalse(
                schemaRead.index(entityParams.schemaDescriptor(badLabel, prop)).hasNext(), "bad label");
        assertFalse(
                schemaRead.index(entityParams.schemaDescriptor(label, badProp)).hasNext(), "bad prop");
        assertFalse(
                schemaRead
                        .index(entityParams.schemaDescriptor(badLabel, badProp))
                        .hasNext(),
                "just bad");
    }

    @Test
    void shouldGetVersionAndKeyFromIndexReference() {
        // Given
        IndexDescriptor index = schemaRead.indexGetForName(PROP_INDEX_NAME);

        assertEquals(indexParams.providerKey(), index.getIndexProvider().getKey());
        assertEquals(indexParams.providerVersion(), index.getIndexProvider().getVersion());
    }

    @Test
    void shouldNotFindDeletedEntityInIndexScan() throws Exception {
        // Given
        boolean needsValues = indexParams.indexProvidesAllValues();
        IndexReadSession index = read.indexReadSession(schemaRead.indexGetForName(PROP_INDEX_NAME));
        boolean supportsValues = index.reference().getCapability().supportsReturningValues();
        try (KernelTransaction tx = beginTransaction();
                var cursor = entityParams.allocateEntityValueIndexCursor(tx, cursors)) {
            MutableLongSet uniqueIds = new LongHashSet();

            // when
            entityParams.entityIndexScan(tx, index, cursor, unordered(needsValues));
            assertThat(cursor.numberOfProperties()).isEqualTo(1);
            assertFoundEntitiesAndValue(cursor, TOTAL_ENTITY_COUNT, uniqueIds, supportsValues, needsValues);

            // then
            entityParams.entityDelete(tx, strOne);
            entityParams.entityIndexScan(tx, index, cursor, unordered(needsValues));
            assertFoundEntitiesAndValue(cursor, TOTAL_ENTITY_COUNT - 1, uniqueIds, supportsValues, needsValues);
        }
    }

    @Test
    void shouldNotFindDeletedEntityInIndexSeek() throws Exception {
        // Given
        boolean needsValues = false;
        int prop = token.propertyKey(PROP_NAME);
        IndexReadSession index = read.indexReadSession(schemaRead.indexGetForName(PROP_INDEX_NAME));
        try (KernelTransaction tx = beginTransaction();
                var cursor = entityParams.allocateEntityValueIndexCursor(tx, cursors)) {
            // when
            entityParams.entityDelete(tx, strOne);
            entityParams.entityIndexSeek(
                    tx, index, cursor, unordered(needsValues), PropertyIndexQuery.exact(prop, "one"));

            // then
            assertFalse(cursor.next());
        }
    }

    @Test
    void shouldNotFindEntityWithRemovedTokenInIndexSeek() throws Exception {
        assumeTrue(entityParams.tokenlessEntitySupported());

        // Given
        boolean needsValues = false;
        int label = entityParams.entityTokenId(tx, DEFAULT_ENTITY_TOKEN);
        int prop = token.propertyKey(PROP_NAME);
        IndexReadSession index = read.indexReadSession(schemaRead.indexGetForName(PROP_INDEX_NAME));
        try (KernelTransaction tx = beginTransaction();
                var cursor = entityParams.allocateEntityValueIndexCursor(tx, cursors)) {
            // when
            entityParams.entityRemoveToken(tx, strOne, label);
            entityParams.entityIndexSeek(
                    tx, index, cursor, unordered(needsValues), PropertyIndexQuery.exact(prop, "one"));

            // then
            assertFalse(cursor.next());
        }
    }

    @Test
    void shouldNotFindUpdatedEntityInIndexSeek() throws Exception {
        // Given
        boolean needsValues = false;
        int prop = token.propertyKey(PROP_NAME);
        IndexReadSession index = read.indexReadSession(schemaRead.indexGetForName(PROP_INDEX_NAME));
        try (KernelTransaction tx = beginTransaction();
                var cursor = entityParams.allocateEntityValueIndexCursor(tx, cursors)) {
            // when
            entityParams.entitySetProperty(tx, strOne, prop, "ett");
            entityParams.entityIndexSeek(
                    tx, index, cursor, unordered(needsValues), PropertyIndexQuery.exact(prop, "one"));

            // then
            assertFalse(cursor.next());
        }
    }

    @Test
    void shouldFindUpdatedEntityInIndexSeek() throws Exception {
        // Given
        boolean needsValues = false;
        int prop = token.propertyKey(PROP_NAME);
        IndexReadSession index = read.indexReadSession(schemaRead.indexGetForName(PROP_INDEX_NAME));
        try (KernelTransaction tx = beginTransaction();
                var cursor = entityParams.allocateEntityValueIndexCursor(tx, cursors)) {
            // when
            entityParams.entitySetProperty(tx, strOne, prop, "ett");
            entityParams.entityIndexSeek(
                    tx, index, cursor, unordered(needsValues), PropertyIndexQuery.exact(prop, "ett"));

            // then
            assertTrue(cursor.next());
            assertEquals(strOne, entityParams.entityReference(cursor));
        }
    }

    @Test
    void shouldFindSwappedEntityInIndexSeek() throws Exception {
        assumeTrue(entityParams.tokenlessEntitySupported());

        // Given
        boolean needsValues = false;
        int label = entityParams.entityTokenId(tx, DEFAULT_ENTITY_TOKEN);
        int prop = token.propertyKey(PROP_NAME);
        IndexReadSession index = read.indexReadSession(schemaRead.indexGetForName(PROP_INDEX_NAME));
        try (KernelTransaction tx = beginTransaction();
                var cursor = entityParams.allocateEntityValueIndexCursor(tx, cursors)) {
            // when
            entityParams.entityRemoveToken(tx, strOne, label);
            entityParams.entityAddToken(tx, strOneNoLabel, label);
            entityParams.entityIndexSeek(
                    tx, index, cursor, unordered(needsValues), PropertyIndexQuery.exact(prop, "one"));

            // then
            assertTrue(cursor.next());
            assertEquals(strOneNoLabel, entityParams.entityReference(cursor));
        }
    }

    @Test
    void shouldNotFindDeletedEntityInRangeSearch() throws Exception {
        // Given
        boolean needsValues = indexParams.indexProvidesStringValues();
        int prop = token.propertyKey(PROP_NAME);
        IndexReadSession index = read.indexReadSession(schemaRead.indexGetForName(PROP_INDEX_NAME));
        try (KernelTransaction tx = beginTransaction();
                var cursor = entityParams.allocateEntityValueIndexCursor(tx, cursors)) {
            // when
            entityParams.entityDelete(tx, strOne);
            entityParams.entityDelete(tx, strThree1);
            entityParams.entityDelete(tx, strThree2);
            entityParams.entityDelete(tx, strThree3);
            entityParams.entityIndexSeek(
                    tx,
                    index,
                    cursor,
                    unordered(needsValues),
                    PropertyIndexQuery.range(prop, "one", true, "three", true));

            // then
            assertFalse(cursor.next());
        }
    }

    @Test
    void shouldNotFindEntityWithRemovedLabelInRangeSearch() throws Exception {
        assumeTrue(entityParams.tokenlessEntitySupported());
        // Given
        boolean needsValues = indexParams.indexProvidesStringValues();
        int label = entityParams.entityTokenId(tx, DEFAULT_ENTITY_TOKEN);
        int prop = token.propertyKey(PROP_NAME);
        IndexReadSession index = read.indexReadSession(schemaRead.indexGetForName(PROP_INDEX_NAME));
        try (KernelTransaction tx = beginTransaction();
                var cursor = entityParams.allocateEntityValueIndexCursor(tx, cursors)) {
            // when
            entityParams.entityRemoveToken(tx, strOne, label);
            entityParams.entityRemoveToken(tx, strThree1, label);
            entityParams.entityRemoveToken(tx, strThree2, label);
            entityParams.entityRemoveToken(tx, strThree3, label);
            entityParams.entityIndexSeek(
                    tx,
                    index,
                    cursor,
                    unordered(needsValues),
                    PropertyIndexQuery.range(prop, "one", true, "three", true));

            // then
            assertFalse(cursor.next());
        }
    }

    @Test
    void shouldNotFindUpdatedEntityInRangeSearch() throws Exception {
        // Given
        boolean needsValues = indexParams.indexProvidesStringValues();
        int prop = token.propertyKey(PROP_NAME);
        IndexReadSession index = read.indexReadSession(schemaRead.indexGetForName(PROP_INDEX_NAME));
        try (KernelTransaction tx = beginTransaction();
                var cursor = entityParams.allocateEntityValueIndexCursor(tx, cursors)) {
            // when
            entityParams.entitySetProperty(tx, strOne, prop, "ett");
            entityParams.entitySetProperty(tx, strThree1, prop, "tre");
            entityParams.entitySetProperty(tx, strThree2, prop, "tre");
            entityParams.entitySetProperty(tx, strThree3, prop, "tre");
            entityParams.entityIndexSeek(
                    tx,
                    index,
                    cursor,
                    unordered(needsValues),
                    PropertyIndexQuery.range(prop, "one", true, "three", true));

            // then
            assertFalse(cursor.next());
        }
    }

    @Test
    void shouldFindUpdatedEntityInRangeSearch() throws Exception {
        // Given
        boolean needsValues = indexParams.indexProvidesStringValues();
        int prop = token.propertyKey(PROP_NAME);
        IndexReadSession index = read.indexReadSession(schemaRead.indexGetForName(PROP_INDEX_NAME));
        try (KernelTransaction tx = beginTransaction();
                var cursor = entityParams.allocateEntityValueIndexCursor(tx, cursors)) {
            // when
            entityParams.entitySetProperty(tx, strOne, prop, "ett");
            entityParams.entityIndexSeek(
                    tx,
                    index,
                    cursor,
                    unordered(needsValues),
                    PropertyIndexQuery.range(prop, "ett", true, "tre", true));

            // then
            assertTrue(cursor.next());
            assertEquals(strOne, entityParams.entityReference(cursor));
        }
    }

    @Test
    void shouldFindSwappedEntityInRangeSearch() throws Exception {
        assumeTrue(entityParams.tokenlessEntitySupported());
        // Given
        boolean needsValues = indexParams.indexProvidesStringValues();
        int label = entityParams.entityTokenId(tx, DEFAULT_ENTITY_TOKEN);
        int prop = token.propertyKey(PROP_NAME);
        IndexReadSession index = read.indexReadSession(schemaRead.indexGetForName(PROP_INDEX_NAME));
        try (KernelTransaction tx = beginTransaction();
                var cursor = entityParams.allocateEntityValueIndexCursor(tx, cursors)) {
            // when
            entityParams.entityRemoveToken(tx, strOne, label);
            entityParams.entityAddToken(tx, strOneNoLabel, label);
            entityParams.entityIndexSeek(
                    tx,
                    index,
                    cursor,
                    unordered(needsValues),
                    PropertyIndexQuery.range(prop, "one", true, "ones", true));

            // then
            assertTrue(cursor.next());
            assertEquals(strOneNoLabel, entityParams.entityReference(cursor));
            assertFalse(cursor.next());
        }
    }

    @Test
    void shouldNotFindDeletedEntityInPrefixSearch() throws Exception {
        // Given
        boolean needsValues = indexParams.indexProvidesStringValues();
        int prop = token.propertyKey(PROP_NAME);
        IndexReadSession index = read.indexReadSession(schemaRead.indexGetForName(PROP_INDEX_NAME));
        try (KernelTransaction tx = beginTransaction();
                var cursor = entityParams.allocateEntityValueIndexCursor(tx, cursors)) {
            // when
            entityParams.entityDelete(tx, strOne);
            entityParams.entityIndexSeek(
                    tx,
                    index,
                    cursor,
                    unordered(needsValues),
                    PropertyIndexQuery.stringPrefix(prop, stringValue("on")));

            // then
            assertFalse(cursor.next());
        }
    }

    @Test
    void shouldNotFindEntityWithRemovedLabelInPrefixSearch() throws Exception {
        assumeTrue(entityParams.tokenlessEntitySupported());
        // Given
        boolean needsValues = indexParams.indexProvidesStringValues();
        int label = entityParams.entityTokenId(tx, DEFAULT_ENTITY_TOKEN);
        int prop = token.propertyKey(PROP_NAME);
        IndexReadSession index = read.indexReadSession(schemaRead.indexGetForName(PROP_INDEX_NAME));
        try (KernelTransaction tx = beginTransaction();
                var cursor = entityParams.allocateEntityValueIndexCursor(tx, cursors)) {
            // when
            entityParams.entityRemoveToken(tx, strOne, label);
            entityParams.entityIndexSeek(
                    tx,
                    index,
                    cursor,
                    unordered(needsValues),
                    PropertyIndexQuery.stringPrefix(prop, stringValue("on")));

            // then
            assertFalse(cursor.next());
        }
    }

    @Test
    void shouldNotFindUpdatedEntityInPrefixSearch() throws Exception {
        // Given
        boolean needsValues = indexParams.indexProvidesStringValues();
        int prop = token.propertyKey(PROP_NAME);
        IndexReadSession index = read.indexReadSession(schemaRead.indexGetForName(PROP_INDEX_NAME));
        try (KernelTransaction tx = beginTransaction();
                var cursor = entityParams.allocateEntityValueIndexCursor(tx, cursors)) {
            // when
            entityParams.entitySetProperty(tx, strOne, prop, "ett");
            entityParams.entityIndexSeek(
                    tx,
                    index,
                    cursor,
                    unordered(needsValues),
                    PropertyIndexQuery.stringPrefix(prop, stringValue("on")));

            // then
            assertFalse(cursor.next());
        }
    }

    @Test
    void shouldFindUpdatedEntityInPrefixSearch() throws Exception {
        // Given
        boolean needsValues = indexParams.indexProvidesStringValues();
        int prop = token.propertyKey(PROP_NAME);
        IndexReadSession index = read.indexReadSession(schemaRead.indexGetForName(PROP_INDEX_NAME));
        try (KernelTransaction tx = beginTransaction();
                var cursor = entityParams.allocateEntityValueIndexCursor(tx, cursors)) {
            // when
            entityParams.entitySetProperty(tx, strOne, prop, "ett");
            entityParams.entityIndexSeek(
                    tx,
                    index,
                    cursor,
                    unordered(needsValues),
                    PropertyIndexQuery.stringPrefix(prop, stringValue("et")));

            // then
            assertTrue(cursor.next());
            assertEquals(strOne, entityParams.entityReference(cursor));
        }
    }

    @Test
    void shouldFindSwappedEntityInPrefixSearch() throws Exception {
        assumeTrue(entityParams.tokenlessEntitySupported());
        // Given
        boolean needsValues = indexParams.indexProvidesStringValues();
        int label = entityParams.entityTokenId(tx, DEFAULT_ENTITY_TOKEN);
        int prop = token.propertyKey(PROP_NAME);
        IndexReadSession index = read.indexReadSession(schemaRead.indexGetForName(PROP_INDEX_NAME));
        try (KernelTransaction tx = beginTransaction();
                var cursor = entityParams.allocateEntityValueIndexCursor(tx, cursors)) {
            // when
            entityParams.entityRemoveToken(tx, strOne, label);
            entityParams.entityAddToken(tx, strOneNoLabel, label);
            entityParams.entityIndexSeek(
                    tx,
                    index,
                    cursor,
                    unordered(needsValues),
                    PropertyIndexQuery.stringPrefix(prop, stringValue("on")));

            // then
            assertTrue(cursor.next());
            assertEquals(strOneNoLabel, entityParams.entityReference(cursor));
        }
    }

    @Test
    void shouldNotFindDeletedEntityInCompositeIndex() throws Exception {
        // Given
        boolean needsValues = false;
        int firstName = token.propertyKey(FIRSTNAME_PROP_NAME);
        int surname = token.propertyKey(SURNAME_PROP_NAME);
        IndexReadSession index = read.indexReadSession(schemaRead.indexGetForName(COMPOSITE_INDEX_NAME));
        try (KernelTransaction tx = beginTransaction();
                var cursor = entityParams.allocateEntityValueIndexCursor(tx, cursors)) {
            // when
            entityParams.entityDelete(tx, jackDalton);
            entityParams.entityIndexSeek(
                    tx,
                    index,
                    cursor,
                    unordered(needsValues),
                    PropertyIndexQuery.exact(firstName, "Jack"),
                    PropertyIndexQuery.exact(surname, "Dalton"));

            // then
            assertFalse(cursor.next());
        }
    }

    @Test
    void shouldNotFindEntityWithRemovedLabelInCompositeIndex() throws Exception {
        assumeTrue(entityParams.tokenlessEntitySupported());
        // Given
        boolean needsValues = false;
        int label = entityParams.entityTokenId(tx, PERSON_TOKEN);
        int firstName = token.propertyKey(FIRSTNAME_PROP_NAME);
        int surname = token.propertyKey(SURNAME_PROP_NAME);
        IndexReadSession index = read.indexReadSession(schemaRead.indexGetForName(COMPOSITE_INDEX_NAME));
        try (KernelTransaction tx = beginTransaction();
                var cursor = entityParams.allocateEntityValueIndexCursor(tx, cursors)) {
            // when
            entityParams.entityRemoveToken(tx, joeDalton, label);
            entityParams.entityIndexSeek(
                    tx,
                    index,
                    cursor,
                    unordered(needsValues),
                    PropertyIndexQuery.exact(firstName, "Joe"),
                    PropertyIndexQuery.exact(surname, "Dalton"));
            // then
            assertFalse(cursor.next());
        }
    }

    @Test
    void shouldNotFindUpdatedEntityInCompositeIndex() throws Exception {
        // Given
        boolean needsValues = false;
        int firstName = token.propertyKey(FIRSTNAME_PROP_NAME);
        int surname = token.propertyKey(SURNAME_PROP_NAME);
        IndexReadSession index = read.indexReadSession(schemaRead.indexGetForName(COMPOSITE_INDEX_NAME));
        try (KernelTransaction tx = beginTransaction();
                var cursor = entityParams.allocateEntityValueIndexCursor(tx, cursors)) {
            // when
            entityParams.entitySetProperty(tx, jackDalton, firstName, "Jesse");
            entityParams.entitySetProperty(tx, jackDalton, surname, "James");
            entityParams.entityIndexSeek(
                    tx,
                    index,
                    cursor,
                    unordered(needsValues),
                    PropertyIndexQuery.exact(firstName, "Jack"),
                    PropertyIndexQuery.exact(surname, "Dalton"));

            // then
            assertFalse(cursor.next());
        }
    }

    @Test
    void shouldFindUpdatedEntityInCompositeIndex() throws Exception {
        // Given
        boolean needsValues = false;
        int firstName = token.propertyKey(FIRSTNAME_PROP_NAME);
        int surname = token.propertyKey(SURNAME_PROP_NAME);
        IndexReadSession index = read.indexReadSession(schemaRead.indexGetForName(COMPOSITE_INDEX_NAME));
        try (KernelTransaction tx = beginTransaction();
                var cursor = entityParams.allocateEntityValueIndexCursor(tx, cursors)) {
            // when
            entityParams.entitySetProperty(tx, jackDalton, firstName, "Jesse");
            entityParams.entitySetProperty(tx, jackDalton, surname, "James");
            entityParams.entityIndexSeek(
                    tx,
                    index,
                    cursor,
                    unordered(needsValues),
                    PropertyIndexQuery.exact(firstName, "Jesse"),
                    PropertyIndexQuery.exact(surname, "James"));

            // then
            assertTrue(cursor.next());
            assertEquals(jackDalton, entityParams.entityReference(cursor));
        }
    }

    @Test
    void shouldFindSwappedEntityInCompositeIndex() throws Exception {
        assumeTrue(entityParams.tokenlessEntitySupported());
        // Given
        boolean needsValues = false;
        int label = entityParams.entityTokenId(tx, PERSON_TOKEN);
        int firstName = token.propertyKey(FIRSTNAME_PROP_NAME);
        int surname = token.propertyKey(SURNAME_PROP_NAME);
        IndexReadSession index = read.indexReadSession(schemaRead.indexGetForName(COMPOSITE_INDEX_NAME));
        try (KernelTransaction tx = beginTransaction();
                var cursor = entityParams.allocateEntityValueIndexCursor(tx, cursors)) {
            // when
            entityParams.entityRemoveToken(tx, joeDalton, label);
            entityParams.entityAddToken(tx, strOneNoLabel, label);
            entityParams.entitySetProperty(tx, strOneNoLabel, firstName, "Jesse");
            entityParams.entitySetProperty(tx, strOneNoLabel, surname, "James");
            entityParams.entityIndexSeek(
                    tx,
                    index,
                    cursor,
                    unordered(needsValues),
                    PropertyIndexQuery.exact(firstName, "Jesse"),
                    PropertyIndexQuery.exact(surname, "James"));

            // then
            assertTrue(cursor.next());
            assertEquals(strOneNoLabel, entityParams.entityReference(cursor));
        }
    }

    @Test
    void shouldHandleOrderedExactSeekWithNeedsValuesTrue() throws Exception {
        // given
        int prop = token.propertyKey("prop");
        IndexReadSession index = tx.dataRead().indexReadSession(tx.schemaRead().indexGetForName(PROP_INDEX_NAME));
        try (var cursor = entityParams.allocateEntityValueIndexCursor(tx, cursors)) {
            // when
            entityParams.entityIndexSeek(
                    tx, index, cursor, constrained(IndexOrder.ASCENDING, true), PropertyIndexQuery.exact(prop, 5));

            // then
            assertTrue(cursor.next());
            assertTrue(cursor.hasValue());
            assertEquals(cursor.propertyValue(0), intValue(5));
            assertFalse(cursor.next());
        }
    }

    @Test
    void shouldHandleOrderedExactSeekWithNeedsValuesTrueWithTxChanges() throws Exception {
        // given
        int prop = token.propertyKey("prop");
        IndexReadSession index = tx.dataRead().indexReadSession(tx.schemaRead().indexGetForName(PROP_INDEX_NAME));
        int label = entityParams.entityTokenId(tx, DEFAULT_ENTITY_TOKEN);
        try (KernelTransaction tx = beginTransaction();
                var cursor = entityParams.allocateEntityValueIndexCursor(tx, cursors)) {
            // when
            long newEntity = entityParams.entityCreateNew(tx, label);
            entityParams.entitySetProperty(tx, newEntity, prop, intValue(5));
            entityParams.entityIndexSeek(
                    tx, index, cursor, constrained(IndexOrder.ASCENDING, true), PropertyIndexQuery.exact(prop, 5));

            // then
            assertTrue(cursor.next());
            assertTrue(cursor.hasValue());
            assertEquals(cursor.propertyValue(0), intValue(5));
            assertTrue(cursor.next());
            assertTrue(cursor.hasValue());
            assertEquals(cursor.propertyValue(0), intValue(5));

            assertFalse(cursor.next());
        }
    }

    private long entityWithProp(Transaction tx, Object value) {
        return entityWithProp(tx, PROP_NAME, value);
    }

    private long entityWithProp(Transaction tx, String key, Object value) {
        return entityParams.entityWithProp(tx, DEFAULT_ENTITY_TOKEN, key, value);
    }

    private long entityWithWhatever(Transaction tx, Object value) {
        return entityParams.entityWithProp(tx, WHAT_TOKEN, EVER_PROP_NAME, value);
    }

    private long entityWithNoLabel(Transaction tx, Object value) {
        return entityParams.entityNoTokenWithProp(tx, PROP_NAME, value);
    }

    private long person(Transaction tx, String firstName, String surname) {
        return entityParams.entityWithTwoProps(
                tx, PERSON_TOKEN, FIRSTNAME_PROP_NAME, firstName, SURNAME_PROP_NAME, surname);
    }
}
