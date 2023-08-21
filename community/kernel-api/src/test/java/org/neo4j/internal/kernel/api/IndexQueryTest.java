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
package org.neo4j.internal.kernel.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.util.Arrays.array;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.values.storable.Values.pointValue;
import static org.neo4j.values.storable.Values.stringValue;

import java.time.ZonedDateTime;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.kernel.api.PropertyIndexQuery.BoundingBoxPredicate;
import org.neo4j.internal.kernel.api.PropertyIndexQuery.ExactPredicate;
import org.neo4j.internal.kernel.api.PropertyIndexQuery.ExistsPredicate;
import org.neo4j.internal.kernel.api.PropertyIndexQuery.RangePredicate;
import org.neo4j.internal.kernel.api.PropertyIndexQuery.StringContainsPredicate;
import org.neo4j.internal.kernel.api.PropertyIndexQuery.StringPrefixPredicate;
import org.neo4j.internal.kernel.api.PropertyIndexQuery.StringSuffixPredicate;
import org.neo4j.internal.schema.IndexQuery.IndexQueryType;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueCategory;
import org.neo4j.values.storable.Values;

class IndexQueryTest {
    private final int propId = 0;

    // ALL

    @Test
    void testAll() {
        final var allEntries = PropertyIndexQuery.allEntries();
        Stream.of(
                        999,
                        array(-999, 999),
                        "foo",
                        array("foo", "bar"),
                        pointValue(CoordinateReferenceSystem.WGS_84, 12.994807, 55.612088),
                        array(
                                pointValue(CoordinateReferenceSystem.WGS_84, 12.994807, 55.612088),
                                pointValue(CoordinateReferenceSystem.WGS_84, -0.101008, 51.503773)),
                        ZonedDateTime.now(),
                        array(ZonedDateTime.now(), ZonedDateTime.now().plusWeeks(2)),
                        true,
                        array(false, true))
                .map(value -> test(allEntries, value))
                .forEach(Assertions::assertTrue);
    }

    // EXISTS

    @Test
    void testExists() {
        ExistsPredicate p = PropertyIndexQuery.exists(propId);

        assertTrue(test(p, "string"));
        assertTrue(test(p, 1));
        assertTrue(test(p, 1.0));
        assertTrue(test(p, true));
        assertTrue(test(p, new long[] {1L}));
        assertTrue(test(p, pointValue(CoordinateReferenceSystem.WGS_84, 12.3, 45.6)));

        assertFalse(test(p, null));
    }

    // EXACT

    @Test
    void testExact() {
        assertExactPredicate("string");
        assertExactPredicate(1);
        assertExactPredicate(1.0);
        assertExactPredicate(true);
        assertExactPredicate(new long[] {1L});
        assertExactPredicate(pointValue(CoordinateReferenceSystem.WGS_84, 12.3, 45.6));
    }

    private void assertExactPredicate(Object value) {
        ExactPredicate p = PropertyIndexQuery.exact(propId, value);

        assertTrue(test(p, value));

        assertFalseForOtherThings(p);
    }

    @Test
    void testExact_ComparingBigDoublesAndLongs() {
        ExactPredicate p = PropertyIndexQuery.exact(propId, 9007199254740993L);

        assertFalse(test(p, 9007199254740992D));
    }

    // NUMERIC RANGE

    @Test
    void testNumRange_FalseForIrrelevant() {
        RangePredicate<?> p = PropertyIndexQuery.range(propId, 11, true, 13, true);

        assertFalseForOtherThings(p);
    }

    @Test
    void testNumRange_InclusiveLowerInclusiveUpper() {
        RangePredicate<?> p = PropertyIndexQuery.range(propId, 11, true, 13, true);

        assertFalse(test(p, 10));
        assertTrue(test(p, 11));
        assertTrue(test(p, 12));
        assertTrue(test(p, 13));
        assertFalse(test(p, 14));
    }

    @Test
    void testNumRange_ExclusiveLowerExclusiveLower() {
        RangePredicate<?> p = PropertyIndexQuery.range(propId, 11, false, 13, false);

        assertFalse(test(p, 11));
        assertTrue(test(p, 12));
        assertFalse(test(p, 13));
    }

    @Test
    void testNumRange_InclusiveLowerExclusiveUpper() {
        RangePredicate<?> p = PropertyIndexQuery.range(propId, 11, true, 13, false);

        assertFalse(test(p, 10));
        assertTrue(test(p, 11));
        assertTrue(test(p, 12));
        assertFalse(test(p, 13));
    }

    @Test
    void testNumRange_ExclusiveLowerInclusiveUpper() {
        RangePredicate<?> p = PropertyIndexQuery.range(propId, 11, false, 13, true);

        assertFalse(test(p, 11));
        assertTrue(test(p, 12));
        assertTrue(test(p, 13));
        assertFalse(test(p, 14));
    }

    @Test
    void testNumRange_LowerNullValue() {
        RangePredicate<?> p = PropertyIndexQuery.range(propId, null, true, 13, true);

        assertTrue(test(p, 10));
        assertTrue(test(p, 11));
        assertTrue(test(p, 12));
        assertTrue(test(p, 13));
        assertFalse(test(p, 14));
    }

    @Test
    void testNumRange_UpperNullValue() {
        RangePredicate<?> p = PropertyIndexQuery.range(propId, 11, true, null, true);

        assertFalse(test(p, 10));
        assertTrue(test(p, 11));
        assertTrue(test(p, 12));
        assertTrue(test(p, 13));
        assertTrue(test(p, 14));
    }

    @Test
    void testNumRange_ComparingBigDoublesAndLongs() {
        RangePredicate<?> p = PropertyIndexQuery.range(propId, 9007199254740993L, true, null, true);

        assertFalse(test(p, 9007199254740992D));
    }

    // STRING RANGE

    @Test
    void testStringRange_FalseForIrrelevant() {
        RangePredicate<?> p = PropertyIndexQuery.range(propId, "bbb", true, "bee", true);

        assertFalseForOtherThings(p);
    }

    @Test
    void testStringRange_InclusiveLowerInclusiveUpper() {
        RangePredicate<?> p = PropertyIndexQuery.range(propId, "bbb", true, "bee", true);

        assertFalse(test(p, "bba"));
        assertTrue(test(p, "bbb"));
        assertTrue(test(p, "bee"));
        assertFalse(test(p, "beea"));
        assertFalse(test(p, "bef"));
    }

    @Test
    void testStringRange_ExclusiveLowerInclusiveUpper() {
        RangePredicate<?> p = PropertyIndexQuery.range(propId, "bbb", false, "bee", true);

        assertFalse(test(p, "bbb"));
        assertTrue(test(p, "bbba"));
        assertTrue(test(p, "bee"));
        assertFalse(test(p, "beea"));
    }

    @Test
    void testStringRange_InclusiveLowerExclusiveUpper() {
        RangePredicate<?> p = PropertyIndexQuery.range(propId, "bbb", true, "bee", false);

        assertFalse(test(p, "bba"));
        assertTrue(test(p, "bbb"));
        assertTrue(test(p, "bed"));
        assertFalse(test(p, "bee"));
    }

    @Test
    void testStringRange_ExclusiveLowerExclusiveUpper() {
        RangePredicate<?> p = PropertyIndexQuery.range(propId, "bbb", false, "bee", false);

        assertFalse(test(p, "bbb"));
        assertTrue(test(p, "bbba"));
        assertTrue(test(p, "bed"));
        assertFalse(test(p, "bee"));
    }

    @Test
    void testStringRange_UpperUnbounded() {
        RangePredicate<?> p = PropertyIndexQuery.range(propId, "bbb", false, null, false);

        assertFalse(test(p, "bbb"));
        assertTrue(test(p, "bbba"));
        assertTrue(test(p, "xxxxx"));
    }

    @Test
    void testStringRange_LowerUnbounded() {
        RangePredicate<?> p = PropertyIndexQuery.range(propId, null, false, "bee", false);

        assertTrue(test(p, ""));
        assertTrue(test(p, "bed"));
        assertFalse(test(p, "bee"));
    }

    @Test
    void testDateRange() {
        RangePredicate<?> p =
                PropertyIndexQuery.range(propId, DateValue.date(2014, 7, 7), true, DateValue.date(2017, 3, 7), false);

        assertFalse(test(p, DateValue.date(2014, 6, 8)));
        assertTrue(test(p, DateValue.date(2014, 7, 7)));
        assertTrue(test(p, DateValue.date(2016, 6, 8)));
        assertFalse(test(p, DateValue.date(2017, 3, 7)));
        assertFalse(test(p, DateValue.date(2017, 3, 8)));
        assertFalse(test(p, LocalDateTimeValue.localDateTime(2016, 3, 8, 0, 0, 0, 0)));
    }

    // BOUNDING BOX

    private final PointValue gps1 = pointValue(CoordinateReferenceSystem.WGS_84, -12.6, -56.7);
    private final PointValue gps2 = pointValue(CoordinateReferenceSystem.WGS_84, -12.6, -55.7);
    private final PointValue gps3 = pointValue(CoordinateReferenceSystem.WGS_84, -11.0, -55);
    private final PointValue gps4 = pointValue(CoordinateReferenceSystem.WGS_84, 0, 0);
    private final PointValue gps5 = pointValue(CoordinateReferenceSystem.WGS_84, 14.6, 56.7);
    private final PointValue gps6 = pointValue(CoordinateReferenceSystem.WGS_84, 14.6, 58.7);
    private final PointValue gps7 = pointValue(CoordinateReferenceSystem.WGS_84, 15.6, 59.7);
    private final PointValue car1 = pointValue(CoordinateReferenceSystem.CARTESIAN, 0, 0);
    private final PointValue car2 = pointValue(CoordinateReferenceSystem.CARTESIAN, 2, 2);
    private final PointValue car3 = pointValue(CoordinateReferenceSystem.CARTESIAN_3D, 1, 2, 3);
    private final PointValue car4 = pointValue(CoordinateReferenceSystem.CARTESIAN_3D, 2, 3, 4);
    private final PointValue gps1_3d = pointValue(CoordinateReferenceSystem.WGS_84_3D, 12.6, 56.8, 100.0);
    private final PointValue gps2_3d = pointValue(CoordinateReferenceSystem.WGS_84_3D, 12.8, 56.9, 200.0);

    // TODO: Also insert points which can't be compared e.g. Cartesian and (-100, 100)

    @Test
    void testBoundingBox_FalseForIrrelevant() {
        BoundingBoxPredicate p = PropertyIndexQuery.boundingBox(propId, gps2, gps5);

        assertFalseForOtherThings(p);
    }

    @Test
    void testBoundingBox_InclusiveLowerInclusiveUpper() {
        BoundingBoxPredicate p = PropertyIndexQuery.boundingBox(propId, gps2, gps5);

        assertFalse(test(p, gps1));
        assertTrue(test(p, gps2));
        assertTrue(test(p, gps5));
        assertFalse(test(p, gps6));
        assertFalse(test(p, gps7));
        assertFalse(test(p, car1));
        assertFalse(test(p, car2));
        assertFalse(test(p, car3));
        assertFalse(test(p, gps1_3d));
    }

    @Test
    void testBoundingBox_Cartesian3D() {
        BoundingBoxPredicate p = PropertyIndexQuery.boundingBox(propId, car3, car4);

        assertFalse(test(p, gps1));
        assertFalse(test(p, gps3));
        assertFalse(test(p, gps5));
        assertFalse(test(p, car1));
        assertFalse(test(p, car2));
        assertTrue(test(p, car3));
        assertTrue(test(p, car4));
        assertFalse(test(p, gps1_3d));
        assertFalse(test(p, gps2_3d));
    }

    @Test
    void testBoundingBox_WGS84_3D() {
        BoundingBoxPredicate p = PropertyIndexQuery.boundingBox(propId, gps1_3d, gps2_3d);

        assertFalse(test(p, gps1));
        assertFalse(test(p, gps3));
        assertFalse(test(p, gps5));
        assertFalse(test(p, car1));
        assertFalse(test(p, car2));
        assertFalse(test(p, car3));
        assertFalse(test(p, car4));
        assertTrue(test(p, gps1_3d));
        assertTrue(test(p, gps2_3d));
    }

    // STRING PREFIX

    @Test
    void testStringPrefix_FalseForIrrelevant() {
        StringPrefixPredicate p = PropertyIndexQuery.stringPrefix(propId, stringValue("dog"));

        assertFalseForOtherThings(p);
    }

    @Test
    void testStringPrefix_SomeValues() {
        StringPrefixPredicate p = PropertyIndexQuery.stringPrefix(propId, stringValue("dog"));

        assertFalse(test(p, "doffington"));
        assertFalse(test(p, "doh, not this again!"));
        assertTrue(test(p, "dog"));
        assertTrue(test(p, "doggidog"));
        assertTrue(test(p, "doggidogdog"));
    }

    // STRING CONTAINS

    @Test
    void testStringContains_FalseForIrrelevant() {
        StringContainsPredicate p = PropertyIndexQuery.stringContains(propId, stringValue("cat"));

        assertFalseForOtherThings(p);
    }

    @Test
    void testStringContains_SomeValues() {
        StringContainsPredicate p = PropertyIndexQuery.stringContains(propId, stringValue("cat"));

        assertFalse(test(p, "dog"));
        assertFalse(test(p, "cameraman"));
        assertFalse(test(p, "Cat"));
        assertTrue(test(p, "cat"));
        assertTrue(test(p, "bobcat"));
        assertTrue(test(p, "scatman"));
    }

    // STRING SUFFIX

    @Test
    void testStringSuffix_FalseForIrrelevant() {
        StringSuffixPredicate p = PropertyIndexQuery.stringSuffix(propId, stringValue("less"));

        assertFalseForOtherThings(p);
    }

    @Test
    void testStringSuffix_SomeValues() {
        StringSuffixPredicate p = PropertyIndexQuery.stringSuffix(propId, stringValue("less"));

        assertFalse(test(p, "lesser being"));
        assertFalse(test(p, "make less noise please..."));
        assertTrue(test(p, "less"));
        assertTrue(test(p, "clueless"));
        assertTrue(test(p, "cluelessly clueless"));
    }

    // TOKEN

    @Test
    void testValueCategoryOfTokenPredicate() {
        TokenPredicate query = new TokenPredicate(1);

        assertThat(query.valueCategory()).isEqualTo(ValueCategory.NO_CATEGORY);
    }

    @Test
    void testIndexQueryTypeOfTokenPredicate() {
        TokenPredicate query = new TokenPredicate(1);

        assertThat(query.type()).isEqualTo(IndexQueryType.TOKEN_LOOKUP);
    }

    // HELPERS

    private static void assertFalseForOtherThings(PropertyIndexQuery p) {
        assertFalse(test(p, "other string"));
        assertFalse(test(p, "string1"));
        assertFalse(test(p, ""));
        assertFalse(test(p, -1));
        assertFalse(test(p, -1.0));
        assertFalse(test(p, false));
        assertFalse(test(p, new long[] {-1L}));
        assertFalse(test(p, null));
    }

    private static boolean test(PropertyIndexQuery p, Object x) {
        return p.acceptsValue(x instanceof Value ? (Value) x : Values.of(x));
    }
}
