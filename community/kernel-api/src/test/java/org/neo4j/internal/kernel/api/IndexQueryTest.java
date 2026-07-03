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
import static org.neo4j.internal.kernel.api.PropertyIndexQuery.range;
import static org.neo4j.values.storable.Values.pointValue;
import static org.neo4j.values.storable.Values.stringValue;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAmount;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.kernel.api.PropertyIndexQuery.BoundingBoxPredicate;
import org.neo4j.internal.kernel.api.PropertyIndexQuery.ExactPredicate;
import org.neo4j.internal.kernel.api.PropertyIndexQuery.ExistsPredicate;
import org.neo4j.internal.kernel.api.PropertyIndexQuery.NotExistsPredicate;
import org.neo4j.internal.kernel.api.PropertyIndexQuery.RangePredicate;
import org.neo4j.internal.kernel.api.PropertyIndexQuery.StringContainsPredicate;
import org.neo4j.internal.kernel.api.PropertyIndexQuery.StringPrefixPredicate;
import org.neo4j.internal.kernel.api.PropertyIndexQuery.StringSuffixPredicate;
import org.neo4j.internal.schema.IndexQuery.IndexQueryType;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.PointArray;
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
                .forEach(b -> assertThat(b).isTrue());
    }

    // EXISTS

    @Test
    void testExists() {
        ExistsPredicate p = PropertyIndexQuery.exists(propId);

        assertThat(test(p, "string")).isTrue();
        assertThat(test(p, 1)).isTrue();
        assertThat(test(p, 1.0)).isTrue();
        assertThat(test(p, true)).isTrue();
        assertThat(test(p, new long[] {1L})).isTrue();
        assertThat(test(p, pointValue(CoordinateReferenceSystem.WGS_84, 12.3, 45.6)))
                .isTrue();

        assertThat(test(p, null)).isFalse();
    }

    @Test
    void testNotExists() {
        NotExistsPredicate p = PropertyIndexQuery.notExists(propId);

        assertThat(test(p, "string")).isFalse();
        assertThat(test(p, 1)).isFalse();
        assertThat(test(p, 1.0)).isFalse();
        assertThat(test(p, true)).isFalse();
        assertThat(test(p, new long[] {1L})).isFalse();
        assertThat(test(p, pointValue(CoordinateReferenceSystem.WGS_84, 12.3, 45.6)))
                .isFalse();

        assertThat(test(p, null)).isTrue();
        assertThat(test(p, Values.NO_VALUE)).isTrue();
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

        assertThat(test(p, value)).isTrue();

        assertFalseForOtherThings(p);
    }

    @Test
    void testExact_ComparingBigDoublesAndLongs() {
        ExactPredicate p = PropertyIndexQuery.exact(propId, 9007199254740993L);

        assertThat(test(p, 9007199254740992D)).isFalse();
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

        assertThat(test(p, 10)).isFalse();
        assertThat(test(p, 11)).isTrue();
        assertThat(test(p, 12)).isTrue();
        assertThat(test(p, 13)).isTrue();
        assertThat(test(p, 14)).isFalse();
    }

    @Test
    void testNumRange_ExclusiveLowerExclusiveUpper() {
        RangePredicate<?> p = PropertyIndexQuery.range(propId, 11, false, 13, false);

        assertThat(test(p, 11)).isFalse();
        assertThat(test(p, 12)).isTrue();
        assertThat(test(p, 13)).isFalse();
    }

    @Test
    void testNumRange_InclusiveLowerExclusiveUpper() {
        RangePredicate<?> p = PropertyIndexQuery.range(propId, 11, true, 13, false);

        assertThat(test(p, 10)).isFalse();
        assertThat(test(p, 11)).isTrue();
        assertThat(test(p, 12)).isTrue();
        assertThat(test(p, 13)).isFalse();
    }

    @Test
    void testNumRange_ExclusiveLowerInclusiveUpper() {
        RangePredicate<?> p = PropertyIndexQuery.range(propId, 11, false, 13, true);

        assertThat(test(p, 11)).isFalse();
        assertThat(test(p, 12)).isTrue();
        assertThat(test(p, 13)).isTrue();
        assertThat(test(p, 14)).isFalse();
    }

    @Test
    void testNumRange_LowerNullValue() {
        RangePredicate<?> p = PropertyIndexQuery.range(propId, null, true, 13, true);

        assertThat(test(p, 10)).isTrue();
        assertThat(test(p, 11)).isTrue();
        assertThat(test(p, 12)).isTrue();
        assertThat(test(p, 13)).isTrue();
        assertThat(test(p, 14)).isFalse();
    }

    @Test
    void testNumRange_UpperNullValue() {
        RangePredicate<?> p = PropertyIndexQuery.range(propId, 11, true, null, true);

        assertThat(test(p, 10)).isFalse();
        assertThat(test(p, 11)).isTrue();
        assertThat(test(p, 12)).isTrue();
        assertThat(test(p, 13)).isTrue();
        assertThat(test(p, 14)).isTrue();
    }

    @Test
    void testNumRange_ComparingBigDoublesAndLongs() {
        RangePredicate<?> p = PropertyIndexQuery.range(propId, 9007199254740993L, true, null, true);

        assertThat(test(p, 9007199254740992D)).isFalse();
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

        assertThat(test(p, "bba")).isFalse();
        assertThat(test(p, "bbb")).isTrue();
        assertThat(test(p, "bee")).isTrue();
        assertThat(test(p, "beea")).isFalse();
        assertThat(test(p, "bef")).isFalse();
    }

    @Test
    void testStringRange_ExclusiveLowerInclusiveUpper() {
        RangePredicate<?> p = PropertyIndexQuery.range(propId, "bbb", false, "bee", true);

        assertThat(test(p, "bbb")).isFalse();
        assertThat(test(p, "bbba")).isTrue();
        assertThat(test(p, "bee")).isTrue();
        assertThat(test(p, "beea")).isFalse();
    }

    @Test
    void testStringRange_InclusiveLowerExclusiveUpper() {
        RangePredicate<?> p = PropertyIndexQuery.range(propId, "bbb", true, "bee", false);

        assertThat(test(p, "bba")).isFalse();
        assertThat(test(p, "bbb")).isTrue();
        assertThat(test(p, "bed")).isTrue();
        assertThat(test(p, "bee")).isFalse();
    }

    @Test
    void testStringRange_ExclusiveLowerExclusiveUpper() {
        RangePredicate<?> p = PropertyIndexQuery.range(propId, "bbb", false, "bee", false);

        assertThat(test(p, "bbb")).isFalse();
        assertThat(test(p, "bbba")).isTrue();
        assertThat(test(p, "bed")).isTrue();
        assertThat(test(p, "bee")).isFalse();
    }

    @Test
    void testStringRange_UpperUnbounded() {
        RangePredicate<?> p = PropertyIndexQuery.range(propId, "bbb", false, null, false);

        assertThat(test(p, "bbb")).isFalse();
        assertThat(test(p, "bbba")).isTrue();
        assertThat(test(p, "xxxxx")).isTrue();
    }

    @Test
    void testStringRange_LowerUnbounded() {
        RangePredicate<?> p = PropertyIndexQuery.range(propId, null, false, "bee", false);

        assertThat(test(p, "")).isTrue();
        assertThat(test(p, "bed")).isTrue();
        assertThat(test(p, "bee")).isFalse();
    }

    @Test
    void testDateRange() {
        RangePredicate<?> p =
                PropertyIndexQuery.range(propId, DateValue.date(2014, 7, 7), true, DateValue.date(2017, 3, 7), false);

        assertThat(test(p, DateValue.date(2014, 6, 8))).isFalse();
        assertThat(test(p, DateValue.date(2014, 7, 7))).isTrue();
        assertThat(test(p, DateValue.date(2016, 6, 8))).isTrue();
        assertThat(test(p, DateValue.date(2017, 3, 7))).isFalse();
        assertThat(test(p, DateValue.date(2017, 3, 8))).isFalse();
        assertThat(test(p, LocalDateTimeValue.localDateTime(2016, 3, 8, 0, 0, 0, 0)))
                .isFalse();
    }

    // Duration RANGE

    @Test
    void testDurationRange_FalseForIrrelevant() {
        RangePredicate<?> p = range(propId, secs(11), true, secs(13), true);

        assertFalseForOtherThings(p);
    }

    @Test
    void testDurationRange_InclusiveLowerInclusiveUpper() {

        RangePredicate<?> p = range(propId, secs(11), true, secs(13), true);

        assertThat(test(p, secs(10))).isFalse();
        assertThat(test(p, secs(11))).isFalse();
        assertThat(test(p, secs(12))).isFalse();
        assertThat(test(p, secs(13))).isFalse();
        assertThat(test(p, secs(14))).isFalse();
    }

    @Test
    void testDurationRange_ExclusiveLowerExclusiveUpper() {
        RangePredicate<?> p = range(propId, secs(11), false, secs(13), false);

        assertThat(test(p, secs(11))).isFalse();
        assertThat(test(p, secs(12))).isFalse();
        assertThat(test(p, secs(13))).isFalse();
    }

    @Test
    void testDurationRange_InclusiveLowerExclusiveUpper() {
        RangePredicate<?> p = range(propId, secs(11), true, secs(13), false);

        assertThat(test(p, secs(10))).isFalse();
        assertThat(test(p, secs(11))).isFalse();
        assertThat(test(p, secs(12))).isFalse();
        assertThat(test(p, secs(13))).isFalse();
    }

    @Test
    void testDurationRange_ExclusiveLowerInclusiveUpper() {
        RangePredicate<?> p = range(propId, secs(11), false, secs(13), true);

        assertThat(test(p, secs(11))).isFalse();
        assertThat(test(p, secs(12))).isFalse();
        assertThat(test(p, secs(13))).isFalse();
        assertThat(test(p, secs(14))).isFalse();
    }

    @Test
    void testDurationRange_LowerNullValue() {
        RangePredicate<?> p = range(propId, null, true, secs(13), true);

        assertThat(test(p, secs(10))).isFalse();
        assertThat(test(p, secs(11))).isFalse();
        assertThat(test(p, secs(12))).isFalse();
        assertThat(test(p, secs(13))).isTrue();
        assertThat(test(p, secs(14))).isFalse();
    }

    @Test
    void testDurationRange_UpperNullValue() {
        RangePredicate<?> p = range(propId, secs(11), true, null, true);

        assertThat(test(p, secs(10))).isFalse();
        assertThat(test(p, secs(11))).isTrue();
        assertThat(test(p, secs(12))).isFalse();
        assertThat(test(p, secs(13))).isFalse();
        assertThat(test(p, secs(14))).isFalse();
    }

    @Test
    void testDurationRange_EqualBoundsInclusive() {
        RangePredicate<?> p = range(propId, secs(11), true, secs(11), true);

        assertThat(test(p, secs(10))).isFalse();
        assertThat(test(p, secs(11))).isTrue();
        assertThat(test(p, secs(12))).isFalse();
    }

    @Test
    void testDurationRange_EqualBoundsNotInclusive() {
        RangePredicate<?> fromInclusive = range(propId, secs(11), true, secs(11), false);

        assertThat(test(fromInclusive, secs(10))).isFalse();
        assertThat(test(fromInclusive, secs(11))).isFalse();
        assertThat(test(fromInclusive, secs(12))).isFalse();

        RangePredicate<?> toInclusive = range(propId, secs(11), false, secs(11), true);

        assertThat(test(toInclusive, secs(10))).isFalse();
        assertThat(test(toInclusive, secs(11))).isFalse();
        assertThat(test(toInclusive, secs(12))).isFalse();
    }

    // Point RANGE

    @Test
    void testPointRange_FalseForIrrelevant() {
        RangePredicate<?> p = range(propId, point(11), true, point(13), true);

        assertFalseForOtherThings(p);
    }

    @Test
    void testPointRange_InclusiveLowerInclusiveUpper() {
        RangePredicate<?> p = range(propId, point(11), true, point(13), true);

        assertThat(test(p, point(10))).isFalse();
        assertThat(test(p, point(11))).isFalse();
        assertThat(test(p, point(12))).isFalse();
        assertThat(test(p, point(13))).isFalse();
        assertThat(test(p, point(14))).isFalse();
    }

    @Test
    void testPointRange_ExclusiveLowerExclusiveUpper() {
        RangePredicate<?> p = range(propId, point(11), false, point(13), false);

        assertThat(test(p, point(11))).isFalse();
        assertThat(test(p, point(12))).isFalse();
        assertThat(test(p, point(13))).isFalse();
    }

    @Test
    void testPointRange_InclusiveLowerExclusiveUpper() {
        RangePredicate<?> p = range(propId, point(11), true, point(13), false);

        assertThat(test(p, point(10))).isFalse();
        assertThat(test(p, point(11))).isFalse();
        assertThat(test(p, point(12))).isFalse();
        assertThat(test(p, point(13))).isFalse();
    }

    @Test
    void testPointRange_ExclusiveLowerInclusiveUpper() {
        RangePredicate<?> p = range(propId, point(11), false, point(13), true);

        assertThat(test(p, point(11))).isFalse();
        assertThat(test(p, point(12))).isFalse();
        assertThat(test(p, point(13))).isFalse();
        assertThat(test(p, point(14))).isFalse();
    }

    @Test
    void testPointRange_LowerNullValue() {
        RangePredicate<?> p = range(propId, null, true, point(13), true);

        assertThat(test(p, point(10))).isFalse();
        assertThat(test(p, point(11))).isFalse();
        assertThat(test(p, point(12))).isFalse();
        assertThat(test(p, point(13))).isTrue();
        assertThat(test(p, point(14))).isFalse();
    }

    @Test
    void testPointRange_UpperNullValue() {
        RangePredicate<?> p = range(propId, point(11), true, null, true);

        assertThat(test(p, point(10))).isFalse();
        assertThat(test(p, point(11))).isTrue();
        assertThat(test(p, point(12))).isFalse();
        assertThat(test(p, point(13))).isFalse();
        assertThat(test(p, point(14))).isFalse();
    }

    @Test
    void testPointRange_EqualBounds() {
        RangePredicate<?> p = range(propId, point(11), true, point(11), true);

        assertThat(test(p, point(10))).isFalse();
        assertThat(test(p, point(11))).isTrue();
        assertThat(test(p, point(12))).isFalse();
    }

    @Test
    void testPointRange_EqualBoundsNotInclusive() {
        RangePredicate<?> fromInclusive = range(propId, point(11), true, point(11), false);

        assertThat(test(fromInclusive, point(10))).isFalse();
        assertThat(test(fromInclusive, point(11))).isFalse();
        assertThat(test(fromInclusive, point(12))).isFalse();

        RangePredicate<?> toInclusive = range(propId, point(11), false, point(11), true);

        assertThat(test(toInclusive, point(10))).isFalse();
        assertThat(test(toInclusive, point(11))).isFalse();
        assertThat(test(toInclusive, point(12))).isFalse();
    }

    // Duration Array RANGE

    @Test
    void testDurationArrayRange_FalseForIrrelevant() {
        RangePredicate<?> p = range(propId, secsArray(11), true, secsArray(13), true);

        assertFalseForOtherThings(p);
    }

    @Test
    void testDurationArrayRange_InclusiveLowerInclusiveUpper() {
        RangePredicate<?> p = range(propId, secsArray(11), true, secsArray(13), true);

        assertThat(test(p, secsArray(10))).isFalse();
        assertThat(test(p, secsArray(11))).isFalse();
        assertThat(test(p, secsArray(12))).isFalse();
        assertThat(test(p, secsArray(13))).isFalse();
        assertThat(test(p, secsArray(14))).isFalse();
    }

    @Test
    void testDurationArrayRange_ExclusiveLowerExclusiveUpper() {
        RangePredicate<?> p = range(propId, secsArray(11), false, secsArray(13), false);

        assertThat(test(p, secsArray(11))).isFalse();
        assertThat(test(p, secsArray(12))).isFalse();
        assertThat(test(p, secsArray(13))).isFalse();
    }

    @Test
    void testDurationArrayRange_InclusiveLowerExclusiveUpper() {
        RangePredicate<?> p = range(propId, secsArray(11), true, secsArray(13), false);

        assertThat(test(p, secsArray(10))).isFalse();
        assertThat(test(p, secsArray(11))).isFalse();
        assertThat(test(p, secsArray(12))).isFalse();
        assertThat(test(p, secsArray(13))).isFalse();
    }

    @Test
    void testDurationArrayRange_ExclusiveLowerInclusiveUpper() {
        RangePredicate<?> p = range(propId, secsArray(11), false, secsArray(13), true);

        assertThat(test(p, secsArray(11))).isFalse();
        assertThat(test(p, secsArray(12))).isFalse();
        assertThat(test(p, secsArray(13))).isFalse();
        assertThat(test(p, secsArray(14))).isFalse();
    }

    @Test
    void testDurationArrayRange_LowerNullValue() {
        RangePredicate<?> p = range(propId, null, true, secsArray(13), true);

        assertThat(test(p, secsArray(10))).isFalse();
        assertThat(test(p, secsArray(11))).isFalse();
        assertThat(test(p, secsArray(12))).isFalse();
        assertThat(test(p, secsArray(13))).isTrue();
        assertThat(test(p, secsArray(14))).isFalse();
    }

    @Test
    void testDurationArrayRange_UpperNullValue() {
        RangePredicate<?> p = range(propId, secsArray(11), true, null, true);

        assertThat(test(p, secsArray(10))).isFalse();
        assertThat(test(p, secsArray(11))).isTrue();
        assertThat(test(p, secsArray(12))).isFalse();
        assertThat(test(p, secsArray(13))).isFalse();
        assertThat(test(p, secsArray(14))).isFalse();
    }

    @Test
    void testDurationArrayRange_EqualBounds() {
        RangePredicate<?> p = range(propId, secsArray(11), true, secsArray(11), true);

        assertThat(test(p, secsArray(10))).isFalse();
        assertThat(test(p, secsArray(11))).isTrue();
        assertThat(test(p, secsArray(12))).isFalse();
    }

    @Test
    void testDurationArrayRange_EqualBoundsNotInclusive() {
        RangePredicate<?> fromInclusive = range(propId, secsArray(11), true, secsArray(11), false);

        assertThat(test(fromInclusive, secsArray(10))).isFalse();
        assertThat(test(fromInclusive, secsArray(11))).isFalse();
        assertThat(test(fromInclusive, secsArray(12))).isFalse();

        RangePredicate<?> toInclusive = range(propId, secsArray(11), false, secsArray(11), true);

        assertThat(test(toInclusive, secsArray(10))).isFalse();
        assertThat(test(toInclusive, secsArray(11))).isFalse();
        assertThat(test(toInclusive, secsArray(12))).isFalse();
    }

    // Point Array RANGE

    @Test
    void testPointArrayRange_FalseForIrrelevant() {
        RangePredicate<?> p = range(propId, pointArray(11), true, pointArray(13), true);

        assertFalseForOtherThings(p);
    }

    @Test
    void testPointArrayRange_InclusiveLowerInclusiveUpper() {
        RangePredicate<?> p = range(propId, pointArray(11), true, pointArray(13), true);

        assertThat(test(p, pointArray(10))).isFalse();
        assertThat(test(p, pointArray(11))).isFalse();
        assertThat(test(p, pointArray(12))).isFalse();
        assertThat(test(p, pointArray(13))).isFalse();
        assertThat(test(p, pointArray(14))).isFalse();
    }

    @Test
    void testPointArrayRange_ExclusiveLowerExclusiveUpper() {
        RangePredicate<?> p = range(propId, pointArray(11), false, pointArray(13), false);

        assertThat(test(p, pointArray(11))).isFalse();
        assertThat(test(p, pointArray(12))).isFalse();
        assertThat(test(p, pointArray(13))).isFalse();
    }

    @Test
    void testPointArrayRange_InclusiveLowerExclusiveUpper() {
        RangePredicate<?> p = range(propId, pointArray(11), true, pointArray(13), false);

        assertThat(test(p, pointArray(10))).isFalse();
        assertThat(test(p, pointArray(11))).isFalse();
        assertThat(test(p, pointArray(12))).isFalse();
        assertThat(test(p, pointArray(13))).isFalse();
    }

    @Test
    void testPointArrayRange_ExclusiveLowerInclusiveUpper() {
        RangePredicate<?> p = range(propId, pointArray(11), false, pointArray(13), true);

        assertThat(test(p, pointArray(11))).isFalse();
        assertThat(test(p, pointArray(12))).isFalse();
        assertThat(test(p, pointArray(13))).isFalse();
        assertThat(test(p, pointArray(14))).isFalse();
    }

    @Test
    void testPointArrayRange_LowerNullValue() {
        RangePredicate<?> p = range(propId, null, true, pointArray(13), true);

        assertThat(test(p, pointArray(10))).isFalse();
        assertThat(test(p, pointArray(11))).isFalse();
        assertThat(test(p, pointArray(12))).isFalse();
        assertThat(test(p, pointArray(13))).isTrue();
        assertThat(test(p, pointArray(14))).isFalse();
    }

    @Test
    void testPointArrayRange_UpperNullValue() {
        RangePredicate<?> p = range(propId, pointArray(11), true, null, true);

        assertThat(test(p, pointArray(10))).isFalse();
        assertThat(test(p, pointArray(11))).isTrue();
        assertThat(test(p, pointArray(12))).isFalse();
        assertThat(test(p, pointArray(13))).isFalse();
        assertThat(test(p, pointArray(14))).isFalse();
    }

    @Test
    void testPointArrayRange_EqualBounds() {
        RangePredicate<?> p = range(propId, pointArray(11), true, pointArray(11), true);

        assertThat(test(p, pointArray(10))).isFalse();
        assertThat(test(p, pointArray(11))).isTrue();
        assertThat(test(p, pointArray(12))).isFalse();
    }

    @Test
    void testPointArrayRange_EqualBoundsNotInclusive() {
        RangePredicate<?> fromInclusive = range(propId, pointArray(11), true, pointArray(11), false);

        assertThat(test(fromInclusive, pointArray(10))).isFalse();
        assertThat(test(fromInclusive, pointArray(11))).isFalse();
        assertThat(test(fromInclusive, pointArray(12))).isFalse();

        RangePredicate<?> toInclusive = range(propId, pointArray(11), false, pointArray(11), true);

        assertThat(test(toInclusive, pointArray(10))).isFalse();
        assertThat(test(toInclusive, pointArray(11))).isFalse();
        assertThat(test(toInclusive, pointArray(12))).isFalse();
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

        assertThat(test(p, gps1)).isFalse();
        assertThat(test(p, gps2)).isTrue();
        assertThat(test(p, gps5)).isTrue();
        assertThat(test(p, gps6)).isFalse();
        assertThat(test(p, gps7)).isFalse();
        assertThat(test(p, car1)).isFalse();
        assertThat(test(p, car2)).isFalse();
        assertThat(test(p, car3)).isFalse();
        assertThat(test(p, gps1_3d)).isFalse();
    }

    @Test
    void testBoundingBox_Cartesian3D() {
        BoundingBoxPredicate p = PropertyIndexQuery.boundingBox(propId, car3, car4);

        assertThat(test(p, gps1)).isFalse();
        assertThat(test(p, gps3)).isFalse();
        assertThat(test(p, gps5)).isFalse();
        assertThat(test(p, car1)).isFalse();
        assertThat(test(p, car2)).isFalse();
        assertThat(test(p, car3)).isTrue();
        assertThat(test(p, car4)).isTrue();
        assertThat(test(p, gps1_3d)).isFalse();
        assertThat(test(p, gps2_3d)).isFalse();
    }

    @Test
    void testBoundingBox_WGS84_3D() {
        BoundingBoxPredicate p = PropertyIndexQuery.boundingBox(propId, gps1_3d, gps2_3d);

        assertThat(test(p, gps1)).isFalse();
        assertThat(test(p, gps3)).isFalse();
        assertThat(test(p, gps5)).isFalse();
        assertThat(test(p, car1)).isFalse();
        assertThat(test(p, car2)).isFalse();
        assertThat(test(p, car3)).isFalse();
        assertThat(test(p, car4)).isFalse();
        assertThat(test(p, gps1_3d)).isTrue();
        assertThat(test(p, gps2_3d)).isTrue();
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

        assertThat(test(p, "doffington")).isFalse();
        assertThat(test(p, "doh, not this again!")).isFalse();
        assertThat(test(p, "dog")).isTrue();
        assertThat(test(p, "doggidog")).isTrue();
        assertThat(test(p, "doggidogdog")).isTrue();
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

        assertThat(test(p, "dog")).isFalse();
        assertThat(test(p, "cameraman")).isFalse();
        assertThat(test(p, "Cat")).isFalse();
        assertThat(test(p, "cat")).isTrue();
        assertThat(test(p, "bobcat")).isTrue();
        assertThat(test(p, "scatman")).isTrue();
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

        assertThat(test(p, "lesser being")).isFalse();
        assertThat(test(p, "make less noise please...")).isFalse();
        assertThat(test(p, "less")).isTrue();
        assertThat(test(p, "clueless")).isTrue();
        assertThat(test(p, "cluelessly clueless")).isTrue();
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
        assertThat(test(p, "other string")).isFalse();
        assertThat(test(p, "string1")).isFalse();
        assertThat(test(p, "")).isFalse();
        assertThat(test(p, -1)).isFalse();
        assertThat(test(p, -1.0)).isFalse();
        assertThat(test(p, false)).isFalse();
        assertThat(test(p, new long[] {-1L})).isFalse();
        assertThat(test(p, null)).isFalse();
    }

    private static boolean test(PropertyIndexQuery p, Object x) {
        return p.acceptsValue(x instanceof Value ? (Value) x : Values.of(x));
    }

    private static DurationValue secs(int seconds) {
        return Values.durationValue(Duration.ofSeconds(seconds));
    }

    private static Value secsArray(int secs) {
        var v = Values.durationValue(Duration.ofSeconds(secs));
        return Values.durationArray(new TemporalAmount[] {v, v});
    }

    private static PointValue point(int coordinate) {
        return Values.pointValue(CoordinateReferenceSystem.CARTESIAN, coordinate, coordinate);
    }

    private static PointArray pointArray(int coordinate) {
        var v = Values.pointValue(CoordinateReferenceSystem.CARTESIAN, coordinate, coordinate);
        return Values.pointArray(new PointValue[] {v, v});
    }
}
