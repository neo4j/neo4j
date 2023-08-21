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
package org.neo4j.values.storable;

import static java.time.ZoneOffset.UTC;
import static java.time.ZoneOffset.ofHours;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.MONTHS;
import static java.time.temporal.ChronoUnit.NANOS;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.internal.helpers.collection.Pair.pair;
import static org.neo4j.values.storable.DateTimeValue.datetime;
import static org.neo4j.values.storable.DateValue.date;
import static org.neo4j.values.storable.DurationValue.MAX_VALUE;
import static org.neo4j.values.storable.DurationValue.between;
import static org.neo4j.values.storable.DurationValue.duration;
import static org.neo4j.values.storable.DurationValue.durationBetween;
import static org.neo4j.values.storable.DurationValue.parse;
import static org.neo4j.values.storable.LocalTimeValue.localTime;
import static org.neo4j.values.storable.TimeValue.time;
import static org.neo4j.values.storable.Values.doubleValue;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.utils.AnyValueTestUtil.assertEqual;
import static org.neo4j.values.utils.AnyValueTestUtil.assertNotEqual;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.neo4j.exceptions.InvalidArgumentException;
import org.neo4j.exceptions.TemporalParseException;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.values.utils.TemporalUtil;

class DurationValueTest {
    @Test
    void shouldNormalizeNanoseconds() {
        // given
        DurationValue evenPos = duration(0, 0, 0, 1_000_000_000);
        DurationValue evenNeg = duration(0, 0, 0, -1_000_000_000);
        DurationValue pos = duration(0, 0, 0, 1_500_000_000);
        DurationValue neg = duration(0, 0, 0, -1_400_000_000);

        // then
        assertEquals(500_000_000, pos.get(NANOS), "+nanos");
        assertEquals(1, pos.get(SECONDS), "+seconds");
        assertEquals(600_000_000, neg.get(NANOS), "+nanos");
        assertEquals(-2, neg.get(SECONDS), "-seconds");

        assertEquals(0, evenPos.get(NANOS), "+nanos");
        assertEquals(1, evenPos.get(SECONDS), "+seconds");
        assertEquals(0, evenNeg.get(NANOS), "+nanos");
        assertEquals(-1, evenNeg.get(SECONDS), "-seconds");
    }

    @Test
    void shouldFormatDurationToString() {
        testDurationToString(1, 0, "PT1S");
        testDurationToString(-1, 0, "PT-1S");

        testDurationToString(59, -500_000_000, "PT58.5S");
        testDurationToString(59, 500_000_000, "PT59.5S");
        testDurationToString(60, -500_000_000, "PT59.5S");
        testDurationToString(60, 500_000_000, "PT1M0.5S");
        testDurationToString(61, -500_000_000, "PT1M0.5S");

        testDurationToString(-59, 500_000_000, "PT-58.5S");
        testDurationToString(-59, -500_000_000, "PT-59.5S");
        testDurationToString(-60, 500_000_000, "PT-59.5S");
        testDurationToString(-60, -500_000_000, "PT-1M-0.5S");
        testDurationToString(-61, 500_000_000, "PT-1M-0.5S");
        testDurationToString(-61, -500_000_000, "PT-1M-1.5S");

        testDurationToString(0, 5, "PT0.000000005S");
        testDurationToString(0, -5, "PT-0.000000005S");
        testDurationToString(0, 999_999_999, "PT0.999999999S");
        testDurationToString(0, -999_999_999, "PT-0.999999999S");

        testDurationToString(1, 5, "PT1.000000005S");
        testDurationToString(-1, -5, "PT-1.000000005S");
        testDurationToString(1, -5, "PT0.999999995S");
        testDurationToString(-1, 5, "PT-0.999999995S");
        testDurationToString(1, 999999999, "PT1.999999999S");
        testDurationToString(-1, -999999999, "PT-1.999999999S");
        testDurationToString(1, -999999999, "PT0.000000001S");
        testDurationToString(-1, 999999999, "PT-0.000000001S");

        testDurationToString(-78036, -143000000, "PT-21H-40M-36.143S");
    }

    private static void testDurationToString(long seconds, int nanos, String expectedValue) {
        assertEquals(expectedValue, duration(0, 0, seconds, nanos).prettyPrint());
    }

    @Test
    void shouldNormalizeSecondsAndNanos() {
        // given
        DurationValue pos = duration(0, 0, 5, -1_400_000_000);
        DurationValue neg = duration(0, 0, -5, 1_500_000_000);
        DurationValue x = duration(0, 0, 1, -1_400_000_000);

        DurationValue y = duration(0, 0, -59, -500_000_000);
        DurationValue y2 = duration(0, 0, -60, 500_000_000);

        // then
        assertEquals(600_000_000, pos.get(NANOS), "+nanos");
        assertEquals(3, pos.get(SECONDS), "+seconds");
        assertEquals(500_000_000, neg.get(NANOS), "+nanos");
        assertEquals(-4, neg.get(SECONDS), "-seconds");
        assertEquals(600_000_000, x.get(NANOS), "+nanos");
        assertEquals(-1, x.get(SECONDS), "-seconds");
        assertEquals(500_000_000, y.get(NANOS), "+nanos");
        assertEquals(-60, y.get(SECONDS), "-seconds");
        assertEquals(500_000_000, y2.get(NANOS), "+nanos");
        assertEquals(-60, y2.get(SECONDS), "-seconds");
    }

    @Test
    void shouldFormatAsPrettyString() {
        assertEquals("P1Y", prettyPrint(12, 0, 0, 0));
        assertEquals("P5M", prettyPrint(5, 0, 0, 0));
        assertEquals("P84D", prettyPrint(0, 84, 0, 0));
        assertEquals("P2Y4M11D", prettyPrint(28, 11, 0, 0));
        assertEquals("PT5S", prettyPrint(0, 0, 5, 0));
        assertEquals("PT30H22M8S", prettyPrint(0, 0, 109328, 0));
        assertEquals("PT7.123456789S", prettyPrint(0, 0, 7, 123_456_789));
        assertEquals("PT0.000000001S", prettyPrint(0, 0, 0, 1));
        assertEquals("PT0.1S", prettyPrint(0, 0, 0, 100_000_000));
        assertEquals("PT0S", prettyPrint(0, 0, 0, 0));
        assertEquals("PT1S", prettyPrint(0, 0, 0, 1_000_000_000));
        assertEquals("PT-1S", prettyPrint(0, 0, 0, -1_000_000_000));
        assertEquals("PT1.5S", prettyPrint(0, 0, 1, 500_000_000));
        assertEquals("PT-1.4S", prettyPrint(0, 0, -1, -400_000_000));
    }

    private static String prettyPrint(long months, long days, long seconds, int nanos) {
        return duration(months, days, seconds, nanos).prettyPrint();
    }

    @Test
    void shouldHandleLargeNanos() {
        DurationValue duration = DurationValue.duration(0L, 0L, 0L, Long.MAX_VALUE);
        assertEquals(Long.MAX_VALUE, duration.get("nanoseconds").value());
    }

    @Test
    void shouldParseDuration() {
        assertEquals(duration(14, 25, 18367, 800_000_000), parse("+P1Y2M3W4DT5H6M7.8S"));

        assertEquals(duration(0, 0, 0, -100000000), parse("PT-0.1S"));
        assertEquals(duration(0, 0, 0, -20000000), parse("PT-0.02S"));
        assertEquals(duration(0, 0, 0, -3000000), parse("PT-0.003S"));
        assertEquals(duration(0, 0, 0, -400000), parse("PT-0.0004S"));
        assertEquals(duration(0, 0, 0, -50000), parse("PT-0.00005S"));
        assertEquals(duration(0, 0, 0, -6000), parse("PT-0.000006S"));
        assertEquals(duration(0, 0, 0, -700), parse("PT-0.0000007S"));
        assertEquals(duration(0, 0, 0, -80), parse("PT-0.00000008S"));
        assertEquals(duration(0, 0, 0, -9), parse("PT-0.000000009S"));

        assertEquals(duration(0, 0, 0, 900_000_000), parse("PT0.900000000S"));
        assertEquals(duration(0, 0, 0, 800_000_000), parse("PT0.80000000S"));
        assertEquals(duration(0, 0, 0, 700_000_000), parse("PT0.7000000S"));
        assertEquals(duration(0, 0, 0, 600_000_000), parse("PT0.600000S"));
        assertEquals(duration(0, 0, 0, 500_000_000), parse("PT0.50000S"));
        assertEquals(duration(0, 0, 0, 400_000_000), parse("PT0.4000S"));
        assertEquals(duration(0, 0, 0, 300_000_000), parse("PT0.300S"));
        assertEquals(duration(0, 0, 0, 200_000_000), parse("PT0.20S"));
        assertEquals(duration(0, 0, 0, 100_000_000), parse("PT0.1S"));

        assertParsesOne("P", "Y", 12, 0, 0);
        assertParsesOne("P", "M", 1, 0, 0);
        assertParsesOne("P", "W", 0, 7, 0);
        assertParsesOne("P", "D", 0, 1, 0);
        assertParsesOne("PT", "H", 0, 0, 3600);
        assertParsesOne("PT", "M", 0, 0, 60);
        assertParsesOne("PT", "S", 0, 0, 1);

        assertEquals(duration(0, 0, -1, -100_000_000), parse("PT-1,1S"));

        assertEquals(duration(10, 0, 0, 0), parse("P1Y-2M"));
        assertEquals(duration(0, 20, 0, 0), parse("P3W-1D"));
        assertEquals(duration(0, 0, 3000, 0), parse("PT1H-10M"));
        assertEquals(duration(0, 0, 3000, 0), parse("PT1H-600S"));
        assertEquals(duration(0, 0, 50, 0), parse("PT1M-10S"));
    }

    private static void assertParsesOne(String prefix, String suffix, int months, int days, int seconds) {
        assertEquals(duration(months, days, seconds, 0), parse(prefix + "1" + suffix));
        assertEquals(duration(months, days, seconds, 0), parse("+" + prefix + "1" + suffix));
        assertEquals(duration(months, days, seconds, 0), parse(prefix + "+1" + suffix));
        assertEquals(duration(months, days, seconds, 0), parse("+" + prefix + "+1" + suffix));

        assertEquals(duration(-months, -days, -seconds, 0), parse("-" + prefix + "1" + suffix));
        assertEquals(duration(-months, -days, -seconds, 0), parse(prefix + "-1" + suffix));
        assertEquals(duration(-months, -days, -seconds, 0), parse("+" + prefix + "-1" + suffix));
        assertEquals(duration(-months, -days, -seconds, 0), parse("-" + prefix + "+1" + suffix));

        assertEquals(duration(months, days, seconds, 0), parse("-" + prefix + "-1" + suffix));
    }

    @Test
    void shouldParseDateBasedDuration() {
        assertEquals(duration(14, 17, 45252, 123400000), parse("P0001-02-17T12:34:12.1234"));
        assertEquals(duration(14, 17, 45252, 123400000), parse("P00010217T123412.1234"));
    }

    @Test
    void shouldParseNegativeDuration() {
        assertEquals(duration(-12, 0, 0, 0), parse("-P1Y"));
        assertEquals(duration(12, 0, 0, 0), parse("-P-1Y"));
        assertEquals(duration(6, 0, 0, 0), parse("-P-6M"));

        /*
         * -(1 year, -2 months, 3 days, -4h, 5m, -6s) =
         * -1 year, 2 months, -3 days, 4h, -5m, 6s =
         * (-12 + 2) months, -3 days, (4*60*60 - 5*60 + 6) s =
         * -10 months, -3 days, 14106s
         */
        assertEquals(duration(-10, -3, 14106, 0), parse("-P1Y-2M3DT-4H5M-6S"));
    }

    @Test
    void shouldParseNegativeDurationWithFractionalComponent() {
        assertEquals(duration(-6, 0, 0, 0), parse("-P0.5Y"));
        assertEquals(duration(6, 0, 0, 0), parse("-P-0.5Y"));

        assertEquals(parse("P0.5M"), parse("-P-0.5M"));
        assertEquals(parse("P1Y-0.5M"), parse("-P-1Y0.5M"));

        // 1.5 weeks = 10.5 days = 10 days 12h = 10 days 12*60*60s = 10 days 43200s
        assertEquals(duration(0, -10, -43200, 0), parse("-P1.5W"));
        assertEquals(duration(10, -10, -43200, 0), parse("-P-1Y2M1.5W"));

        // 0.1 days = 2.4h = 2.4*60*60s = 8640s
        assertEquals(duration(0, -3, -8640, 0), parse("-P3.1D"));
        assertEquals(duration(-5, 3, 8640, 0), parse("-P5M-3.1D"));

        // 0.2h = 12min = 12*60s = 720s
        assertEquals(duration(0, 0, 720, 0), parse("-PT-0.2H"));
        assertEquals(duration(0, 3, -720, 0), parse("-P-3DT0.2H"));

        assertEquals(duration(0, 0, -66, 0), parse("-PT1.1M"));
        assertEquals(duration(0, -1, 66, 0), parse("-P1DT-1.1M"));

        assertEquals(duration(0, 0, 6, 550000000), parse("-PT-6.55S"));
    }

    @Test
    void shouldParseDurationWithFractionalComponentConversion() {
        // years + months => months
        assertEquals(parse("P14.2M"), parse("P1Y2.2M"));

        // weeks + days => days
        // 0.5 days = 12h = 12*60*60s = 43200s
        assertEquals(duration(0, 16, 43200, 0), parse("P2W2.5D"));

        // hours + minutes => seconds
        // 1 h 72.5 min = 1*60*60 + 72*60 + 30 s = 7950s
        assertEquals(duration(0, 0, 7950, 0), parse("PT1H72.5M"));

        // hours + minutes + seconds => seconds
        // 1 h 72 min 72.5 s = 1*60*60 + 72*60 + 72.5 s = 7992.5s
        assertEquals(duration(0, 0, 7992, 500000000), parse("PT1H72M72.5S"));

        // minutes + seconds => seconds
        // 2 min 72.5 s = 2*60 + 72.5 s = 192.5s
        assertEquals(duration(0, 0, 192, 500000000), parse("PT2M72.5S"));
    }

    @Test
    void shouldNotParseInvalidDurationStrings() {
        assertThrows(TemporalParseException.class, () -> parse(""));
        assertThrows(TemporalParseException.class, () -> parse("P"));
        assertThrows(TemporalParseException.class, () -> parse("PT"));
        assertThrows(TemporalParseException.class, () -> parse("PT.S"));
        assertThrows(TemporalParseException.class, () -> parse("PT,S"));
        assertThrows(TemporalParseException.class, () -> parse("PT.0S"));
        assertThrows(TemporalParseException.class, () -> parse("PT,0S"));
        assertThrows(TemporalParseException.class, () -> parse("PT0.S"));
        assertThrows(TemporalParseException.class, () -> parse("PT0,S"));
        assertThrows(TemporalParseException.class, () -> parse("PT1,-1S"));
        assertThrows(TemporalParseException.class, () -> parse("PT1.-1S"));
        for (String s : new String[] {"Y", "M", "W", "D"}) {
            assertThrows(TemporalParseException.class, () -> parse("P-" + s));
            assertThrows(TemporalParseException.class, () -> parse("P1" + s + "T"));
        }
        for (String s : new String[] {"H", "M", "S"}) {
            assertThrows(TemporalParseException.class, () -> parse("PT-" + s));
            assertThrows(TemporalParseException.class, () -> parse("T1" + s));
        }
    }

    @Test
    void shouldWriteDuration() {
        // given
        for (DurationValue duration : new DurationValue[] {
            duration(0, 0, 0, 0),
            duration(1, 0, 0, 0),
            duration(0, 1, 0, 0),
            duration(0, 0, 1, 0),
            duration(0, 0, 0, 1),
        }) {
            List<DurationValue> values = new ArrayList<>(1);
            ValueWriter<RuntimeException> writer = new ThrowingValueWriter.AssertOnly() {
                @Override
                public void writeDuration(long months, long days, long seconds, int nanos) {
                    values.add(duration(months, days, seconds, nanos));
                }
            };

            // when
            duration.writeTo(writer);

            // then
            assertEquals(singletonList(duration), values);
        }
    }

    @Test
    void shouldAddToLocalDate() {
        assertEquals(LocalDate.of(2017, 12, 5), LocalDate.of(2017, 12, 4).plus(parse("PT24H")), "seconds");
        assertEquals(LocalDate.of(2017, 12, 3), LocalDate.of(2017, 12, 4).minus(parse("PT24H")), "seconds");
        assertEquals(LocalDate.of(2017, 12, 4), LocalDate.of(2017, 12, 4).plus(parse("PT24H-1S")), "seconds");
        assertEquals(LocalDate.of(2017, 12, 4), LocalDate.of(2017, 12, 4).minus(parse("PT24H-1S")), "seconds");
        assertEquals(LocalDate.of(2017, 12, 5), LocalDate.of(2017, 12, 4).plus(parse("P1D")), "days");
        assertEquals(LocalDate.of(2017, 12, 3), LocalDate.of(2017, 12, 4).minus(parse("P1D")), "days");
    }

    @Test
    void shouldHaveSensibleHashCode() {
        assertEquals(0, duration(0, 0, 0, 0).hashCode());

        assertNotEquals(duration(0, 0, 0, 1).hashCode(), duration(0, 0, 0, 2).hashCode());
        assertNotEquals(duration(0, 0, 0, 1).hashCode(), duration(0, 0, 1, 0).hashCode());
        assertNotEquals(duration(0, 0, 0, 1).hashCode(), duration(0, 1, 0, 0).hashCode());
        assertNotEquals(duration(0, 0, 0, 1).hashCode(), duration(1, 0, 0, 0).hashCode());

        assertNotEquals(duration(0, 0, 1, 0).hashCode(), duration(0, 0, 2, 0).hashCode());
        assertNotEquals(duration(0, 0, 1, 0).hashCode(), duration(0, 0, 0, 1).hashCode());
        assertNotEquals(duration(0, 0, 1, 0).hashCode(), duration(0, 1, 0, 0).hashCode());
        assertNotEquals(duration(0, 0, 1, 0).hashCode(), duration(1, 0, 0, 0).hashCode());

        assertNotEquals(duration(0, 1, 0, 0).hashCode(), duration(0, 2, 0, 0).hashCode());
        assertNotEquals(duration(0, 1, 0, 0).hashCode(), duration(0, 0, 0, 1).hashCode());
        assertNotEquals(duration(0, 1, 0, 0).hashCode(), duration(0, 0, 1, 0).hashCode());
        assertNotEquals(duration(0, 1, 0, 0).hashCode(), duration(1, 0, 0, 0).hashCode());

        assertNotEquals(duration(1, 0, 0, 0).hashCode(), duration(2, 0, 0, 0).hashCode());
        assertNotEquals(duration(1, 0, 0, 0).hashCode(), duration(0, 0, 0, 1).hashCode());
        assertNotEquals(duration(1, 0, 0, 0).hashCode(), duration(0, 0, 1, 0).hashCode());
        assertNotEquals(duration(1, 0, 0, 0).hashCode(), duration(0, 1, 0, 0).hashCode());
    }

    @Test
    void shouldThrowExceptionOnAddOverflow() {
        DurationValue duration1 = duration(0, 0, Long.MAX_VALUE, 500_000_000);
        DurationValue duration2 = duration(0, 0, 1, 0);
        DurationValue duration3 = duration(0, 0, 0, 500_000_000);
        assertThrows(InvalidArgumentException.class, () -> duration1.add(duration2));
        assertThrows(InvalidArgumentException.class, () -> duration1.add(duration3));
    }

    @Test
    void shouldThrowExceptionOnSubtractOverflow() {
        DurationValue duration1 = duration(0, 0, Long.MIN_VALUE, 0);
        DurationValue duration2 = duration(0, 0, 1, 0);
        assertThrows(InvalidArgumentException.class, () -> duration1.sub(duration2));
    }

    @Test
    void shouldThrowExceptionOnMultiplyOverflow() {
        DurationValue duration = duration(0, 0, Long.MAX_VALUE, 0);
        assertThrows(InvalidArgumentException.class, () -> duration.mul(Values.intValue(2)));
        assertThrows(InvalidArgumentException.class, () -> duration.mul(Values.floatValue(2)));
    }

    @Test
    void shouldThrowExceptionOnDivideOverflow() {
        DurationValue duration = duration(0, 0, Long.MAX_VALUE, 0);
        assertThrows(InvalidArgumentException.class, () -> duration.div(Values.floatValue(0.5f)));
    }

    @Test
    void shouldMultiplyDurationByInteger() {
        assertEquals(duration(2, 0, 0, 0), duration(1, 0, 0, 0).mul(longValue(2)));
        assertEquals(duration(0, 2, 0, 0), duration(0, 1, 0, 0).mul(longValue(2)));
        assertEquals(duration(0, 0, 2, 0), duration(0, 0, 1, 0).mul(longValue(2)));
        assertEquals(duration(0, 0, 0, 2), duration(0, 0, 0, 1).mul(longValue(2)));

        assertEquals(duration(0, 40, 0, 0), duration(0, 20, 0, 0).mul(longValue(2)));
        assertEquals(duration(0, 0, 100_000, 0), duration(0, 0, 50_000, 0).mul(longValue(2)));
        assertEquals(duration(0, 0, 1, 0), duration(0, 0, 0, 500_000_000).mul(longValue(2)));
    }

    @Test
    void shouldMultiplyDurationByFloat() {
        assertEquals(duration(0, 0, 0, 500_000_000), duration(0, 0, 1, 0).mul(doubleValue(0.5)));
        assertEquals(duration(0, 0, 43200, 0), duration(0, 1, 0, 0).mul(doubleValue(0.5)));
        assertEquals(duration(0, 15, 18873, 0), duration(1, 0, 0, 0).mul(doubleValue(0.5)));
    }

    @Test
    void shouldDivideDuration() {
        assertEquals(duration(0, 0, 0, 500_000_000), duration(0, 0, 1, 0).div(longValue(2)));
        assertEquals(duration(0, 0, 43200, 0), duration(0, 1, 0, 0).div(longValue(2)));
        assertEquals(duration(0, 15, 18873, 0), duration(1, 0, 0, 0).div(longValue(2)));
    }

    @Test
    void shouldComputeDurationBetweenDates() {
        assertEquals(duration(22, 23, 0, 0), durationBetween(date(2016, 1, 27), date(2017, 12, 20)));
        assertEquals(duration(0, 693, 0, 0), between(DAYS, date(2016, 1, 27), date(2017, 12, 20)));
        assertEquals(duration(22, 0, 0, 0), between(MONTHS, date(2016, 1, 27), date(2017, 12, 20)));
        assertEquals(duration(0, 0, 24 * 60 * 60, 0), between(SECONDS, date(2016, 1, 27), date(2016, 1, 28)));
    }

    @Test
    void shouldComputeDurationBetweenLocalTimes() {
        assertEquals(duration(0, 0, 10623, 0), durationBetween(localTime(11, 30, 52, 0), localTime(14, 27, 55, 0)));
        assertEquals(duration(0, 0, 10623, 0), between(SECONDS, localTime(11, 30, 52, 0), localTime(14, 27, 55, 0)));
    }

    @Test
    void shouldComputeDurationBetweenTimes() {
        assertEquals(
                duration(0, 0, 140223, 0),
                durationBetween(time(11, 30, 52, 0, ofHours(18)), time(14, 27, 55, 0, ofHours(-18))));
        assertEquals(duration(0, 0, 10623, 0), between(SECONDS, time(11, 30, 52, 0, UTC), time(14, 27, 55, 0, UTC)));

        assertEquals(duration(0, 0, 10623, 0), durationBetween(time(11, 30, 52, 0, UTC), localTime(14, 27, 55, 0)));
        assertEquals(
                duration(0, 0, 10623, 0), durationBetween(time(11, 30, 52, 0, ofHours(17)), localTime(14, 27, 55, 0)));
        assertEquals(duration(0, 0, -10623, 0), durationBetween(localTime(14, 27, 55, 0), time(11, 30, 52, 0, UTC)));
        assertEquals(
                duration(0, 0, -10623, 0), durationBetween(localTime(14, 27, 55, 0), time(11, 30, 52, 0, ofHours(17))));
    }

    @Test
    void shouldComputeDurationBetweenDateAndTime() {
        assertEquals(parse("PT14H32M11S"), durationBetween(date(2017, 12, 21), localTime(14, 32, 11, 0)));
        assertEquals(parse("PT14H32M11S"), between(SECONDS, date(2017, 12, 21), localTime(14, 32, 11, 0)));
        assertEquals(parse("P0D"), between(DAYS, date(2017, 12, 21), localTime(14, 32, 11, 0)));
        assertEquals(parse("-PT14H32M11S"), durationBetween(localTime(14, 32, 11, 0), date(2017, 12, 21)));
        assertEquals(parse("-PT14H32M11S"), between(SECONDS, localTime(14, 32, 11, 0), date(2017, 12, 21)));
        assertEquals(parse("P0D"), between(DAYS, localTime(14, 32, 11, 0), date(2017, 12, 21)));
        assertEquals(parse("PT14H32M11S"), durationBetween(date(2017, 12, 21), time(14, 32, 11, 0, UTC)));
        assertEquals(parse("-PT14H32M11S"), durationBetween(time(14, 32, 11, 0, UTC), date(2017, 12, 21)));
        assertEquals(parse("PT14H32M11S"), durationBetween(date(2017, 12, 21), time(14, 32, 11, 0, ofHours(-12))));
        assertEquals(parse("-PT14H32M11S"), durationBetween(time(14, 32, 11, 0, ofHours(-12)), date(2017, 12, 21)));
    }

    @Test
    void shouldComputeDurationBetweenDateTimeAndTime() {
        assertEquals(
                parse("PT8H-20M"),
                durationBetween(datetime(date(2017, 12, 21), time(6, 52, 11, 0, UTC)), localTime(14, 32, 11, 0)));
        assertEquals(
                parse("PT-8H+20M"),
                durationBetween(localTime(14, 32, 11, 0), datetime(date(2017, 12, 21), time(6, 52, 11, 0, UTC))));

        assertEquals(parse("-PT14H32M11S"), durationBetween(localTime(14, 32, 11, 0), date(2017, 12, 21)));
        assertEquals(parse("PT14H32M11S"), durationBetween(date(2017, 12, 21), time(14, 32, 11, 0, UTC)));
        assertEquals(parse("-PT14H32M11S"), durationBetween(time(14, 32, 11, 0, UTC), date(2017, 12, 21)));
        assertEquals(parse("PT14H32M11S"), durationBetween(date(2017, 12, 21), time(14, 32, 11, 0, ofHours(-12))));
        assertEquals(parse("-PT14H32M11S"), durationBetween(time(14, 32, 11, 0, ofHours(-12)), date(2017, 12, 21)));
    }

    @Test
    void shouldComputeDurationBetweenDateTimeAndDateTime() {
        assertEquals(
                parse("PT1H"),
                durationBetween(
                        datetime(date(2017, 12, 21), time(6, 52, 11, 0, UTC)),
                        datetime(date(2017, 12, 21), time(7, 52, 11, 0, UTC))));
        assertEquals(
                parse("P1D"),
                durationBetween(
                        datetime(date(2017, 12, 21), time(6, 52, 11, 0, UTC)),
                        datetime(date(2017, 12, 22), time(6, 52, 11, 0, UTC))));
        assertEquals(
                parse("P1DT1H"),
                durationBetween(
                        datetime(date(2017, 12, 21), time(6, 52, 11, 0, UTC)),
                        datetime(date(2017, 12, 22), time(7, 52, 11, 0, UTC))));
    }

    @Test
    void shouldGetSameInstantWhenAddingDurationBetweenToInstant() {
        // given
        @SuppressWarnings("unchecked")
        Pair<Temporal, Temporal>[] input = new Pair[] {
            pair( // change from CET to CEST - second time of day after first
                    datetime(date(2017, 3, 20), localTime(13, 37, 0, 0), ZoneId.of("Europe/Stockholm")),
                    datetime(date(2017, 3, 26), localTime(19, 40, 0, 0), ZoneId.of("Europe/Stockholm"))),
            pair( // change from CET to CEST - second time of day before first
                    datetime(date(2017, 3, 20), localTime(13, 37, 0, 0), ZoneId.of("Europe/Stockholm")),
                    datetime(date(2017, 3, 26), localTime(11, 40, 0, 0), ZoneId.of("Europe/Stockholm"))),
            pair( // change from CEST to CET - second time of day after first
                    datetime(date(2017, 10, 20), localTime(13, 37, 0, 0), ZoneId.of("Europe/Stockholm")),
                    datetime(date(2017, 10, 29), localTime(19, 40, 0, 0), ZoneId.of("Europe/Stockholm"))),
            pair( // change from CEST to CET - second time of day before first
                    datetime(date(2017, 10, 20), localTime(13, 37, 0, 0), ZoneId.of("Europe/Stockholm")),
                    datetime(date(2017, 10, 29), localTime(11, 40, 0, 0), ZoneId.of("Europe/Stockholm"))),
        };
        for (Pair<Temporal, Temporal> pair : input) {
            Temporal a = pair.first(), b = pair.other();

            // when
            DurationValue diffAB = durationBetween(a, b);
            DurationValue diffBA = durationBetween(b, a);
            DurationValue diffABs = between(SECONDS, a, b);
            DurationValue diffBAs = between(SECONDS, b, a);

            // then
            assertEquals(b, a.plus(diffAB), diffAB.prettyPrint());
            assertEquals(a, b.plus(diffBA), diffBA.prettyPrint());
            assertEquals(b, a.plus(diffABs), diffABs.prettyPrint());
            assertEquals(a, b.plus(diffBAs), diffBAs.prettyPrint());
        }
    }

    @Test
    void shouldEqualItself() {
        assertEqual(duration(40, 3, 13, 37), duration(40, 3, 13, 37));
        assertEqual(duration(40, 3, 14, 37), duration(40, 3, 13, 1_000_000_037));
    }

    @Test
    void shouldNotEqualOther() {
        assertNotEqual(duration(40, 3, 13, 37), duration(40, 3, 14, 37));

        // average nbr of seconds on a month doesn't imply equality
        assertNotEqual(duration(1, 0, 0, 0), duration(0, 0, 2_629_800, 0));

        // not the same due to leap seconds
        assertNotEqual(duration(0, 1, 0, 0), duration(0, 0, 60 * 60 * 24, 0));

        // average nbr of days in 400 years doesn't imply equality
        assertNotEqual(duration(400 * 12, 0, 0, 0), duration(0, 146_097, 0, 0));
    }

    @Test
    public void shouldApproximateFractionalMonth() {
        DurationValue result = DurationValue.approximate(10.8, 0, 0, 0);
        assertEqual(result, DurationValue.duration(10, 24, 30196, 800000001));
    }

    @Test
    public void shouldApproximateFractionalMonthFromNegativeDuration() {
        DurationValue result = DurationValue.approximate(10.8, 0, 0, 0, -1);
        assertEqual(result, DurationValue.duration(-10, -24, -30196, -800000001));
    }

    @Test
    public void shouldApproximateWithoutAccumulatedRoundingErrors() {
        double months = 1.9013243104086859E-16; // 0.5 ns
        double nanos =
                0.6; // with 1.1 ns we should be on the safe side to get rounded to 1 ns, even with rounding errors
        DurationValue result = DurationValue.approximate(months, 0, 0, nanos);
        assertEqual(result, DurationValue.duration(0, 0, 0, 1));
    }

    @Test
    public void shouldNotOverflowOnBiggerValues() {
        assertEqual(DurationValue.approximate(293 * 12, 0, 0, 0), DurationValue.duration(293 * 12, 0, 0, 0));
        assertEqual(DurationValue.approximate(0, 106752, 0, 0), DurationValue.duration(0, 106752, 0, 0));
        assertEqual(DurationValue.approximate(0, 0, 9223372037L, 0), DurationValue.duration(0, 0, 9223372037L, 0));
    }

    @Test
    void shouldNotThrowWhenInsideOverflowLimit() {
        // when
        duration(0, 0, Long.MAX_VALUE, 999_999_999);

        // then should not throw
    }

    @Test
    void shouldThrowOnOverflowOnNanos() {
        // when
        int nanos = 1_000_000_000;
        long seconds = Long.MAX_VALUE;
        assertConstructorThrows(0, 0, seconds, nanos);
    }

    @Test
    void shouldThrowOnOverflowFromDaysSecondsNanosCombo() {
        // when
        assertConstructorThrows(0, 1, Long.MAX_VALUE - TemporalUtil.SECONDS_PER_DAY, TemporalUtil.NANOS_PER_SECOND);
        assertConstructorThrows(
                0, -1, Long.MIN_VALUE + TemporalUtil.SECONDS_PER_DAY, -2 * TemporalUtil.NANOS_PER_SECOND);
        assertConstructorThrows(0, 0, Long.MIN_VALUE, -1L);
        assertConstructorThrows(0, 1, Long.MIN_VALUE, -10 * TemporalUtil.NANOS_PER_SECOND);
        assertConstructorThrows(
                0, 1, Long.MIN_VALUE, -TemporalUtil.SECONDS_PER_DAY * TemporalUtil.NANOS_PER_SECOND - 1);
    }

    @Test
    void shouldThrowOnNegativeOverflowOnNanos() {
        // when
        int nanos = -1_000_000_000;
        long seconds = Long.MIN_VALUE;
        assertConstructorThrows(0, 0, seconds, nanos);
    }

    @Test
    void shouldThrowOnOverflowOnDays() {
        // when
        long days = Long.MAX_VALUE / TemporalUtil.SECONDS_PER_DAY;
        long seconds = Long.MAX_VALUE - days * TemporalUtil.SECONDS_PER_DAY;
        assertConstructorThrows(0, days, seconds + 1, 0);
    }

    @Test
    void shouldThrowOnNegativeOverflowOnDays() {
        // when
        long days = Long.MIN_VALUE / TemporalUtil.SECONDS_PER_DAY;
        long seconds = Long.MIN_VALUE - days * TemporalUtil.SECONDS_PER_DAY;
        assertConstructorThrows(0, days, seconds - 1, 0);
    }

    @Test
    void shouldThrowOnOverflowOnMonths() {
        // when
        long months = Long.MAX_VALUE / TemporalUtil.AVG_SECONDS_PER_MONTH;
        long seconds = Long.MAX_VALUE - months * TemporalUtil.AVG_SECONDS_PER_MONTH;
        assertConstructorThrows(months, 0, seconds + 1, 0);
    }

    @Test
    void shouldThrowOnNegativeOverflowOnMonths() {
        // when
        long months = Long.MIN_VALUE / TemporalUtil.AVG_SECONDS_PER_MONTH;
        long seconds = Long.MIN_VALUE - months * TemporalUtil.AVG_SECONDS_PER_MONTH;
        assertConstructorThrows(months, 0, seconds - 1, 0);
    }

    @Test
    void shouldThrowOnParsingYearsOverflow() {
        long years = Long.MAX_VALUE;
        InvalidArgumentException e =
                assertThrows(InvalidArgumentException.class, () -> DurationValue.parse("P" + years + "Y"));
        assertThat(e.getMessage()).contains("Invalid value for duration").contains("years=" + years);
    }

    @Test
    void shouldThrowOnParsingYearAndMonthOverflow() {
        long years = 1;
        long months = Long.MAX_VALUE;
        InvalidArgumentException e = assertThrows(
                InvalidArgumentException.class, () -> DurationValue.parse("P" + years + "Y" + months + "M"));
        assertThat(e.getMessage())
                .contains("Invalid value for duration")
                .contains("years=" + years)
                .contains("months=" + months);
    }

    @Test
    void shouldThrowOnParsingWeeksOverflow() {
        long weeks = Long.MAX_VALUE;
        InvalidArgumentException e =
                assertThrows(InvalidArgumentException.class, () -> DurationValue.parse("P" + weeks + "W"));
        assertThat(e.getMessage()).contains("Invalid value for duration").contains("weeks=" + weeks);
    }

    @Test
    void shouldThrowOnParsingWeeksAndDaysOverflow() {
        long weeks = 1;
        long days = Long.MAX_VALUE;
        InvalidArgumentException e =
                assertThrows(InvalidArgumentException.class, () -> DurationValue.parse("P" + weeks + "W" + days + "D"));
        assertThat(e.getMessage())
                .contains("Invalid value for duration")
                .contains("weeks=" + weeks)
                .contains("days=" + days);
    }

    @Test
    void shouldThrowOnParsingHoursOverflow() {
        long hours = Long.MAX_VALUE;
        InvalidArgumentException e =
                assertThrows(InvalidArgumentException.class, () -> DurationValue.parse("PT" + hours + "H"));
        assertThat(e.getMessage()).contains("Invalid value for duration").contains("hours=" + hours);
    }

    @Test
    void shouldThrowOnParsingHoursAndSecondsOverflow() {
        long hours = 1;
        long seconds = Long.MAX_VALUE;
        InvalidArgumentException e = assertThrows(
                InvalidArgumentException.class, () -> DurationValue.parse("PT" + hours + "H" + seconds + "S"));
        assertThat(e.getMessage())
                .contains("Invalid value for duration")
                .contains("hours=" + hours)
                .contains("seconds=" + seconds);
    }

    @Test
    void shouldThrowOnMinutesOverflow() {
        long minutes = Long.MAX_VALUE;
        InvalidArgumentException e =
                assertThrows(InvalidArgumentException.class, () -> DurationValue.parse("PT" + minutes + "M"));
        assertThat(e.getMessage()).contains("Invalid value for duration").contains("minutes=" + minutes);
    }

    @Test
    void shouldParseMaxNumberOfSeconds() {
        DurationValue value = DurationValue.parse("PT" + Long.MAX_VALUE + ".999999999S");
        assertEquals(MAX_VALUE, value);
    }

    @Test
    void shouldParseFractions() {
        assertEquals(DurationValue.duration(6, 0, 0, 0), DurationValue.parse("P0.5Y"));
        assertEquals(DurationValue.duration(1, 15, 18873, 0), DurationValue.parse("P1.5M"));
        assertEquals(DurationValue.duration(0, 17, 43200, 0), DurationValue.parse("P2.5W"));
        assertEquals(DurationValue.duration(0, 3, 43200, 0), DurationValue.parse("P3.5D"));
        assertEquals(DurationValue.duration(16, 15, 18873, 0), DurationValue.parse("P1Y4.5M"));
        assertEquals(DurationValue.duration(13, 38, 43200, 0), DurationValue.parse("P1Y1M5.5W"));
        assertEquals(DurationValue.duration(13, 6, 43200, 0), DurationValue.parse("P1Y1M6.5D"));
    }

    private static void assertConstructorThrows(long months, long days, long seconds, long nanos) {
        InvalidArgumentException e =
                assertThrows(InvalidArgumentException.class, () -> duration(months, days, seconds, nanos));

        assertThat(e.getMessage())
                .contains("Invalid value for duration")
                .contains("months=" + months)
                .contains("days=" + days)
                .contains("seconds=" + seconds)
                .contains("nanos=" + nanos);
    }
}
