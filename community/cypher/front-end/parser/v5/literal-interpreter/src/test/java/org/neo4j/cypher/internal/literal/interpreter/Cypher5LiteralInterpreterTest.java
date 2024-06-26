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
package org.neo4j.cypher.internal.literal.interpreter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.cypher.internal.literal.interpreter.Cypher5LiteralInterpreter.parseExpression;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.util.Maps;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.exceptions.SyntaxException;
import org.neo4j.exceptions.UnsupportedTemporalUnitException;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TimeValue;

@ExtendWith(RandomExtension.class)
public class Cypher5LiteralInterpreterTest {
    public static final ZoneId DEFAULT_ZONE_ID = ZoneId.systemDefault();

    @Inject
    RandomSupport rand;

    @Test
    void testLiteralZero() {
        assertEquals(0L, parseExpression("0"));
    }

    @Test
    void randomLong() {
        var val = rand.nextLong();
        assertEquals(val, parseExpression(String.valueOf(val)));
    }

    @Test
    void shouldInterpretNumbers() {

        assertEquals(0L, parseExpression("0"));
        assertEquals(12345L, parseExpression("12345"));
        assertEquals(-12345L, parseExpression("-12345"));
        assertEquals(Long.MAX_VALUE, parseExpression(Long.toString(Long.MAX_VALUE)));
        assertEquals(Long.MIN_VALUE, parseExpression(Long.toString(Long.MIN_VALUE)));

        // old syntax
        assertEquals(8L, parseExpression("010"));
        assertEquals(-8L, parseExpression("-010"));
        assertEquals(
                Long.MIN_VALUE,
                parseExpression("-0" + Long.toString(Long.MIN_VALUE, 8).substring(1)));

        assertEquals(8L, parseExpression("0o10"));
        assertEquals(-8L, parseExpression("-0o10"));
        assertEquals(
                Long.MIN_VALUE,
                parseExpression("-0o" + Long.toString(Long.MIN_VALUE, 8).substring(1)));

        assertEquals(255L, parseExpression("0xff"));
        assertEquals(-255L, parseExpression("-0xff"));
        assertEquals(
                Long.MIN_VALUE,
                parseExpression("-0x" + Long.toString(Long.MIN_VALUE, 16).substring(1)));

        assertEquals(0.0d, parseExpression("0.0"));
        assertEquals(0.0d, parseExpression("0.0e0"));
        assertEquals(-0.0d, parseExpression("-0.0e0"));
        assertEquals(1.0d, parseExpression("1.0e0"));
        assertEquals(98723.0e31d, parseExpression("98723.0e31"));
        assertEquals(Double.MAX_VALUE, parseExpression(Double.toString(Double.MAX_VALUE)));
        assertEquals(Double.MIN_VALUE, parseExpression(Double.toString(Double.MIN_VALUE)));
    }

    @Test
    void shouldInterpretString() {
        assertEquals("a string", parseExpression("\"a string\""));
        assertEquals("ÅÄü", parseExpression("'ÅÄü'"));
        assertEquals("Ελληνικά", parseExpression("\"Ελληνικά\""));
        assertEquals("\uD83D\uDCA9", parseExpression("'\uD83D\uDCA9'"));
    }

    @Test
    void shouldInterpretNull() {
        assertNull(parseExpression("null"));
    }

    @Test
    void shouldHandleNullMap() {
        assertEquals(Maps.newHashMap("hello", null), parseExpression("{hello:null}"));
    }

    @Test
    void shouldInterpretBoolean() {
        assertEquals(true, parseExpression("true"));
        assertEquals(false, parseExpression("false"));
    }

    @Test
    void shouldInterpretInfinity() {
        assertEquals(Double.POSITIVE_INFINITY, parseExpression("Infinity"));
    }

    @Test
    void shouldInterpretNaN() {
        assertEquals(Double.NaN, parseExpression("NaN"));
    }

    @Test
    void shouldInterpretList() {
        assertEquals(List.of(1L, 2L, 3L), parseExpression("[1, 2, 3]"));
    }

    @Test
    void shouldInterpretMap() {
        assertEquals(Map.of(), parseExpression("{}"));
        assertEquals(Map.of("1", 1L), parseExpression("{`1`:1}"));
        assertEquals(Map.of("1", 2L, "3", 4L, "5", 6L), parseExpression("{`1`:2, `3`:4, `5`:6}"));
    }

    @Test
    void shouldInterpretNumbers2() {
        assertEquals(0L, parseExpression("0"));
        assertEquals(12345L, parseExpression("12345"));
        assertEquals(-12345L, parseExpression("-12345"));
        assertEquals(Long.MAX_VALUE, parseExpression(Long.toString(Long.MAX_VALUE)));
        assertEquals(Long.MIN_VALUE, parseExpression(Long.toString(Long.MIN_VALUE)));

        assertEquals(8L, parseExpression("010"));
        assertEquals(-8L, parseExpression("-010"));
        assertEquals(Long.MIN_VALUE, parseExpression("-01000000000000000000000"));

        assertEquals(8L, parseExpression("0o10"));
        assertEquals(-8L, parseExpression("-0o10"));
        assertEquals(-8L, parseExpression("-0o10"));
        assertEquals(Long.MIN_VALUE, parseExpression("-0o1000000000000000000000"));

        assertEquals(255L, parseExpression("0xff"));
        assertEquals(-255L, parseExpression("-0xff"));
        assertEquals(Long.MIN_VALUE, parseExpression("-0x8000000000000000"));

        assertEquals(0L, parseExpression("0"));
        assertEquals(0.0d, parseExpression("0.0"));
        assertEquals(-0.0d, parseExpression("-0.0"));
        assertEquals(1.0d, parseExpression("1.0"));
        assertEquals(98723.0e31d, parseExpression("98723.0e31"));
        assertEquals(Double.MAX_VALUE, parseExpression(Double.toString(Double.MAX_VALUE)));
        assertEquals(Double.MIN_VALUE, parseExpression(Double.toString(Double.MIN_VALUE)));
    }

    @Test
    void shouldInterpretString2() {
        assertEquals("a string", parseExpression("'a string'"));

        assertEquals("ÅÄü", parseExpression("'ÅÄü'"));
        assertEquals("Ελληνικά", parseExpression("'Ελληνικά'"));
        assertEquals("\uD83D\uDCA9", parseExpression("'\uD83D\uDCA9'"));
    }

    @Test
    void shouldInterpretNull2() {
        assertNull(parseExpression("null"));
    }

    @Test
    void shouldInterpretBoolean2() {
        assertEquals(true, parseExpression("true"));
        assertEquals(false, parseExpression("false"));
    }

    @Test
    void shouldInterpretList2() {
        assertThat(parseExpression("[1,2,3]"))
                .asInstanceOf(InstanceOfAssertFactories.list(Long.class))
                .containsExactly(1L, 2L, 3L);

        assertThat(parseExpression(" [ 1, 2, 3 ] "))
                .asInstanceOf(InstanceOfAssertFactories.list(Long.class))
                .containsExactly(1L, 2L, 3L);
    }

    @Test
    void shouldInterpretNestedList() {
        List<?> list1 = (List<?>) parseExpression("[1,[2,[3]]]");

        assertThat(list1).hasSize(2);
        assertThat(list1.get(0)).isEqualTo(1L);

        List<?> list2 = (List<?>) list1.get(1);
        assertThat(list2).hasSize(2);
        assertThat(list2.get(0)).isEqualTo(2L);

        List<?> list3 = (List<?>) list2.get(1);
        assertThat(list3).hasSize(1);
        assertThat(list3.get(0)).isEqualTo(3L);
    }

    @Test
    void shouldInterpretMap2() {
        assertThat(parseExpression("{}"))
                .asInstanceOf(InstanceOfAssertFactories.map(String.class, Object.class))
                .isEmpty();

        assertThat(parseExpression("{age: 2}"))
                .asInstanceOf(InstanceOfAssertFactories.map(String.class, Object.class))
                .containsOnly(entry("age", 2L));

        assertThat(parseExpression("{name: 'Scotty', age: 4, height: 94.3}"))
                .asInstanceOf(InstanceOfAssertFactories.map(String.class, Object.class))
                .containsOnly(entry("name", "Scotty"), entry("age", 4L), entry("height", 94.3));
    }

    @Test
    void shouldNotInterpretIncomplete() {
        assertThatThrownBy(() -> parseExpression("{}}"))
                .isInstanceOf(SyntaxException.class)
                .message()
                .isEqualToIgnoringNewLines(
                        """
                                Invalid cypher expression
                                "{}}"
                                   ^""");
    }

    @Test
    void shouldFailUnsupportedOperations() {
        assertThatThrownBy(() -> parseExpression("1 + 1")).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> parseExpression("abc")).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> parseExpression("my.date()")).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void shouldFailInvalidSyntax() {
        assertThatThrownBy(() -> parseExpression("{1a:1}"))
                .isInstanceOf(SyntaxException.class)
                .hasMessageStartingWith("mismatched input '1a'")
                .hasMessageEndingWith("\"{1a:1}\"%n  ^".formatted());
        assertThatThrownBy(() -> parseExpression("[1, "))
                .isInstanceOf(SyntaxException.class)
                .hasMessageStartingWith("mismatched input '<EOF>'")
                .hasMessageEndingWith("\"[1,\"%n     ^".formatted());
    }

    @Test
    void shouldParseEscapeCodes() {
        assertEquals("\"\"", parseExpression("\"\\\"\\\"\""));
        assertEquals("\"\"", parseExpression("'\\\"\\\"'"));
        assertEquals("''", parseExpression("'\\'\\''"));
        assertEquals("\t", parseExpression("'\\t'"));
        assertEquals("\b", parseExpression("'\\b'"));
        assertEquals("\n", parseExpression("'\\n'"));
        assertEquals("\r", parseExpression("'\\r'"));
        assertEquals("\f", parseExpression("'\\f'"));
        assertEquals("\\", parseExpression("\"\\\\\""));
        assertEquals("\t", parseExpression("\"\\t\""));
        assertEquals("\b", parseExpression("\"\\b\""));
        assertEquals("\n", parseExpression("\"\\n\""));
        assertEquals("\r", parseExpression("\"\\r\""));
        assertEquals("\f", parseExpression("\"\\f\""));
        assertEquals("\\", parseExpression("\"\\\\\""));
        assertEquals("!", parseExpression("'\\u0021'"));
        assertEquals("!?=", parseExpression("'\\u0021\\u003F\\u003D'"));
        assertEquals("\uD83C\uDF1E", parseExpression("\"\\uD83C\\uDF1E\""));
    }

    @Test
    void shouldInterpretNestedMap() {
        Map<?, ?> map1 = (Map<?, ?>) parseExpression("{k1: 1, map2: {k2: 2, map3: {k3: 3}}}");

        assertThat(map1).hasSize(2);
        assertThat(map1.get("k1")).isEqualTo(1L);

        Map<?, ?> map2 = (Map<?, ?>) map1.get("map2");
        assertThat(map2).hasSize(2);
        assertThat(map2.get("k2")).isEqualTo(2L);

        Map<?, ?> map3 = (Map<?, ?>) map2.get("map3");
        assertThat(map3).hasSize(1);
        assertThat(map3.get("k3")).isEqualTo(3L);
    }

    @Test
    void shouldInterpretDate() {
        DateValue date = DateValue.date(2020, 12, 10);
        assertEquals(date, parseExpression("date('2020-12-10')"));
        assertEquals(date, parseExpression("date({year:2020, month:12, day:10})"));

        assertNotNull(parseExpression("date()")); // should not throw

        assertThrows(IllegalArgumentException.class, () -> parseExpression("date(2020, 12, 10)"));
        assertThrows(
                UnsupportedTemporalUnitException.class,
                () -> parseExpression("date({year:2020, month:12, day:10, timezone: 'America/Los Angeles'})"));
        assertNull(parseExpression("date(null)"));
    }

    @Test
    void shouldInterpretDateTime() {
        DateTimeValue date = DateTimeValue.datetime(2020, 12, 10, 6, 41, 23, 0, DEFAULT_ZONE_ID);
        DateTimeValue dateTimeZone =
                DateTimeValue.datetime(2020, 12, 10, 6, 41, 23, 0, ZoneId.of("America/Los_Angeles"));
        assertEquals(date, parseExpression("datetime('2020-12-10T6:41:23.0')"));
        assertEquals(date, parseExpression("datetime({year:2020, month:12, day:10, hour: 6, minute: 41, second: 23})"));
        assertEquals(
                dateTimeZone,
                parseExpression(
                        "datetime({year:2020, month:12, day:10, hour: 6, minute: 41, second: 23, timezone: 'America/Los Angeles'})"));
        assertNotNull(parseExpression("datetime()")); // should not throw

        assertThrows(IllegalArgumentException.class, () -> parseExpression("datetime(2020, 12, 10, 6, 41, 23, 0)"));

        assertNull(parseExpression("datetime(null)"));
    }

    @Test
    void shouldInterpretTime() {
        Instant instant = Instant.now();
        ZoneOffset currentOffsetForMyZone = DEFAULT_ZONE_ID.getRules().getOffset(instant);
        TimeValue date = TimeValue.time(6, 41, 23, 0, currentOffsetForMyZone);
        assertEquals(date, parseExpression("time('6:41:23.0')"));
        assertEquals(date, parseExpression("time({hour: 6, minute: 41, second: 23})"));
        assertNotNull(parseExpression("time()")); // should not throw

        assertThrows(IllegalArgumentException.class, () -> parseExpression("time(6, 41, 23, 0)"));

        assertNull(parseExpression("time(null)"));
    }

    @Test
    void shouldInterpretLocalTime() {
        LocalTimeValue date = LocalTimeValue.localTime(6, 41, 23, 0);
        assertEquals(date, parseExpression("localtime('6:41:23.0')"));
        assertEquals(date, parseExpression("localtime({hour: 6, minute: 41, second: 23})"));
        assertNotNull(parseExpression("localtime()")); // should not throw

        assertThrows(IllegalArgumentException.class, () -> parseExpression("localtime(6, 41, 23, 0)"));

        assertNull(parseExpression("localtime(null)"));
    }

    @Test
    void shouldInterpretLocalDateTime() {
        LocalDateTimeValue date = LocalDateTimeValue.localDateTime(2020, 12, 10, 6, 41, 23, 0);
        assertEquals(date, parseExpression("localdatetime('2020-12-10T6:41:23.0')"));
        assertEquals(
                date, parseExpression("localdatetime({year:2020, month:12, day:10, hour: 6, minute: 41, second: 23})"));
        assertNotNull(parseExpression("localdatetime()")); // should not throw

        assertThrows(
                IllegalArgumentException.class, () -> parseExpression("localdatetime(2020, 12, 10, 6, 41, 23, 0)"));

        assertNull(parseExpression("localdatetime(null)"));
    }

    @Test
    void shouldInterpretPoint() {
        PointValue point = PointValue.parse("{ x:3, y:0 }");
        assertEquals(point, parseExpression("point({ x:3, y:0 })"));

        PointValue point3d = PointValue.parse("{ x:0, y:4, z:1 }");
        assertEquals(point3d, parseExpression("point({ x:0, y:4, z:1 })"));

        PointValue pointWGS84 = PointValue.parse("{ longitude: 56.7, latitude: 12.78 }");
        assertEquals(pointWGS84, parseExpression("point({ longitude: 56.7, latitude: 12.78 })"));
        assertEquals(pointWGS84.getCoordinateReferenceSystem().getName(), "wgs-84");

        assertThrows(IllegalArgumentException.class, () -> parseExpression("point(2020)"));

        assertNull(parseExpression("point(null)"));
    }
}
