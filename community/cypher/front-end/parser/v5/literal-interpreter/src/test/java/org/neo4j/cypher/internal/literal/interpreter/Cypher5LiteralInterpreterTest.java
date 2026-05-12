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
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;
import static org.neo4j.cypher.internal.literal.interpreter.Cypher5LiteralInterpreter.parseExpression;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.util.Maps;
import org.junit.jupiter.api.Test;
import org.neo4j.exceptions.SyntaxException;
import org.neo4j.exceptions.UnsupportedTemporalUnitException;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomSupportExtension;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TimeValue;

@RandomSupportExtension
public class Cypher5LiteralInterpreterTest {
    public static final ZoneId DEFAULT_ZONE_ID = ZoneId.systemDefault();

    @Inject
    RandomSupport rand;

    @Test
    void literalZero() {
        assertThat(parseExpression("0")).isEqualTo(0L);
    }

    @Test
    void randomLong() {
        var val = rand.nextLong();
        assertThat(parseExpression(String.valueOf(val))).isEqualTo(val);
    }

    @Test
    void shouldInterpretNumbers() {

        assertThat(parseExpression("0")).isEqualTo(0L);
        assertThat(parseExpression("12345")).isEqualTo(12345L);
        assertThat(parseExpression("-12345")).isEqualTo(-12345L);
        assertThat(parseExpression(Long.toString(Long.MAX_VALUE))).isEqualTo(Long.MAX_VALUE);
        assertThat(parseExpression(Long.toString(Long.MIN_VALUE))).isEqualTo(Long.MIN_VALUE);

        // old syntax
        assertThat(parseExpression("010")).isEqualTo(8L);
        assertThat(parseExpression("-010")).isEqualTo(-8L);
        assertThat(parseExpression("-0" + Long.toString(Long.MIN_VALUE, 8).substring(1)))
                .isEqualTo(Long.MIN_VALUE);

        assertThat(parseExpression("0o10")).isEqualTo(8L);
        assertThat(parseExpression("-0o10")).isEqualTo(-8L);
        assertThat(parseExpression("-0o" + Long.toString(Long.MIN_VALUE, 8).substring(1)))
                .isEqualTo(Long.MIN_VALUE);

        assertThat(parseExpression("0xff")).isEqualTo(255L);
        assertThat(parseExpression("-0xff")).isEqualTo(-255L);
        assertThat(parseExpression("-0x" + Long.toString(Long.MIN_VALUE, 16).substring(1)))
                .isEqualTo(Long.MIN_VALUE);

        assertThat(parseExpression("0.0")).isEqualTo(0.0d);
        assertThat(parseExpression("0.0e0")).isEqualTo(0.0d);
        assertThat(parseExpression("-0.0e0")).isEqualTo(-0.0d);
        assertThat(parseExpression("1.0e0")).isEqualTo(1.0d);
        assertThat(parseExpression("98723.0e31")).isEqualTo(98723.0e31d);
        assertThat(parseExpression(Double.toString(Double.MAX_VALUE))).isEqualTo(Double.MAX_VALUE);
        assertThat(parseExpression(Double.toString(Double.MIN_VALUE))).isEqualTo(Double.MIN_VALUE);
    }

    @Test
    void shouldInterpretString() {
        assertThat(parseExpression("\"a string\"")).isEqualTo("a string");
        assertThat(parseExpression("'ÅÄü'")).isEqualTo("ÅÄü");
        assertThat(parseExpression("\"Ελληνικά\"")).isEqualTo("Ελληνικά");
        assertThat(parseExpression("'\uD83D\uDCA9'")).isEqualTo("\uD83D\uDCA9");
    }

    @Test
    void shouldInterpretNull() {
        assertThat(parseExpression("null")).isNull();
    }

    @Test
    void shouldHandleNullMap() {
        assertThat(parseExpression("{hello:null}")).isEqualTo(Maps.newHashMap("hello", null));
    }

    @Test
    void shouldInterpretBoolean() {
        assertThat(parseExpression("true")).isEqualTo(true);
        assertThat(parseExpression("false")).isEqualTo(false);
    }

    @Test
    void shouldInterpretInfinity() {
        assertThat(parseExpression("Infinity")).isEqualTo(Double.POSITIVE_INFINITY);
    }

    @Test
    void shouldInterpretNaN() {
        assertThat(parseExpression("NaN")).isEqualTo(Double.NaN);
    }

    @Test
    void shouldInterpretList() {
        assertThat(parseExpression("[1, 2, 3]")).isEqualTo(List.of(1L, 2L, 3L));
    }

    @Test
    void shouldInterpretMap() {
        assertThat(parseExpression("{}")).isEqualTo(Map.of());
        assertThat(parseExpression("{`1`:1}")).isEqualTo(Map.of("1", 1L));
        assertThat(parseExpression("{`1`:2, `3`:4, `5`:6}")).isEqualTo(Map.of("1", 2L, "3", 4L, "5", 6L));
    }

    @Test
    void shouldInterpretNumbers2() {
        assertThat(parseExpression("0")).isEqualTo(0L);
        assertThat(parseExpression("12345")).isEqualTo(12345L);
        assertThat(parseExpression("-12345")).isEqualTo(-12345L);
        assertThat(parseExpression(Long.toString(Long.MAX_VALUE))).isEqualTo(Long.MAX_VALUE);
        assertThat(parseExpression(Long.toString(Long.MIN_VALUE))).isEqualTo(Long.MIN_VALUE);

        assertThat(parseExpression("010")).isEqualTo(8L);
        assertThat(parseExpression("-010")).isEqualTo(-8L);
        assertThat(parseExpression("-01000000000000000000000")).isEqualTo(Long.MIN_VALUE);

        assertThat(parseExpression("0o10")).isEqualTo(8L);
        assertThat(parseExpression("-0o10")).isEqualTo(-8L);
        assertThat(parseExpression("-0o10")).isEqualTo(-8L);
        assertThat(parseExpression("-0o1000000000000000000000")).isEqualTo(Long.MIN_VALUE);

        assertThat(parseExpression("0xff")).isEqualTo(255L);
        assertThat(parseExpression("-0xff")).isEqualTo(-255L);
        assertThat(parseExpression("-0x8000000000000000")).isEqualTo(Long.MIN_VALUE);

        assertThat(parseExpression("0")).isEqualTo(0L);
        assertThat(parseExpression("0.0")).isEqualTo(0.0d);
        assertThat(parseExpression("-0.0")).isEqualTo(-0.0d);
        assertThat(parseExpression("1.0")).isEqualTo(1.0d);
        assertThat(parseExpression("98723.0e31")).isEqualTo(98723.0e31d);
        assertThat(parseExpression(Double.toString(Double.MAX_VALUE))).isEqualTo(Double.MAX_VALUE);
        assertThat(parseExpression(Double.toString(Double.MIN_VALUE))).isEqualTo(Double.MIN_VALUE);
    }

    @Test
    void shouldInterpretString2() {
        assertThat(parseExpression("'a string'")).isEqualTo("a string");

        assertThat(parseExpression("'ÅÄü'")).isEqualTo("ÅÄü");
        assertThat(parseExpression("'Ελληνικά'")).isEqualTo("Ελληνικά");
        assertThat(parseExpression("'\uD83D\uDCA9'")).isEqualTo("\uD83D\uDCA9");
    }

    @Test
    void shouldInterpretNull2() {
        assertThat(parseExpression("null")).isNull();
    }

    @Test
    void shouldInterpretBoolean2() {
        assertThat(parseExpression("true")).isEqualTo(true);
        assertThat(parseExpression("false")).isEqualTo(false);
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
                .isEqualToIgnoringNewLines("""
                                Invalid cypher expression
                                "{}}"
                                   ^
                                """);
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
        assertThat(parseExpression("\"\\\"\\\"\"")).isEqualTo("\"\"");
        assertThat(parseExpression("'\\\"\\\"'")).isEqualTo("\"\"");
        assertThat(parseExpression("'\\'\\''")).isEqualTo("''");
        assertThat(parseExpression("'\\t'")).isEqualTo("\t");
        assertThat(parseExpression("'\\b'")).isEqualTo("\b");
        assertThat(parseExpression("'\\n'")).isEqualTo("\n");
        assertThat(parseExpression("'\\r'")).isEqualTo("\r");
        assertThat(parseExpression("'\\f'")).isEqualTo("\f");
        assertThat(parseExpression("\"\\\\\"")).isEqualTo("\\");
        assertThat(parseExpression("\"\\t\"")).isEqualTo("\t");
        assertThat(parseExpression("\"\\b\"")).isEqualTo("\b");
        assertThat(parseExpression("\"\\n\"")).isEqualTo("\n");
        assertThat(parseExpression("\"\\r\"")).isEqualTo("\r");
        assertThat(parseExpression("\"\\f\"")).isEqualTo("\f");
        assertThat(parseExpression("\"\\\\\"")).isEqualTo("\\");
        assertThat(parseExpression("'\\u0021'")).isEqualTo("!");
        assertThat(parseExpression("'\\u0021\\u003F\\u003D'")).isEqualTo("!?=");
        assertThat(parseExpression("\"\\uD83C\\uDF1E\"")).isEqualTo("\uD83C\uDF1E");
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
        assertThat(parseExpression("date('2020-12-10')")).isEqualTo(date);
        assertThat(parseExpression("date({year:2020, month:12, day:10})")).isEqualTo(date);

        assertThat(parseExpression("date()")).isNotNull(); // should not throw

        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> parseExpression("date(2020, 12, 10)"));
        assertThatExceptionOfType(UnsupportedTemporalUnitException.class)
                .isThrownBy(
                        () -> parseExpression("date({year:2020, month:12, day:10, timezone: 'America/Los Angeles'})"));
        assertThat(parseExpression("date(null)")).isNull();
    }

    @Test
    void shouldInterpretDateTime() {
        DateTimeValue date = DateTimeValue.datetime(2020, 12, 10, 6, 41, 23, 0, DEFAULT_ZONE_ID);
        DateTimeValue dateTimeZone =
                DateTimeValue.datetime(2020, 12, 10, 6, 41, 23, 0, ZoneId.of("America/Los_Angeles"));
        assertThat(parseExpression("datetime('2020-12-10T6:41:23.0')")).isEqualTo(date);
        assertThat(parseExpression("datetime({year:2020, month:12, day:10, hour: 6, minute: 41, second: 23})"))
                .isEqualTo(date);
        assertThat(
                        parseExpression(
                                "datetime({year:2020, month:12, day:10, hour: 6, minute: 41, second: 23, timezone: 'America/Los Angeles'})"))
                .isEqualTo(dateTimeZone);
        assertThat(parseExpression("datetime()")).isNotNull(); // should not throw

        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> parseExpression("datetime(2020, 12, 10, 6, 41, 23, 0)"));

        assertThat(parseExpression("datetime(null)")).isNull();
    }

    @Test
    void shouldInterpretTime() {
        Instant instant = Instant.now();
        ZoneOffset currentOffsetForMyZone = DEFAULT_ZONE_ID.getRules().getOffset(instant);
        TimeValue date = TimeValue.time(6, 41, 23, 0, currentOffsetForMyZone);
        assertThat(parseExpression("time('6:41:23.0')")).isEqualTo(date);
        assertThat(parseExpression("time({hour: 6, minute: 41, second: 23})")).isEqualTo(date);
        assertThat(parseExpression("time()")).isNotNull(); // should not throw

        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> parseExpression("time(6, 41, 23, 0)"));

        assertThat(parseExpression("time(null)")).isNull();
    }

    @Test
    void shouldInterpretLocalTime() {
        LocalTimeValue date = LocalTimeValue.localTime(6, 41, 23, 0);
        assertThat(parseExpression("localtime('6:41:23.0')")).isEqualTo(date);
        assertThat(parseExpression("localtime({hour: 6, minute: 41, second: 23})"))
                .isEqualTo(date);
        assertThat(parseExpression("localtime()")).isNotNull(); // should not throw

        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> parseExpression("localtime(6, 41, 23, 0)"));

        assertThat(parseExpression("localtime(null)")).isNull();
    }

    @Test
    void shouldInterpretLocalDateTime() {
        LocalDateTimeValue date = LocalDateTimeValue.localDateTime(2020, 12, 10, 6, 41, 23, 0);
        assertThat(parseExpression("localdatetime('2020-12-10T6:41:23.0')")).isEqualTo(date);
        assertThat(parseExpression("localdatetime({year:2020, month:12, day:10, hour: 6, minute: 41, second: 23})"))
                .isEqualTo(date);
        assertThat(parseExpression("localdatetime()")).isNotNull(); // should not throw

        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> parseExpression("localdatetime(2020, 12, 10, 6, 41, 23, 0)"));

        assertThat(parseExpression("localdatetime(null)")).isNull();
    }

    @Test
    void shouldInterpretPoint() {
        PointValue point = PointValue.parse("{ x:3, y:0 }");
        assertThat(parseExpression("point({ x:3, y:0 })")).isEqualTo(point);

        PointValue point3d = PointValue.parse("{ x:0, y:4, z:1 }");
        assertThat(parseExpression("point({ x:0, y:4, z:1 })")).isEqualTo(point3d);

        PointValue pointWGS84 = PointValue.parse("{ longitude: 56.7, latitude: 12.78 }");
        assertThat(parseExpression("point({ longitude: 56.7, latitude: 12.78 })"))
                .isEqualTo(pointWGS84);
        assertThat(pointWGS84.getCoordinateReferenceSystem().getName()).isEqualTo("wgs-84");

        assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> parseExpression("point(2020)"));

        assertThat(parseExpression("point(null)")).isNull();
    }
}
