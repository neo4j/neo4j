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
package org.neo4j.configuration;

import static java.time.Duration.ofMinutes;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;
import static org.neo4j.configuration.SettingConstraints.POWER_OF_2;
import static org.neo4j.configuration.SettingConstraints.any;
import static org.neo4j.configuration.SettingConstraints.dependency;
import static org.neo4j.configuration.SettingConstraints.except;
import static org.neo4j.configuration.SettingConstraints.is;
import static org.neo4j.configuration.SettingConstraints.lessThanOrEqual;
import static org.neo4j.configuration.SettingConstraints.matches;
import static org.neo4j.configuration.SettingConstraints.max;
import static org.neo4j.configuration.SettingConstraints.min;
import static org.neo4j.configuration.SettingConstraints.noDuplicates;
import static org.neo4j.configuration.SettingConstraints.range;
import static org.neo4j.configuration.SettingValueParsers.BOOL;
import static org.neo4j.configuration.SettingValueParsers.BYTES;
import static org.neo4j.configuration.SettingValueParsers.CIDR_IP;
import static org.neo4j.configuration.SettingValueParsers.DOUBLE;
import static org.neo4j.configuration.SettingValueParsers.DURATION;
import static org.neo4j.configuration.SettingValueParsers.DURATION_RANGE;
import static org.neo4j.configuration.SettingValueParsers.FALSE;
import static org.neo4j.configuration.SettingValueParsers.HOSTNAME_PORT;
import static org.neo4j.configuration.SettingValueParsers.INT;
import static org.neo4j.configuration.SettingValueParsers.JVM_ADDITIONAL;
import static org.neo4j.configuration.SettingValueParsers.LONG;
import static org.neo4j.configuration.SettingValueParsers.NORMALIZED_RELATIVE_URI;
import static org.neo4j.configuration.SettingValueParsers.PATH;
import static org.neo4j.configuration.SettingValueParsers.SECURE_STRING;
import static org.neo4j.configuration.SettingValueParsers.SOCKET_ADDRESS;
import static org.neo4j.configuration.SettingValueParsers.STRING;
import static org.neo4j.configuration.SettingValueParsers.TIMEZONE;
import static org.neo4j.configuration.SettingValueParsers.TRUE;
import static org.neo4j.configuration.SettingValueParsers.listOf;
import static org.neo4j.configuration.SettingValueParsers.ofEnum;
import static org.neo4j.configuration.SettingValueParsers.ofPartialEnum;
import static org.neo4j.configuration.SettingValueParsers.setOf;
import static org.neo4j.configuration.SettingValueParsers.setOfEnums;
import static org.neo4j.graphdb.config.Configuration.EMPTY;

import inet.ipaddr.IPAddressString;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.configuration.helpers.DurationRange;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.io.ByteUnit;
import org.neo4j.string.SecureString;

class SettingTest {
    @Test
    void testInteger() {
        var setting = (SettingImpl<Integer>) setting("setting", INT);
        assertEquals(5, setting.parse("5"));
        assertEquals(5, setting.parse(" 5 "));
        assertEquals(-76, setting.parse("-76"));
        assertThrows(IllegalArgumentException.class, () -> setting.parse("foo"));
    }

    @Test
    void testLong() {
        var setting = (SettingImpl<Long>) setting("setting", LONG);
        assertEquals(112233445566778899L, setting.parse("112233445566778899"));
        assertEquals(112233445566778899L, setting.parse(" 112233445566778899 "));
        assertEquals(-112233445566778899L, setting.parse("-112233445566778899"));
        assertThrows(IllegalArgumentException.class, () -> setting.parse("foo"));
    }

    @Test
    void testString() {
        var setting = (SettingImpl<String>) setting("setting", STRING);
        assertEquals("foo", setting.parse("foo"));
        assertEquals("bar", setting.parse("  bar   "));
    }

    @Test
    void testSecureString() {
        var setting = (SettingImpl<SecureString>) setting("setting", SECURE_STRING);
        assertEquals("foo", setting.parse("foo").getString());
        assertNotEquals("foo", setting.parse("foo").toString());
        assertEquals("bar", setting.parse("  bar   ").getString());
        assertNotEquals("foo", setting.valueToString(setting.parse("foo")));
    }

    @Test
    void testDouble() {
        BiFunction<Double, Double, Boolean> compareDoubles = (Double d1, Double d2) -> Math.abs(d1 - d2) < 0.000001;

        var setting = (SettingImpl<Double>) setting("setting", DOUBLE);
        assertEquals(5.0, setting.parse("5"));
        assertEquals(5.0, setting.parse("  5 "));
        assertTrue(compareDoubles.apply(-.123, setting.parse("-0.123")));
        assertTrue(compareDoubles.apply(5.0, setting.parse("5")));
        assertThrows(IllegalArgumentException.class, () -> setting.parse("foo"));
    }

    @Test
    void testList() {
        var setting = (SettingImpl<List<Integer>>) setting("setting", listOf(INT));
        assertEquals(5, setting.parse("5").get(0));
        assertEquals(0, setting.parse("").size());
        assertEquals(4, setting.parse("5, 31, -4  ,2").size());
        assertEquals(Arrays.asList(4, 2, 3, 1), setting.parse("4,2,3,1"));
        assertThrows(IllegalArgumentException.class, () -> setting.parse("2,3,foo,7"));

        assertFalse(setting.valueToString(setting.parse("4,2,3,1")).startsWith("["));
        assertFalse(setting.valueToString(setting.parse("4,2,3,1")).endsWith("]"));
    }

    @Test
    void testListValidation() {
        var setting = (SettingImpl<List<Integer>>) setting("setting", listOf(POSITIVE_INT));
        assertDoesNotThrow(() -> setting.validate(List.of(), EMPTY));
        assertDoesNotThrow(() -> setting.validate(List.of(5), EMPTY));
        assertDoesNotThrow(() -> setting.validate(List.of(1, 2, 3), EMPTY));
        assertThrows(IllegalArgumentException.class, () -> setting.validate(List.of(1, -2, 3), EMPTY));
    }

    @Test
    void testSet() {
        var setting = (SettingImpl<Set<Integer>>) setting("setting", setOf(INT));
        assertThat(setting.parse("5")).containsExactly(5);
        assertThat(setting.parse("")).isEmpty();
        assertThat(setting.parse("5, 31, -4  ,2")).containsExactlyInAnyOrder(5, 31, -4, 2);
        assertThat(setting.parse("5, 5, 5, 3, 900, 0")).containsExactlyInAnyOrder(0, 3, 5, 900);
        assertThrows(IllegalArgumentException.class, () -> setting.parse("2,3,foo,7"));
    }

    @Test
    void testSetValidation() {
        var setting = (SettingImpl<Set<Integer>>) setting("setting", setOf(POSITIVE_INT));
        assertDoesNotThrow(() -> setting.validate(Set.of(), EMPTY));
        assertDoesNotThrow(() -> setting.validate(Set.of(5), EMPTY));
        assertDoesNotThrow(() -> setting.validate(Set.of(1, 2, 3), EMPTY));
        assertThrows(IllegalArgumentException.class, () -> setting.validate(Set.of(1, -2, 3), EMPTY));
    }

    @Test
    void testEnum() {
        var setting = (SettingImpl<Colors>) setting("setting", ofEnum(Colors.class));
        assertEquals(Colors.BLUE, setting.parse("BLUE"));
        assertEquals(Colors.GREEN, setting.parse("gReEn"));
        assertEquals(Colors.RED, setting.parse("red"));
        assertEquals(Colors.RED, setting.parse(" red "));
        assertThrows(IllegalArgumentException.class, () -> setting.parse("orange"));
    }

    @Test
    void testPartialEnum() {
        var setting = (SettingImpl<Colors>) setting("setting", ofPartialEnum(Colors.GREEN, Colors.BLUE));
        assertEquals(Colors.BLUE, setting.parse("BLUE"));
        assertEquals(Colors.GREEN, setting.parse("gReEn"));
        assertThrows(IllegalArgumentException.class, () -> setting.parse("red"));
    }

    @Test
    void testStringEnum() {
        var setting = (SettingImpl<StringEnum>) setting("setting", ofEnum(StringEnum.class));
        assertEquals(StringEnum.DEFAULT, setting.parse("default"));
        assertEquals(StringEnum.V_1, setting.parse("1.0"));
        assertEquals(StringEnum.V_1_1, setting.parse("1.1"));
        assertThrows(IllegalArgumentException.class, () -> setting.parse("orange"));
    }

    @Test
    void testBool() {
        var setting = (SettingImpl<Boolean>) setting("setting", BOOL);
        assertTrue(setting.parse("True"));
        assertFalse(setting.parse("false"));
        assertFalse(setting.parse(FALSE));
        assertTrue(setting.parse(TRUE));
        assertTrue(setting.parse(" true "));
        assertFalse(setting.parse("  false"));
        assertThrows(IllegalArgumentException.class, () -> setting.parse("foo"));
    }

    @Test
    void testDuration() {
        var setting = (SettingImpl<Duration>) setting("setting", DURATION);
        assertEquals(60, setting.parse("1m").toSeconds());
        assertEquals(60, setting.parse(" 1m ").toSeconds());
        assertEquals(1000, setting.parse("1s").toMillis());
        assertThrows(IllegalArgumentException.class, () -> setting.parse("foo"));

        assertEquals("1s", setting.valueToString(setting.parse("1s")));
        assertEquals("3m", setting.valueToString(setting.parse("3m")));

        // Anything less than a millisecond is rounded down
        assertEquals("0s", setting.valueToString(setting.parse("0s")));
        assertEquals("0s", setting.valueToString(setting.parse("1ns")));
        assertEquals("0s", setting.valueToString(setting.parse("999999ns")));
        assertEquals("0s", setting.valueToString(setting.parse("999μs")));

        // Time strings containing multiple units are permitted
        assertEquals("11d19h25m4s50ms", setting.valueToString(setting.parse("11d19h25m4s50ms607μs80ns")));
        // Weird time strings will be converted to something more readable
        assertEquals("2m1ms", setting.valueToString(setting.parse("1m60000ms1000000ns")));

        String descriptionWithConstraint = SettingImpl.newBuilder("setting", DURATION, ofMinutes(1))
                .addConstraint(min(Duration.ofSeconds(10)))
                .build()
                .description();

        String expected =
                "setting, a duration (Valid units are: `ns`, `μs`, `ms`, `s`, `m`, `h` and `d`; default unit is `s`) that is minimum `10s`.";
        assertEquals(expected, descriptionWithConstraint);
    }

    @Test
    void testDurationRange() {
        var setting = (SettingImpl<DurationRange>) setting("setting", DURATION_RANGE);
        assertEquals(60, setting.parse("1m-2m").getMin().toSeconds());
        assertEquals(120, setting.parse("1m-2m").getMax().toSeconds());
        assertEquals(60, setting.parse(" 1m-2m ").getMin().toSeconds());
        assertEquals(120, setting.parse(" 1m-2m ").getMax().toSeconds());
        assertEquals(1000, setting.parse("1s-2s").getMin().toMillis());
        assertEquals(2000, setting.parse("1s-2s").getMax().toMillis());
        assertThrows(IllegalArgumentException.class, () -> setting.parse("1s"));
        assertThrows(IllegalArgumentException.class, () -> setting.parse("1s-"));
        assertThrows(IllegalArgumentException.class, () -> setting.parse("-1s"));
        assertThrows(IllegalArgumentException.class, () -> setting.parse("-1s--2s"));
        assertThrows(IllegalArgumentException.class, () -> setting.parse("2s-1s"));
        assertThrows(IllegalArgumentException.class, () -> setting.parse("2000ms-1s"));

        // DurationRange may have zero delta
        assertEquals(1, setting.parse("1s-1s").getMin().toSeconds());
        assertEquals(1, setting.parse("1s-1s").getMax().toSeconds());
        assertEquals(0, setting.parse("1s-1s").getDelta().toNanos());

        assertEquals("0ns-0ns", setting.valueToString(setting.parse("0s-0s")));
        assertEquals("1s-2s", setting.valueToString(setting.parse("1s-2s")));
        assertEquals("3m-6m", setting.valueToString(setting.parse("[3m-6m]")));

        // Time strings containing multiple units are permitted
        assertEquals("0ns-1m23s456ms", setting.valueToString(setting.parse("0s-1m23s456ms")));

        // Units will be converted to something "more readable"
        assertEquals("1s-2s500ms", setting.valueToString(setting.parse("1000ms-2500ms")));

        // Anything less than a millisecond is rounded down
        assertEquals("0ns-0ns", setting.valueToString(setting.parse("999μs-999999ns")));
        assertEquals(0, setting.parse("999μs-999999ns").getDelta().toNanos());
    }

    @Test
    void testHostnamePort() {
        var setting = (SettingImpl<HostnamePort>) setting("setting", HOSTNAME_PORT);
        assertEquals(new HostnamePort("localhost", 7474), setting.parse("localhost:7474"));
        assertEquals(new HostnamePort("localhost", 1000, 2000), setting.parse("localhost:1000-2000"));
        assertEquals(new HostnamePort("localhost"), setting.parse("localhost"));
        assertThrows(IllegalArgumentException.class, () -> setting.parse("localhost:5641:7474"));
        assertThrows(IllegalArgumentException.class, () -> setting.parse("localhost:foo"));
        assertThrows(IllegalArgumentException.class, () -> setting.parse("7474:localhost"));
    }

    @Test
    void testTimeZone() {
        var setting = (SettingImpl<ZoneId>) setting("setting", TIMEZONE);
        assertEquals(ZoneId.from(ZoneOffset.UTC), setting.parse("+00:00"));
        assertEquals(ZoneId.from(ZoneOffset.UTC), setting.parse(" +00:00 "));
        assertThrows(IllegalArgumentException.class, () -> setting.parse("foo"));
    }

    @Test
    void testCidrIp() {
        var setting = (SettingImpl<IPAddressString>) setting("setting", CIDR_IP);
        assertEquals(new IPAddressString("1.1.1.0/8"), setting.parse("1.1.1.0/8"));
        assertThrows(IllegalArgumentException.class, () -> setting.parse("garbage"));
    }

    @Test
    void testSocket() {
        var setting = (SettingImpl<SocketAddress>) setting("setting", SOCKET_ADDRESS);
        assertEquals(new SocketAddress("127.0.0.1", 7474), setting.parse("127.0.0.1:7474"));
        assertEquals(new SocketAddress("127.0.0.1", 7474), setting.parse(" 127.0.0.1:7474 "));
        assertEquals(new SocketAddress("127.0.0.1", -1), setting.parse("127.0.0.1"));
        assertEquals(new SocketAddress(null, 7474), setting.parse(":7474"));
    }

    @Test
    void testSocketSolve() {
        var setting = (SettingImpl<SocketAddress>) setting("setting", SOCKET_ADDRESS);
        assertEquals(
                new SocketAddress("localhost", 7473),
                setting.solveDependency(setting.parse("localhost:7473"), setting.parse("127.0.0.1:7474")));
        assertEquals(
                new SocketAddress("127.0.0.1", 7473),
                setting.solveDependency(setting.parse(":7473"), setting.parse("127.0.0.1:7474")));
        assertEquals(
                new SocketAddress("127.0.0.1", 7473),
                setting.solveDependency(setting.parse(":7473"), setting.parse("127.0.0.1")));
        assertEquals(
                new SocketAddress("localhost", 7474),
                setting.solveDependency(setting.parse("localhost"), setting.parse(":7474")));
        assertEquals(
                new SocketAddress("localhost", 7474),
                setting.solveDependency(setting.parse("localhost"), setting.parse("127.0.0.1:7474")));
        assertEquals(
                new SocketAddress("localhost", 7474), setting.solveDependency(null, setting.parse("localhost:7474")));
    }

    @Test
    void testBytes() {
        var setting = (SettingImpl<Long>) setting("setting", BYTES);
        assertEquals(2048, setting.parse("2k"));
        assertThrows(IllegalArgumentException.class, () -> setting.parse("1gig"));
        assertThrows(IllegalArgumentException.class, () -> setting.parse("-1M"));

        String descriptionWithConstraint = SettingImpl.newBuilder("setting", BYTES, ByteUnit.gibiBytes(2))
                .addConstraint(range(ByteUnit.mebiBytes(100), ByteUnit.gibiBytes(10)))
                .build()
                .description();

        String expected =
                "setting, a byte size (valid multipliers are `B`, `KiB`, `KB`, `K`, `kB`, `kb`, `k`, `MiB`, `MB`, `M`, `mB`, `mb`, `m`, "
                        + "`GiB`, `GB`, `G`, `gB`, `gb`, `g`, `TiB`, `TB`, `PiB`, `PB`, `EiB`, `EB`) that is in the range `100.00MiB` to `10.00GiB`.";
        assertEquals(expected, descriptionWithConstraint);
    }

    @Test
    void testURI() {
        var setting = (SettingImpl<URI>) setting("setting", SettingValueParsers.URI);
        assertEquals(URI.create("/path/to/../something/"), setting.parse("/path/to/../something/"));
    }

    @Test
    void testHttpsURI() {
        var setting = (SettingImpl<URI>) setting("setting", SettingValueParsers.HTTPS_URI(true));
        assertEquals(URI.create("https://www.example.com/path"), setting.parse("https://www.example.com/path"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"http://www.example.com", "neo4js://database", "/path/to/../something/"})
    void testHttpsURIWithInvalidUris(String uri) {
        var setting = (SettingImpl<URI>) setting("setting", SettingValueParsers.HTTPS_URI(true));
        var exception = assertThrows(IllegalArgumentException.class, () -> setting.parse(uri));
        assertEquals(String.format("'%s' does not have required scheme 'https'", uri), exception.getMessage());
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "http://localhost/endpoint",
                "http://127.0.0.1/endpoint",
                "http://[::1]/endpoint",
                "http://[0:0:0:0:0:0:0:1]/endpoint"
            })
    void testHttpURIExemptionForLocalhostURIs(String uri) {
        var setting = (SettingImpl<URI>) setting("setting", SettingValueParsers.HTTPS_URI(true));
        assertEquals(URI.create(uri), setting.parse(uri));

        var invalidSetting = (SettingImpl<URI>) setting("setting", SettingValueParsers.HTTPS_URI(false));
        var exception = assertThrows(IllegalArgumentException.class, () -> invalidSetting.parse(uri));
        assertEquals(String.format("'%s' does not have required scheme 'https'", uri), exception.getMessage());
    }

    @Test
    void testStringMapWithNoConstraintOnKeys() {
        var setting = (SettingImpl<Map<String, String>>) setting("setting", SettingValueParsers.MAP_PATTERN);
        assertEquals(Map.of("k1", "v1", "k2", "v2"), setting.parse("k1=v1;k2=v2"));
    }

    @Test
    void testStringMapWithValuesContainingEquals() {
        var setting = (SettingImpl<Map<String, String>>) setting("setting", SettingValueParsers.MAP_PATTERN);
        assertEquals(
                Map.of("k1", "cn=admin,dc=example,dc=com", "k2", "v2"),
                setting.parse("k1=cn=admin,dc=example,dc=com;k2=v2"));
    }

    @Test
    void testStringMapWithRequiredKeys() {
        var setting = (SettingImpl<Map<String, String>>)
                setting("setting", new SettingValueParsers.MapPattern(Set.of("k1", "k2")));
        assertEquals(Map.of("k1", "v1", "k2", "v2", "k3", "v3"), setting.parse("k1=v1;k2=v2;k3=v3"));
        assertThrows(IllegalArgumentException.class, () -> setting.parse("k1=v1;k3=v3"));
    }

    @Test
    void testStringMapWithRestrictedKeys() {
        var setting = (SettingImpl<Map<String, String>>)
                setting("setting", new SettingValueParsers.MapPattern(Set.of("k1"), Set.of("k1", "k2")));
        assertEquals(Map.of("k1", "v1", "k2", "v2"), setting.parse("k1=v1;k2=v2"));
        assertEquals(Map.of("k1", "v1"), setting.parse("k1=v1"));
        assertThrows(IllegalArgumentException.class, () -> setting.parse("k2=v2"));
        assertThrows(IllegalArgumentException.class, () -> setting.parse("k1=v1;k3=v3"));
        var settingWithoutRequired = (SettingImpl<Map<String, String>>)
                setting("setting", new SettingValueParsers.MapPattern(null, Set.of("k1", "k2")));
        assertEquals(Map.of("k2", "v2"), settingWithoutRequired.parse("k2=v2"));
    }

    @Test
    void testNormalizedRelativeURI() {
        var setting = (SettingImpl<URI>) setting("setting", NORMALIZED_RELATIVE_URI);
        assertEquals(URI.create("/path/to/something"), setting.parse("/path/away/from/../../to/something/"));
    }

    @Test
    void testPath() {
        var setting = (SettingImpl<Path>) setting("setting", PATH);
        assertEquals(Path.of("/absolute/path"), setting.parse("/absolute/path"));
        assertEquals(Path.of("/absolute/path"), setting.parse("/absolute/wrong/../path"));
        assertEquals(Path.of("/test/escaped/chars/r/n/dir"), setting.parse("\test\\escaped\\chars\r\n\\\\dir"));
    }

    @Test
    void testSolvePath() {
        var setting = (SettingImpl<Path>) setting("setting", PATH);
        assertEquals(
                Path.of("/base/path/to/file").toAbsolutePath(),
                setting.solveDependency(
                        setting.parse("to/file"), setting.parse("/base/path").toAbsolutePath()));
        assertEquals(
                Path.of("/to/file").toAbsolutePath(),
                setting.solveDependency(
                        setting.parse("/to/file"), setting.parse("/base/path").toAbsolutePath()));
        assertEquals(
                Path.of("/base/path/").toAbsolutePath(),
                setting.solveDependency(
                        setting.parse(""), setting.parse("/base/path/").toAbsolutePath()));
        assertEquals(
                Path.of("/base/path").toAbsolutePath(),
                setting.solveDependency(
                        setting.parse("path"), setting.parse("/base").toAbsolutePath()));
        assertEquals(
                Path.of("/base").toAbsolutePath(),
                setting.solveDependency(null, setting.parse("/base").toAbsolutePath()));
        assertThrows(
                IllegalArgumentException.class,
                () -> setting.solveDependency(setting.parse("path"), setting.parse("base")));
    }

    @Test
    void testJvmAdditional() {
        var setting = (SettingImpl<String>) setting("setting", JVM_ADDITIONAL);
        var inputs = new String[] {
            "value1", // value1
            "value2 value3", // value2 value3
            "\"value 4\" \"value 5\"", // "value 4" "value 5"
            "\"value  6\"", // "value  6"
            "value\"quoted\"", // value"quoted"
            " valuewithspace  ", // valuewithspace
            "strwithctrl\u000b\u0002", // some control characters
            " values  with   spaces ", // values  with  spaces
            "\"one quoted\"   value  ", // one quoted value             Note double spaces
            "  one  \"quoted   value\"", // one quoted value             Note double spaces
            "\"two quoted\"  \"values\"" // "two quoted" "values"        Note double spaces
        };
        var outputs = new String[] {
            "value1", // value1
            "value2", // value2
            "value3", // value3
            "value 4", // value 4
            "value 5", // value 5
            "value  6", // value  6
            "value\"quoted\"", // value"quoted"
            "valuewithspace", // valuewithspace
            "strwithctrl", // some control characters
            "values", // values
            "with", // with
            "spaces", // spaces
            "one quoted", // one quoted
            "value", // value
            "one", // one
            "quoted   value", // quoted   value
            "two quoted", // two quoted
            "values", // values
        };
        var actualSettings = setting.parse(String.join(System.lineSeparator(), inputs));
        var expectedSettings = String.join(System.lineSeparator(), outputs);
        assertEquals(expectedSettings, actualSettings);
    }

    @Test
    void testJvmAdditionalBadQuoting() {
        // A JVM setting starting with a quote should have an end quote
        var setting = (SettingImpl<String>) setting("setting", JVM_ADDITIONAL);
        assertThrows(IllegalArgumentException.class, () -> setting.parse("\"missing_end_quote"));
    }

    @Test
    void testJvmAdditionalWithProperty() {
        var setting = (SettingImpl<String>) setting("setting", JVM_ADDITIONAL);
        // A JVM setting should not split on whitespace inside quotes
        assertThat(setting.parse("-Da=\"string with space\"")).isEqualTo("-Da=\"string with space\"");
    }

    @Test
    void testDefaultSolve() {
        var defaultSolver = new SettingValueParser<String>() {
            @Override
            public String parse(String value) {
                return value;
            }

            @Override
            public String getDescription() {
                return "default solver";
            }

            @Override
            public Class<String> getType() {
                return String.class;
            }
        };

        var setting = (SettingImpl<String>) setting("setting", defaultSolver);
        assertEquals("foo", setting.solveDependency("foo", "bar"));
        assertEquals("bar", setting.solveDependency(null, "bar"));
        assertEquals("foo", setting.solveDependency("foo", null));
        assertNull(setting.solveDependency(null, null));
    }

    @Test
    void testMinConstraint() {
        var setting = (SettingImpl<Integer>)
                settingBuilder("setting", INT).addConstraint(min(10)).build();
        assertDoesNotThrow(() -> setting.validate(100, EMPTY));
        assertDoesNotThrow(() -> setting.validate(10, EMPTY));
        assertThrows(IllegalArgumentException.class, () -> setting.validate(9, EMPTY));
    }

    @Test
    void testMaxConstraint() {
        var setting = (SettingImpl<Integer>)
                settingBuilder("setting", INT).addConstraint(max(10)).build();
        assertDoesNotThrow(() -> setting.validate(-100, EMPTY));
        assertDoesNotThrow(() -> setting.validate(10, EMPTY));
        assertThrows(IllegalArgumentException.class, () -> setting.validate(11, EMPTY));
    }

    @Test
    void testRangeConstraint() {
        var setting = (SettingImpl<Double>) settingBuilder("setting", DOUBLE)
                .addConstraint(range(10.0, 20.0))
                .build();

        assertThrows(IllegalArgumentException.class, () -> setting.validate(9.9, EMPTY));
        assertThrows(IllegalArgumentException.class, () -> setting.validate(20.01, EMPTY));
        assertDoesNotThrow(() -> setting.validate(10.1, EMPTY));
        assertDoesNotThrow(() -> setting.validate(19.9999, EMPTY));
    }

    @Test
    void testLessThanOrEqualConstraint() {
        // Given
        var intLimit = (SettingImpl<Integer>) settingBuilder("limit.int", INT).build();
        var durationLimit = (SettingImpl<Duration>)
                settingBuilder("limit.duration", DURATION).build();

        Map<Setting<?>, Object> settings = new HashMap<>();
        Configuration simpleConfig = new Configuration() {
            @Override
            public <T> T get(Setting<T> setting) {
                return (T) settings.get(setting);
            }
        };

        settings.put(intLimit, 5);
        settings.put(durationLimit, Duration.ofSeconds(123));

        // When
        var mustBeLessSetting = (SettingImpl<Integer>) settingBuilder("less.than.duration", INT)
                .addConstraint(lessThanOrEqual(intLimit))
                .build();
        // Then
        assertDoesNotThrow(() -> mustBeLessSetting.validate(-1, simpleConfig));
        assertDoesNotThrow(() -> mustBeLessSetting.validate(0, simpleConfig));
        assertDoesNotThrow(() -> mustBeLessSetting.validate(1, simpleConfig));
        assertDoesNotThrow(() -> mustBeLessSetting.validate(5, simpleConfig));
        assertThrows(IllegalArgumentException.class, () -> mustBeLessSetting.validate(6, simpleConfig));

        // When
        var mustBeLessThanHalfSetting = (SettingImpl<Integer>) settingBuilder("less.than.half.int", INT)
                .addConstraint(lessThanOrEqual(i -> (long) i, intLimit, i -> i / 2, "divided by 2"))
                .build();
        // Then
        assertDoesNotThrow(() -> mustBeLessThanHalfSetting.validate(-1, simpleConfig));
        assertDoesNotThrow(() -> mustBeLessThanHalfSetting.validate(0, simpleConfig));
        assertDoesNotThrow(() -> mustBeLessThanHalfSetting.validate(2, simpleConfig));
        assertThrows(IllegalArgumentException.class, () -> mustBeLessThanHalfSetting.validate(3, simpleConfig));

        // When
        var mustBeLessDuration = (SettingImpl<Duration>) settingBuilder("less.than.duration", DURATION)
                .addConstraint(lessThanOrEqual(Duration::toMillis, durationLimit))
                .build();
        // Then
        assertDoesNotThrow(() -> mustBeLessDuration.validate(Duration.ofSeconds(-1), simpleConfig));
        assertDoesNotThrow(() -> mustBeLessDuration.validate(Duration.ofSeconds(0), simpleConfig));
        assertDoesNotThrow(() -> mustBeLessDuration.validate(Duration.ofMinutes(1), simpleConfig));
        assertDoesNotThrow(() -> mustBeLessDuration.validate(Duration.ofSeconds(123), simpleConfig));
        assertThrows(
                IllegalArgumentException.class,
                () -> mustBeLessDuration.validate(Duration.ofMillis(123001), simpleConfig));

        // When
        var mustBeLessThanHalfDuration = (SettingImpl<Duration>) settingBuilder("less.than.duration", DURATION)
                .addConstraint(lessThanOrEqual(Duration::toMillis, durationLimit, i -> i / 2, "divided by 2"))
                .build();
        // Then
        assertDoesNotThrow(() -> mustBeLessThanHalfDuration.validate(Duration.ofSeconds(-1), simpleConfig));
        assertDoesNotThrow(() -> mustBeLessThanHalfDuration.validate(Duration.ofSeconds(0), simpleConfig));
        assertDoesNotThrow(() -> mustBeLessThanHalfDuration.validate(Duration.ofMinutes(1), simpleConfig));
        assertDoesNotThrow(() -> mustBeLessThanHalfDuration.validate(Duration.ofSeconds(61), simpleConfig));
        assertThrows(
                IllegalArgumentException.class,
                () -> mustBeLessThanHalfDuration.validate(Duration.ofMillis(61501), simpleConfig));
    }

    @Test
    void testExceptConstraint() {
        var setting = (SettingImpl<String>)
                settingBuilder("setting", STRING).addConstraint(except("foo")).build();
        assertThrows(IllegalArgumentException.class, () -> setting.validate("foo", EMPTY));
        assertDoesNotThrow(() -> setting.validate("bar", EMPTY));
    }

    @Test
    void testMatchesConstraint() {
        var setting = (SettingImpl<String>) settingBuilder("setting", STRING)
                .addConstraint(matches("^[^.]+\\.[^.]+$"))
                .build();
        assertDoesNotThrow(() -> setting.validate("foo.bar", EMPTY));
        assertThrows(IllegalArgumentException.class, () -> setting.validate("foo", EMPTY));
    }

    @Test
    void testPowerOf2Constraint() {
        var setting = (SettingImpl<Long>)
                settingBuilder("setting", LONG).addConstraint(POWER_OF_2).build();
        assertDoesNotThrow(() -> setting.validate(8L, EMPTY));
        assertDoesNotThrow(() -> setting.validate(4294967296L, EMPTY));
        assertThrows(IllegalArgumentException.class, () -> setting.validate(1023L, EMPTY));
    }

    @Test
    void testIsConstraint() {
        var setting = (SettingImpl<Integer>)
                settingBuilder("setting", INT).addConstraint(is(10)).build();
        assertDoesNotThrow(() -> setting.validate(10, EMPTY));
        assertThrows(IllegalArgumentException.class, () -> setting.validate(9, EMPTY));
    }

    @Test
    void testAnyConstraint() {
        var intSetting = (SettingImpl<Integer>) settingBuilder("setting", INT)
                .addConstraint(any(min(30), is(0), is(-10)))
                .build();
        assertDoesNotThrow(() -> intSetting.validate(30, EMPTY));
        assertDoesNotThrow(() -> intSetting.validate(100, EMPTY));
        assertDoesNotThrow(() -> intSetting.validate(0, EMPTY));
        assertDoesNotThrow(() -> intSetting.validate(-10, EMPTY));
        assertThrows(IllegalArgumentException.class, () -> intSetting.validate(29, EMPTY));
        assertThrows(IllegalArgumentException.class, () -> intSetting.validate(1, EMPTY));
        assertThrows(IllegalArgumentException.class, () -> intSetting.validate(-9, EMPTY));

        var durationSetting = (SettingImpl<Duration>) settingBuilder("setting", DURATION)
                .addConstraint(any(min(ofMinutes(30)), is(Duration.ZERO)))
                .build();
        assertDoesNotThrow(() -> durationSetting.validate(ofMinutes(30), EMPTY));
        assertDoesNotThrow(() -> durationSetting.validate(Duration.ofHours(1), EMPTY));
        assertDoesNotThrow(() -> durationSetting.validate(Duration.ZERO, EMPTY));
        assertThrows(IllegalArgumentException.class, () -> durationSetting.validate(ofMinutes(29), EMPTY));
        assertThrows(IllegalArgumentException.class, () -> durationSetting.validate(Duration.ofMillis(1), EMPTY));

        String expected =
                "setting, a duration (Valid units are: `ns`, `μs`, `ms`, `s`, `m`, `h` and `d`; default unit is `s`) that is minimum `30m` or is `0s`.";
        assertEquals(expected, durationSetting.description());
    }

    @Test
    void testDependencyConstraint() {
        // Given
        var intSetting =
                (SettingImpl<Integer>) settingBuilder("int-setting", INT).build();
        var enumSetting = (SettingImpl<Colors>)
                settingBuilder("enum-setting", ofEnum(Colors.class)).build();
        Map<Setting<?>, Object> settings = new HashMap<>();

        Configuration simpleConfig = new Configuration() {
            @Override
            public <T> T get(Setting<T> setting) {
                return (T) settings.get(setting);
            }
        };
        var dependingIntSetting = (SettingImpl<Integer>) settingBuilder("setting", INT)
                .addConstraint(dependency(max(3), max(7), intSetting, min(3)))
                .build();

        var dependingEnumSetting = (SettingImpl<List<String>>) settingBuilder("setting", listOf(STRING))
                .addConstraint(dependency(
                        SettingConstraints.size(2), SettingConstraints.size(4), enumSetting, is(Colors.BLUE)))
                .build();

        // When
        settings.put(intSetting, 5);
        settings.put(enumSetting, Colors.BLUE);
        // Then
        assertDoesNotThrow(() -> dependingIntSetting.validate(3, simpleConfig));
        assertThrows(IllegalArgumentException.class, () -> dependingIntSetting.validate(4, simpleConfig));

        assertDoesNotThrow(() -> dependingEnumSetting.validate(List.of("a", "b"), simpleConfig));
        assertThrows(
                IllegalArgumentException.class,
                () -> dependingEnumSetting.validate(List.of("a", "b", "c"), simpleConfig));
        assertThrows(
                IllegalArgumentException.class,
                () -> dependingEnumSetting.validate(List.of("a", "b", "c", "d"), simpleConfig));

        // When
        settings.put(intSetting, 2);
        settings.put(enumSetting, Colors.GREEN);
        // Then
        assertDoesNotThrow(() -> dependingIntSetting.validate(4, simpleConfig));
        assertThrows(IllegalArgumentException.class, () -> dependingIntSetting.validate(8, simpleConfig));

        assertDoesNotThrow(() -> dependingEnumSetting.validate(List.of("a", "b", "c", "d"), simpleConfig));
        assertThrows(
                IllegalArgumentException.class, () -> dependingEnumSetting.validate(List.of("a", "b"), simpleConfig));
        assertThrows(
                IllegalArgumentException.class,
                () -> dependingEnumSetting.validate(List.of("a", "b", "c"), simpleConfig));
    }

    @Test
    void testDescriptionWithConstraints() {
        // Given
        var oneConstraintSetting = (SettingImpl<Long>)
                settingBuilder("setting.name", LONG).addConstraint(POWER_OF_2).build();

        var twoConstraintSetting = (SettingImpl<Integer>) settingBuilder("setting.name", INT)
                .addConstraint(min(2))
                .addConstraint(max(10))
                .build();

        var enumSetting = (SettingImpl<Colors>)
                settingBuilder("setting.name", ofEnum(Colors.class)).build();
        var intSetting =
                (SettingImpl<Integer>) settingBuilder("setting.name", INT).build();

        var dependencySetting1 = (SettingImpl<List<String>>) settingBuilder("setting.depending.name", listOf(STRING))
                .addConstraint(dependency(
                        SettingConstraints.size(2), SettingConstraints.size(4), enumSetting, is(Colors.BLUE)))
                .build();
        var dependencySetting2 = (SettingImpl<Integer>) settingBuilder("setting.depending.name", INT)
                .addConstraint(dependency(max(3), max(7), intSetting, min(3)))
                .build();

        // Then
        assertEquals("setting.name, a long that is power of 2.", oneConstraintSetting.description());
        assertEquals(
                "setting.name, an integer that is minimum `2` and is maximum `10`.",
                twoConstraintSetting.description());
        assertEquals(
                "setting.depending.name, a comma-separated list where each element is a string, which depends on setting.name."
                        + " If setting.name is `BLUE` then it is of size `2` otherwise it is of size `4`.",
                dependencySetting1.description());
        assertEquals(
                "setting.depending.name, an integer that depends on setting.name."
                        + " If setting.name is minimum `3` then it is maximum `3` otherwise it is maximum `7`.",
                dependencySetting2.description());
    }

    @Test
    void testListOfEnums() {
        var enumSetting = (SettingImpl<List<Colors>>)
                SettingImpl.newBuilder("setting.name", listOf(ofEnum(Colors.class)), List.of(Colors.GREEN))
                        .build();

        var parsedSetting = enumSetting.parse("red, blue");
        assertEquals(2, parsedSetting.size());
        assertTrue(parsedSetting.containsAll(List.of(Colors.BLUE, Colors.RED)));
        assertTrue(enumSetting.parse("").isEmpty());
        assertEquals(
                "setting.name, a comma-separated list where each element is one of [BLUE, GREEN, RED].",
                enumSetting.description());
        assertEquals(List.of(Colors.GREEN), enumSetting.defaultValue());
        assertThrows(IllegalArgumentException.class, () -> enumSetting.parse("blue, kaputt"));
    }

    @Test
    void testSetOfEnums() {
        var enumSetting = (SettingImpl<Set<Colors>>)
                SettingImpl.newBuilder("setting.name", setOfEnums(Colors.class), EnumSet.of(Colors.GREEN))
                        .build();

        var parsedSetting = enumSetting.parse("red, blue, red");
        assertEquals(2, parsedSetting.size());
        assertTrue(parsedSetting.containsAll(List.of(Colors.BLUE, Colors.RED)));
        assertTrue(enumSetting.parse("").isEmpty());
        assertEquals(
                "setting.name, a comma-separated set where each element is one of [BLUE, GREEN, RED].",
                enumSetting.description());
        assertEquals(Set.of(Colors.GREEN), enumSetting.defaultValue());
        assertThrows(IllegalArgumentException.class, () -> enumSetting.parse("blue, kaputt"));
    }

    @Test
    void testNoDuplicatesConstraint() {
        var setting = (SettingImpl<List<String>>) settingBuilder("setting", listOf(STRING))
                .addConstraint(noDuplicates())
                .build();
        assertDoesNotThrow(() -> setting.validate(List.of("a", "b"), EMPTY));
        assertDoesNotThrow(() -> setting.validate(List.of(), EMPTY));

        var exception =
                assertThrows(IllegalArgumentException.class, () -> setting.validate(List.of("a", "b", "b"), EMPTY));
        assertEquals(
                "Failed to validate '[a, b, b]' for 'setting': items should not have duplicates: a,b,b",
                exception.getMessage());
    }

    @TestFactory
    Collection<DynamicTest> testDescriptionDependency() {
        Collection<DynamicTest> tests = new ArrayList<>();
        tests.add(dynamicTest(
                "Test int dependency description",
                () -> testDescDependency(
                        INT, "setting.child, an integer. If unset, the value is inherited from setting.parent.")));
        tests.add(dynamicTest(
                "Test socket dependency description",
                () -> testDescDependency(
                        SOCKET_ADDRESS,
                        "setting.child, a socket address in the format of `hostname:port`, `hostname`, or `:port`. "
                                + "If missing, it is acquired from setting.parent.")));
        tests.add(dynamicTest(
                "Test path dependency description",
                () -> testDescDependency(
                        PATH, "setting.child, a path. If relative, it is resolved from setting.parent.")));
        return tests;
    }

    private static <T> void testDescDependency(SettingValueParser<T> parser, String expectedDescription) {
        var parent = settingBuilder("setting.parent", parser).immutable().build();
        var child =
                settingBuilder("setting.child", parser).setDependency(parent).build();

        assertEquals(expectedDescription, child.description());
    }

    private static <T> SettingBuilder<T> settingBuilder(String name, SettingValueParser<T> parser) {
        return SettingImpl.newBuilder(name, parser, null);
    }

    private static <T> SettingImpl<T> setting(String name, SettingValueParser<T> parser) {
        return (SettingImpl<T>) SettingImpl.newBuilder(name, parser, null).build();
    }

    private enum Colors {
        BLUE,
        GREEN,
        RED;
    }

    private enum StringEnum {
        DEFAULT("default"),
        V_1("1.0"),
        V_1_1("1.1");

        private final String name;

        StringEnum(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    private static final SettingValueParser<Integer> POSITIVE_INT = new SettingValueParser<>() {
        @Override
        public Integer parse(String value) {
            return INT.parse(value);
        }

        @Override
        public String getDescription() {
            return "a positive integer";
        }

        @Override
        public Class<Integer> getType() {
            return Integer.class;
        }

        @Override
        public void validate(Integer value) {
            if (value <= 0) {
                throw new IllegalArgumentException("value %d is negative".formatted(value));
            }
        }
    };
}
