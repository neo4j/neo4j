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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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
import static org.neo4j.configuration.SettingConstraints.mutuallyExclusiveWith;
import static org.neo4j.configuration.SettingConstraints.noDuplicates;
import static org.neo4j.configuration.SettingConstraints.range;
import static org.neo4j.configuration.SettingConstraints.resolution;
import static org.neo4j.configuration.SettingConstraints.valueDependency;
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
import static org.neo4j.configuration.SettingValueParsers.MAP_PATTERN;
import static org.neo4j.configuration.SettingValueParsers.NORMALIZED_RELATIVE_URI;
import static org.neo4j.configuration.SettingValueParsers.PATH;
import static org.neo4j.configuration.SettingValueParsers.SECURE_STRING;
import static org.neo4j.configuration.SettingValueParsers.SOCKET_ADDRESS;
import static org.neo4j.configuration.SettingValueParsers.SOCKET_ADDRESS_ONLY_HOST_NAME;
import static org.neo4j.configuration.SettingValueParsers.STRING;
import static org.neo4j.configuration.SettingValueParsers.TIMEZONE;
import static org.neo4j.configuration.SettingValueParsers.TRUE;
import static org.neo4j.configuration.SettingValueParsers.UNSIGNED_BYTE;
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
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.io.ByteUnit;

class SettingTest {
    @Test
    void testInteger() {
        var setting = setting("setting", INT);
        assertThat(setting.parse("5")).isEqualTo(5);
        assertThat(setting.parse(" 5 ")).isEqualTo(5);
        assertThat(setting.parse("-76")).isEqualTo(-76);
        assertThatThrownBy(() -> setting.parse("foo")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testLong() {
        var setting = setting("setting", LONG);
        assertThat(setting.parse("112233445566778899")).isEqualTo(112233445566778899L);
        assertThat(setting.parse(" 112233445566778899 ")).isEqualTo(112233445566778899L);
        assertThat(setting.parse("-112233445566778899")).isEqualTo(-112233445566778899L);
        assertThatThrownBy(() -> setting.parse("foo")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testString() {
        var setting = setting("setting", STRING);
        assertThat(setting.parse("foo")).isEqualTo("foo");
        assertThat(setting.parse("  bar   ")).isEqualTo("bar");
    }

    @Test
    void testSecureString() {
        var setting = setting("setting", SECURE_STRING);
        assertThat(setting.parse("foo").getString()).isEqualTo("foo");
        assertThat(setting.parse("foo").toString()).isNotEqualTo("foo");
        assertThat(setting.parse("  bar   ").getString()).isEqualTo("bar");
        assertThat(setting.valueToString(setting.parse("foo"))).isNotEqualTo("foo");
    }

    @Test
    void testDouble() {
        BiFunction<Double, Double, Boolean> compareDoubles = (Double d1, Double d2) -> Math.abs(d1 - d2) < 0.000001;

        var setting = setting("setting", DOUBLE);
        assertThat(setting.parse("5")).isEqualTo(5.0);
        assertThat(setting.parse("  5 ")).isEqualTo(5.0);
        assertThat(compareDoubles.apply(-.123, setting.parse("-0.123"))).isTrue();
        assertThat(compareDoubles.apply(5.0, setting.parse("5"))).isTrue();
        assertThatThrownBy(() -> setting.parse("foo")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testList() {
        var setting = setting("setting", listOf(INT));
        assertThat(setting.parse("5").get(0)).isEqualTo(5);
        assertThat(setting.parse("")).isEmpty();
        assertThat(setting.parse("5, 31, -4  ,2")).hasSize(4);
        assertThat(setting.parse("4,2,3,1")).isEqualTo(Arrays.asList(4, 2, 3, 1));
        assertThatThrownBy(() -> setting.parse("2,3,foo,7")).isInstanceOf(IllegalArgumentException.class);

        assertThat(setting.valueToString(setting.parse("4,2,3,1"))).doesNotStartWith("[");
        assertThat(setting.valueToString(setting.parse("4,2,3,1"))).doesNotEndWith("]");
    }

    @Test
    void testListValidation() {
        var setting = setting("setting", listOf(POSITIVE_INT));
        assertThatCode(() -> setting.validate(List.of(), EMPTY)).doesNotThrowAnyException();
        assertThatCode(() -> setting.validate(List.of(5), EMPTY)).doesNotThrowAnyException();
        assertThatCode(() -> setting.validate(List.of(1, 2, 3), EMPTY)).doesNotThrowAnyException();
        assertThatThrownBy(() -> setting.validate(List.of(1, -2, 3), EMPTY))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testSet() {
        var setting = setting("setting", setOf(INT));
        assertThat(setting.parse("5")).containsExactly(5);
        assertThat(setting.parse("")).isEmpty();
        assertThat(setting.parse("5, 31, -4  ,2")).containsExactlyInAnyOrder(5, 31, -4, 2);
        assertThat(setting.parse("5, 5, 5, 3, 900, 0")).containsExactlyInAnyOrder(0, 3, 5, 900);
        assertThatThrownBy(() -> setting.parse("2,3,foo,7")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testSetValidation() {
        var setting = setting("setting", setOf(POSITIVE_INT));
        assertThatCode(() -> setting.validate(Set.of(), EMPTY)).doesNotThrowAnyException();
        assertThatCode(() -> setting.validate(Set.of(5), EMPTY)).doesNotThrowAnyException();
        assertThatCode(() -> setting.validate(Set.of(1, 2, 3), EMPTY)).doesNotThrowAnyException();
        assertThatThrownBy(() -> setting.validate(Set.of(1, -2, 3), EMPTY))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testEnum() {
        var setting = setting("setting", ofEnum(Colors.class));
        assertThat(setting.parse("BLUE")).isEqualTo(Colors.BLUE);
        assertThat(setting.parse("gReEn")).isEqualTo(Colors.GREEN);
        assertThat(setting.parse("red")).isEqualTo(Colors.RED);
        assertThat(setting.parse(" red ")).isEqualTo(Colors.RED);
        assertThatThrownBy(() -> setting.parse("orange")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testPartialEnum() {
        var setting = setting("setting", ofPartialEnum(Colors.GREEN, Colors.BLUE));
        assertThat(setting.parse("BLUE")).isEqualTo(Colors.BLUE);
        assertThat(setting.parse("gReEn")).isEqualTo(Colors.GREEN);
        assertThatThrownBy(() -> setting.parse("red")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testStringEnum() {
        var setting = setting("setting", ofEnum(StringEnum.class));
        assertThat(setting.parse("default")).isEqualTo(StringEnum.DEFAULT);
        assertThat(setting.parse("1.0")).isEqualTo(StringEnum.V_1);
        assertThat(setting.parse("1.1")).isEqualTo(StringEnum.V_1_1);
        assertThatThrownBy(() -> setting.parse("orange")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testBool() {
        var setting = setting("setting", BOOL);
        assertThat(setting.parse("True")).isTrue();
        assertThat(setting.parse("false")).isFalse();
        assertThat(setting.parse(FALSE)).isFalse();
        assertThat(setting.parse(TRUE)).isTrue();
        assertThat(setting.parse(" true ")).isTrue();
        assertThat(setting.parse("  false")).isFalse();
        assertThatThrownBy(() -> setting.parse("foo")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testUnsignedByte() {
        var setting = setting("setting", UNSIGNED_BYTE);
        assertThat(setting.parse("0")).isEqualTo((byte) 0);
        assertThat(setting.parse("127")).isEqualTo((byte) 127);
        assertThat(setting.parse("128")).isEqualTo((byte) -128);
        assertThat(setting.parse("255")).isEqualTo((byte) -1);
        assertThatThrownBy(() -> setting.parse("-1")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> setting.parse("256")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> setting.parse("foo")).isInstanceOf(IllegalArgumentException.class);

        assertThat(setting.valueToString(setting.parse("0"))).isEqualTo("0");
        assertThat(setting.valueToString(setting.parse("127"))).isEqualTo("127");
        assertThat(setting.valueToString(setting.parse("128"))).isEqualTo("128");
        assertThat(setting.valueToString(setting.parse("255"))).isEqualTo("255");
    }

    @Test
    void testMapPattern() {
        var setting = setting("setting", MAP_PATTERN);
        assertThat(setting.parse("a=1")).isEqualTo(Map.of("a", "1"));
        assertThat(setting.parse("a=1;b=2")).isEqualTo(Map.of("a", "1", "b", "2"));
        assertThat(setting.parse("a=1;a=1")).isEqualTo(Map.of("a", "1"));
        assertThatThrownBy(() -> setting.parse("a")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> setting.parse("1")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> setting.parse("a=1;b")).isInstanceOf(IllegalArgumentException.class);

        assertThat(setting.valueToString(setting.parse("a=1"))).isEqualTo("a=1");
        assertThat(setting.valueToString(setting.parse("a=1;b=2"))).isEqualTo("a=1;b=2");
        assertThat(setting.valueToString(setting.parse("a=1;a=1"))).isEqualTo("a=1");
    }

    @Test
    void testDuration() {
        var setting = setting("setting", DURATION);
        assertThat(setting.parse("1m").toSeconds()).isEqualTo(60);
        assertThat(setting.parse(" 1m ").toSeconds()).isEqualTo(60);
        assertThat(setting.parse("1s").toMillis()).isEqualTo(1000);
        assertThatThrownBy(() -> setting.parse("foo")).isInstanceOf(IllegalArgumentException.class);

        assertThat(setting.valueToString(setting.parse("1s"))).isEqualTo("1s");
        assertThat(setting.valueToString(setting.parse("3m"))).isEqualTo("3m");

        // Anything less than a millisecond is rounded down
        assertThat(setting.valueToString(setting.parse("0s"))).isEqualTo("0s");
        assertThat(setting.valueToString(setting.parse("1ns"))).isEqualTo("0s");
        assertThat(setting.valueToString(setting.parse("999999ns"))).isEqualTo("0s");
        assertThat(setting.valueToString(setting.parse("999μs"))).isEqualTo("0s");

        // Time strings containing multiple units are permitted
        assertThat(setting.valueToString(setting.parse("11d19h25m4s50ms607μs80ns")))
                .isEqualTo("11d19h25m4s50ms");
        // Weird time strings will be converted to something more readable
        assertThat(setting.valueToString(setting.parse("1m60000ms1000000ns"))).isEqualTo("2m1ms");

        String descriptionWithConstraint = SettingImpl.newBuilder("setting", DURATION, ofMinutes(1))
                .addConstraint(min(Duration.ofSeconds(10)))
                .build()
                .description();

        String expected =
                "setting, a duration (Valid units are: `ns`, `μs`, `ms`, `s`, `m`, `h` and `d`; default unit is `s`) that is minimum `10s`.";
        assertThat(descriptionWithConstraint).isEqualTo(expected);
    }

    @Test
    void testDurationRange() {
        var setting = setting("setting", DURATION_RANGE);
        assertThat(setting.parse("1m-2m").getMin().toSeconds()).isEqualTo(60);
        assertThat(setting.parse("1m-2m").getMax().toSeconds()).isEqualTo(120);
        assertThat(setting.parse(" 1m-2m ").getMin().toSeconds()).isEqualTo(60);
        assertThat(setting.parse(" 1m-2m ").getMax().toSeconds()).isEqualTo(120);
        assertThat(setting.parse("1s-2s").getMin().toMillis()).isEqualTo(1000);
        assertThat(setting.parse("1s-2s").getMax().toMillis()).isEqualTo(2000);
        assertThatThrownBy(() -> setting.parse("1s")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> setting.parse("1s-")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> setting.parse("-1s")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> setting.parse("-1s--2s")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> setting.parse("2s-1s")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> setting.parse("2000ms-1s")).isInstanceOf(IllegalArgumentException.class);

        // DurationRange may have zero delta
        assertThat(setting.parse("1s-1s").getMin().toSeconds()).isEqualTo(1);
        assertThat(setting.parse("1s-1s").getMax().toSeconds()).isEqualTo(1);
        assertThat(setting.parse("1s-1s").getDelta().toNanos()).isEqualTo(0);

        assertThat(setting.valueToString(setting.parse("0s-0s"))).isEqualTo("0ns-0ns");
        assertThat(setting.valueToString(setting.parse("1s-2s"))).isEqualTo("1s-2s");
        assertThat(setting.valueToString(setting.parse("[3m-6m]"))).isEqualTo("3m-6m");

        // Time strings containing multiple units are permitted
        assertThat(setting.valueToString(setting.parse("0s-1m23s456ms"))).isEqualTo("0ns-1m23s456ms");

        // Units will be converted to something "more readable"
        assertThat(setting.valueToString(setting.parse("1000ms-2500ms"))).isEqualTo("1s-2s500ms");

        // Anything less than a millisecond is rounded down
        assertThat(setting.valueToString(setting.parse("999μs-999999ns"))).isEqualTo("0ns-0ns");
        assertThat(setting.parse("999μs-999999ns").getDelta().toNanos()).isEqualTo(0);
    }

    @Test
    void testHostnamePort() {
        var setting = setting("setting", HOSTNAME_PORT);
        assertThat(setting.parse("localhost:7474")).isEqualTo(new HostnamePort("localhost", 7474));
        assertThat(setting.parse("localhost:1000-2000")).isEqualTo(new HostnamePort("localhost", 1000, 2000));
        assertThat(setting.parse("localhost")).isEqualTo(new HostnamePort("localhost"));
        assertThatThrownBy(() -> setting.parse("localhost:5641:7474")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> setting.parse("localhost:foo")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> setting.parse("7474:localhost")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testTimeZone() {
        var setting = setting("setting", TIMEZONE);
        assertThat(setting.parse("+00:00")).isEqualTo(ZoneId.from(ZoneOffset.UTC));
        assertThat(setting.parse(" +00:00 ")).isEqualTo(ZoneId.from(ZoneOffset.UTC));
        assertThatThrownBy(() -> setting.parse("foo")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testCidrIp() {
        var setting = setting("setting", CIDR_IP);
        assertThat(setting.parse("1.1.1.0/8")).isEqualTo(new IPAddressString("1.1.1.0/8"));
        assertThatThrownBy(() -> setting.parse("garbage")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testSocket() {
        var setting = setting("setting", SOCKET_ADDRESS);
        assertThat(setting.parse("127.0.0.1:7474")).isEqualTo(new SocketAddress("127.0.0.1", 7474));
        assertThat(setting.parse("[127.0.0.1]:7474")).isEqualTo(new SocketAddress("127.0.0.1", 7474));
        assertThat(setting.parse(" 127.0.0.1:7474 ")).isEqualTo(new SocketAddress("127.0.0.1", 7474));
        assertThat(setting.parse("127.0.0.1")).isEqualTo(new SocketAddress("127.0.0.1", -1));
        assertThat(setting.parse("[127.0.0.1]")).isEqualTo(new SocketAddress("127.0.0.1", -1));
        assertThat(setting.parse(":7474")).isEqualTo(new SocketAddress(null, 7474));
        assertThat(setting.parse("fd01::9419:4c0e:be04:f0e3:4332"))
                .isEqualTo(new SocketAddress("fd01::9419:4c0e:be04:f0e3", 4332));
        assertThat(setting.parse("[fd01::9419:4c0e:be04:f0e3:4332]"))
                .isEqualTo(new SocketAddress("fd01::9419:4c0e:be04:f0e3:4332", -1));
        assertThat(setting.parse("[fd01::9419:4c0e:be04:f0e3]:4332"))
                .isEqualTo(new SocketAddress("fd01::9419:4c0e:be04:f0e3", 4332));
    }

    @Test
    void testSocketSolve() {
        var setting = setting("setting", SOCKET_ADDRESS);
        assertThat(setting.solveDependency(setting.parse("localhost:7473"), setting.parse("127.0.0.1:7474")))
                .isEqualTo(new SocketAddress("localhost", 7473));
        assertThat(setting.solveDependency(setting.parse(":7473"), setting.parse("127.0.0.1:7474")))
                .isEqualTo(new SocketAddress("127.0.0.1", 7473));
        assertThat(setting.solveDependency(setting.parse(":7473"), setting.parse("127.0.0.1")))
                .isEqualTo(new SocketAddress("127.0.0.1", 7473));
        assertThat(setting.solveDependency(setting.parse("localhost"), setting.parse(":7474")))
                .isEqualTo(new SocketAddress("localhost", 7474));
        assertThat(setting.solveDependency(setting.parse("localhost"), setting.parse("127.0.0.1:7474")))
                .isEqualTo(new SocketAddress("localhost", 7474));
        assertThat(setting.solveDependency(null, setting.parse("localhost:7474")))
                .isEqualTo(new SocketAddress("localhost", 7474));
    }

    @Test
    void testSocketOnlyHostname() {
        var setting = setting("setting", SOCKET_ADDRESS_ONLY_HOST_NAME);
        assertThat(setting.parse("127.0.0.1")).isEqualTo(new SocketAddress("127.0.0.1", -1));
        assertThat(setting.parse(" 127.0.0.1 ")).isEqualTo(new SocketAddress("127.0.0.1", -1));
        assertThat(setting.parse("[127.0.0.1]")).isEqualTo(new SocketAddress("127.0.0.1", -1));
        assertThat(setting.parse("fd01::9419:4c0e:be04:f0e3:4332"))
                .isEqualTo(new SocketAddress("fd01::9419:4c0e:be04:f0e3:4332", -1));
    }

    @Test
    void testSocketOnlyHostnameSolve() {
        var parent = setting("parent", SOCKET_ADDRESS_ONLY_HOST_NAME);
        var child = setting("child", SOCKET_ADDRESS);
        assertThat(child.solveDependency(child.parse("localhost:7473"), parent.parse("127.0.0.1")))
                .isEqualTo(new SocketAddress("localhost", 7473));
        assertThat(child.solveDependency(child.parse(":7473"), parent.parse("127.0.0.1")))
                .isEqualTo(new SocketAddress("127.0.0.1", 7473));
        assertThat(child.solveDependency(child.parse("localhost"), parent.parse("127.0.0.1")))
                .isEqualTo(new SocketAddress("localhost", -1));
        assertThat(child.solveDependency(null, parent.parse("localhost")))
                .isEqualTo(new SocketAddress("localhost", -1));
    }

    @Test
    void testBytes() {
        var setting = setting("setting", BYTES);
        assertThat(setting.parse("2k")).isEqualTo(2048);
        assertThatThrownBy(() -> setting.parse("1gig")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> setting.parse("-1M")).isInstanceOf(IllegalArgumentException.class);

        String descriptionWithConstraint = SettingImpl.newBuilder("setting", BYTES, ByteUnit.gibiBytes(2))
                .addConstraint(range(ByteUnit.mebiBytes(100), ByteUnit.gibiBytes(10)))
                .build()
                .description();

        String expected =
                "setting, a byte size (valid multipliers are `B`, `KiB`, `KB`, `K`, `kB`, `kb`, `k`, `MiB`, `MB`, `M`, `mB`, `mb`, `m`, "
                        + "`GiB`, `GB`, `G`, `gB`, `gb`, `g`, `TiB`, `TB`, `PiB`, `PB`, `EiB`, `EB`) that is in the range `100.00MiB` to `10.00GiB`.";
        assertThat(descriptionWithConstraint).isEqualTo(expected);
    }

    @Test
    void testURI() {
        var setting = setting("setting", SettingValueParsers.URI);
        assertThat(setting.parse("/path/to/../something/")).isEqualTo(URI.create("/path/to/../something/"));
    }

    @Test
    void testHttpsURI() {
        var setting = setting("setting", SettingValueParsers.HTTPS_URI(true));
        assertThat(setting.parse("https://www.example.com/path")).isEqualTo(URI.create("https://www.example.com/path"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"http://www.example.com", "neo4js://database", "/path/to/../something/"})
    void testHttpsURIWithInvalidUris(String uri) {
        var setting = setting("setting", SettingValueParsers.HTTPS_URI(true));
        assertThatThrownBy(() -> setting.parse(uri))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(String.format("'%s' does not have required scheme 'https'", uri));
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
        var setting = setting("setting", SettingValueParsers.HTTPS_URI(true));
        assertThat(setting.parse(uri)).isEqualTo(URI.create(uri));

        var invalidSetting = setting("setting", SettingValueParsers.HTTPS_URI(false));
        assertThatThrownBy(() -> invalidSetting.parse(uri))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(String.format("'%s' does not have required scheme 'https'", uri));
    }

    @Test
    void testStringMapWithNoConstraintOnKeys() {
        var setting = setting("setting", SettingValueParsers.MAP_PATTERN);
        assertThat(setting.parse("k1=v1;k2=v2")).isEqualTo(Map.of("k1", "v1", "k2", "v2"));
    }

    @Test
    void testStringMapWithValuesContainingEquals() {
        var setting = setting("setting", SettingValueParsers.MAP_PATTERN);
        assertThat(setting.parse("k1=cn=admin,dc=example,dc=com;k2=v2"))
                .isEqualTo(Map.of("k1", "cn=admin,dc=example,dc=com", "k2", "v2"));
    }

    @Test
    void testStringMapWithRequiredKeys() {
        var setting = setting("setting", new SettingValueParsers.MapPattern(Set.of("k1", "k2")));
        assertThat(setting.parse("k1=v1;k2=v2;k3=v3")).isEqualTo(Map.of("k1", "v1", "k2", "v2", "k3", "v3"));
        assertThatThrownBy(() -> setting.parse("k1=v1;k3=v3")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testStringMapWithRestrictedKeys() {
        var setting = setting("setting", new SettingValueParsers.MapPattern(Set.of("k1"), Set.of("k1", "k2")));
        assertThat(setting.parse("k1=v1;k2=v2")).isEqualTo(Map.of("k1", "v1", "k2", "v2"));
        assertThat(setting.parse("k1=v1")).isEqualTo(Map.of("k1", "v1"));
        assertThatThrownBy(() -> setting.parse("k2=v2")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> setting.parse("k1=v1;k3=v3")).isInstanceOf(IllegalArgumentException.class);
        var settingWithoutRequired = setting("setting", new SettingValueParsers.MapPattern(null, Set.of("k1", "k2")));
        assertThat(settingWithoutRequired.parse("k2=v2")).isEqualTo(Map.of("k2", "v2"));
    }

    @Test
    void testNormalizedRelativeURI() {
        var setting = setting("setting", NORMALIZED_RELATIVE_URI);
        assertThat(setting.parse("/path/away/from/../../to/something/")).isEqualTo(URI.create("/path/to/something"));
    }

    @Test
    void testPath() {
        var setting = setting("setting", PATH);
        assertThat(setting.parse("/absolute/path")).isEqualTo(Path.of("/absolute/path"));
        assertThat(setting.parse("/absolute/wrong/../path")).isEqualTo(Path.of("/absolute/path"));
        assertThat(setting.parse("\test\\escaped\\chars\r\n\\\\dir")).isEqualTo(Path.of("/test/escaped/chars/r/n/dir"));
    }

    @Test
    void testSolvePath() {
        var setting = setting("setting", PATH);
        assertThat(setting.solveDependency(
                        setting.parse("to/file"), setting.parse("/base/path").toAbsolutePath()))
                .isEqualTo(Path.of("/base/path/to/file").toAbsolutePath());
        assertThat(setting.solveDependency(
                        setting.parse("/to/file"), setting.parse("/base/path").toAbsolutePath()))
                .isEqualTo(Path.of("/to/file").toAbsolutePath());
        assertThat(setting.solveDependency(
                        setting.parse(""), setting.parse("/base/path/").toAbsolutePath()))
                .isEqualTo(Path.of("/base/path/").toAbsolutePath());
        assertThat(setting.solveDependency(
                        setting.parse("path"), setting.parse("/base").toAbsolutePath()))
                .isEqualTo(Path.of("/base/path").toAbsolutePath());
        assertThat(setting.solveDependency(null, setting.parse("/base").toAbsolutePath()))
                .isEqualTo(Path.of("/base").toAbsolutePath());
        assertThatThrownBy(() -> setting.solveDependency(setting.parse("path"), setting.parse("base")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testJvmAdditional() {
        var setting = setting("setting", JVM_ADDITIONAL);
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
        assertThat(actualSettings).isEqualTo(expectedSettings);
    }

    @Test
    void testJvmAdditionalBadQuoting() {
        // A JVM setting starting with a quote should have an end quote
        var setting = setting("setting", JVM_ADDITIONAL);
        assertThatThrownBy(() -> setting.parse("\"missing_end_quote")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testJvmAdditionalWithProperty() {
        var setting = setting("setting", JVM_ADDITIONAL);
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

        var setting = setting("setting", defaultSolver);
        assertThat(setting.solveDependency("foo", "bar")).isEqualTo("foo");
        assertThat(setting.solveDependency(null, "bar")).isEqualTo("bar");
        assertThat(setting.solveDependency("foo", null)).isEqualTo("foo");
        assertThat(setting.solveDependency(null, null)).isNull();
    }

    @Test
    void testMinConstraint() {
        var setting = (SettingImpl<Integer>)
                settingBuilder("setting", INT).addConstraint(min(10)).build();
        assertThatCode(() -> setting.validate(100, EMPTY)).doesNotThrowAnyException();
        assertThatCode(() -> setting.validate(10, EMPTY)).doesNotThrowAnyException();
        assertThatThrownBy(() -> setting.validate(9, EMPTY)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testMaxConstraint() {
        var setting = (SettingImpl<Integer>)
                settingBuilder("setting", INT).addConstraint(max(10)).build();
        assertThatCode(() -> setting.validate(-100, EMPTY)).doesNotThrowAnyException();
        assertThatCode(() -> setting.validate(10, EMPTY)).doesNotThrowAnyException();
        assertThatThrownBy(() -> setting.validate(11, EMPTY)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testRangeConstraint() {
        var setting = (SettingImpl<Double>) settingBuilder("setting", DOUBLE)
                .addConstraint(range(10.0, 20.0))
                .build();

        assertThatThrownBy(() -> setting.validate(9.9, EMPTY)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> setting.validate(20.01, EMPTY)).isInstanceOf(IllegalArgumentException.class);
        assertThatCode(() -> setting.validate(10.1, EMPTY)).doesNotThrowAnyException();
        assertThatCode(() -> setting.validate(19.9999, EMPTY)).doesNotThrowAnyException();
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
        assertThatCode(() -> mustBeLessSetting.validate(-1, simpleConfig)).doesNotThrowAnyException();
        assertThatCode(() -> mustBeLessSetting.validate(0, simpleConfig)).doesNotThrowAnyException();
        assertThatCode(() -> mustBeLessSetting.validate(1, simpleConfig)).doesNotThrowAnyException();
        assertThatCode(() -> mustBeLessSetting.validate(5, simpleConfig)).doesNotThrowAnyException();
        assertThatThrownBy(() -> mustBeLessSetting.validate(6, simpleConfig))
                .isInstanceOf(IllegalArgumentException.class);

        // When
        var mustBeLessThanHalfSetting = (SettingImpl<Integer>) settingBuilder("less.than.half.int", INT)
                .addConstraint(lessThanOrEqual(i -> (long) i, intLimit, i -> i / 2, "divided by 2"))
                .build();
        // Then
        assertThatCode(() -> mustBeLessThanHalfSetting.validate(-1, simpleConfig))
                .doesNotThrowAnyException();
        assertThatCode(() -> mustBeLessThanHalfSetting.validate(0, simpleConfig))
                .doesNotThrowAnyException();
        assertThatCode(() -> mustBeLessThanHalfSetting.validate(2, simpleConfig))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> mustBeLessThanHalfSetting.validate(3, simpleConfig))
                .isInstanceOf(IllegalArgumentException.class);

        // When
        var mustBeLessDuration = (SettingImpl<Duration>) settingBuilder("less.than.duration", DURATION)
                .addConstraint(lessThanOrEqual(Duration::toMillis, durationLimit))
                .build();
        // Then
        assertThatCode(() -> mustBeLessDuration.validate(Duration.ofSeconds(-1), simpleConfig))
                .doesNotThrowAnyException();
        assertThatCode(() -> mustBeLessDuration.validate(Duration.ofSeconds(0), simpleConfig))
                .doesNotThrowAnyException();
        assertThatCode(() -> mustBeLessDuration.validate(Duration.ofMinutes(1), simpleConfig))
                .doesNotThrowAnyException();
        assertThatCode(() -> mustBeLessDuration.validate(Duration.ofSeconds(123), simpleConfig))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> mustBeLessDuration.validate(Duration.ofMillis(123001), simpleConfig))
                .isInstanceOf(IllegalArgumentException.class);

        // When
        var mustBeLessThanHalfDuration = (SettingImpl<Duration>) settingBuilder("less.than.duration", DURATION)
                .addConstraint(lessThanOrEqual(Duration::toMillis, durationLimit, i -> i / 2, "divided by 2"))
                .build();
        // Then
        assertThatCode(() -> mustBeLessThanHalfDuration.validate(Duration.ofSeconds(-1), simpleConfig))
                .doesNotThrowAnyException();
        assertThatCode(() -> mustBeLessThanHalfDuration.validate(Duration.ofSeconds(0), simpleConfig))
                .doesNotThrowAnyException();
        assertThatCode(() -> mustBeLessThanHalfDuration.validate(Duration.ofMinutes(1), simpleConfig))
                .doesNotThrowAnyException();
        assertThatCode(() -> mustBeLessThanHalfDuration.validate(Duration.ofSeconds(61), simpleConfig))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> mustBeLessThanHalfDuration.validate(Duration.ofMillis(61501), simpleConfig))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testExceptConstraint() {
        var setting = (SettingImpl<String>)
                settingBuilder("setting", STRING).addConstraint(except("foo")).build();
        assertThatThrownBy(() -> setting.validate("foo", EMPTY)).isInstanceOf(IllegalArgumentException.class);
        assertThatCode(() -> setting.validate("bar", EMPTY)).doesNotThrowAnyException();
    }

    @Test
    void testMatchesConstraint() {
        var setting = (SettingImpl<String>) settingBuilder("setting", STRING)
                .addConstraint(matches("^[^.]+\\.[^.]+$"))
                .build();
        assertThatCode(() -> setting.validate("foo.bar", EMPTY)).doesNotThrowAnyException();
        assertThatThrownBy(() -> setting.validate("foo", EMPTY)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testPowerOf2Constraint() {
        var setting = (SettingImpl<Long>)
                settingBuilder("setting", LONG).addConstraint(POWER_OF_2).build();
        assertThatCode(() -> setting.validate(8L, EMPTY)).doesNotThrowAnyException();
        assertThatCode(() -> setting.validate(4294967296L, EMPTY)).doesNotThrowAnyException();
        assertThatThrownBy(() -> setting.validate(1023L, EMPTY)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testIsConstraint() {
        var setting = (SettingImpl<Integer>)
                settingBuilder("setting", INT).addConstraint(is(10)).build();
        assertThatCode(() -> setting.validate(10, EMPTY)).doesNotThrowAnyException();
        assertThatThrownBy(() -> setting.validate(9, EMPTY)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testAnyConstraint() {
        var intSetting = (SettingImpl<Integer>) settingBuilder("setting", INT)
                .addConstraint(any(min(30), is(0), is(-10)))
                .build();
        assertThatCode(() -> intSetting.validate(30, EMPTY)).doesNotThrowAnyException();
        assertThatCode(() -> intSetting.validate(100, EMPTY)).doesNotThrowAnyException();
        assertThatCode(() -> intSetting.validate(0, EMPTY)).doesNotThrowAnyException();
        assertThatCode(() -> intSetting.validate(-10, EMPTY)).doesNotThrowAnyException();
        assertThatThrownBy(() -> intSetting.validate(29, EMPTY)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> intSetting.validate(1, EMPTY)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> intSetting.validate(-9, EMPTY)).isInstanceOf(IllegalArgumentException.class);

        var durationSetting = (SettingImpl<Duration>) settingBuilder("setting", DURATION)
                .addConstraint(any(min(ofMinutes(30)), is(Duration.ZERO)))
                .build();
        assertThatCode(() -> durationSetting.validate(ofMinutes(30), EMPTY)).doesNotThrowAnyException();
        assertThatCode(() -> durationSetting.validate(Duration.ofHours(1), EMPTY))
                .doesNotThrowAnyException();
        assertThatCode(() -> durationSetting.validate(Duration.ZERO, EMPTY)).doesNotThrowAnyException();
        assertThatThrownBy(() -> durationSetting.validate(ofMinutes(29), EMPTY))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> durationSetting.validate(Duration.ofMillis(1), EMPTY))
                .isInstanceOf(IllegalArgumentException.class);

        String expected =
                "setting, a duration (Valid units are: `ns`, `μs`, `ms`, `s`, `m`, `h` and `d`; default unit is `s`) that is minimum `30m` or is `0s`.";
        assertThat(durationSetting.description()).isEqualTo(expected);
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
        assertThatCode(() -> dependingIntSetting.validate(3, simpleConfig)).doesNotThrowAnyException();
        assertThatThrownBy(() -> dependingIntSetting.validate(4, simpleConfig))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatCode(() -> dependingEnumSetting.validate(List.of("a", "b"), simpleConfig))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> dependingEnumSetting.validate(List.of("a", "b", "c"), simpleConfig))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> dependingEnumSetting.validate(List.of("a", "b", "c", "d"), simpleConfig))
                .isInstanceOf(IllegalArgumentException.class);

        // When
        settings.put(intSetting, 2);
        settings.put(enumSetting, Colors.GREEN);
        // Then
        assertThatCode(() -> dependingIntSetting.validate(4, simpleConfig)).doesNotThrowAnyException();
        assertThatThrownBy(() -> dependingIntSetting.validate(8, simpleConfig))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatCode(() -> dependingEnumSetting.validate(List.of("a", "b", "c", "d"), simpleConfig))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> dependingEnumSetting.validate(List.of("a", "b"), simpleConfig))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> dependingEnumSetting.validate(List.of("a", "b", "c"), simpleConfig))
                .isInstanceOf(IllegalArgumentException.class);
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
        assertThat(oneConstraintSetting.description()).isEqualTo("setting.name, a long that is power of 2.");
        assertThat(twoConstraintSetting.description())
                .isEqualTo("setting.name, an integer that is minimum `2` and is maximum `10`.");
        assertThat(dependencySetting1.description())
                .isEqualTo(
                        "setting.depending.name, a comma-separated list where each element is a string, which depends on setting.name."
                                + " If setting.name is `BLUE` then it is of size `2` otherwise it is of size `4`.");
        assertThat(dependencySetting2.description())
                .isEqualTo("setting.depending.name, an integer that depends on setting.name."
                        + " If setting.name is minimum `3` then it is maximum `3` otherwise it is maximum `7`.");
    }

    @Test
    void testListOfEnums() {
        var enumSetting = (SettingImpl<List<Colors>>)
                SettingImpl.newBuilder("setting.name", listOf(ofEnum(Colors.class)), List.of(Colors.GREEN))
                        .build();

        var parsedSetting = enumSetting.parse("red, blue");
        assertThat(parsedSetting).hasSize(2);
        assertThat(parsedSetting).containsAll(List.of(Colors.BLUE, Colors.RED));
        assertThat(enumSetting.parse("")).isEmpty();
        assertThat(enumSetting.description())
                .isEqualTo("setting.name, a comma-separated list where each element is one of [BLUE, GREEN, RED].");
        assertThat(enumSetting.defaultValue()).isEqualTo(List.of(Colors.GREEN));
        assertThatThrownBy(() -> enumSetting.parse("blue, kaputt")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testSetOfEnums() {
        var enumSetting = (SettingImpl<Set<Colors>>)
                SettingImpl.newBuilder("setting.name", setOfEnums(Colors.class), EnumSet.of(Colors.GREEN))
                        .build();

        var parsedSetting = enumSetting.parse("red, blue, red");
        assertThat(parsedSetting).hasSize(2);
        assertThat(parsedSetting).containsAll(List.of(Colors.BLUE, Colors.RED));
        assertThat(enumSetting.parse("")).isEmpty();
        assertThat(enumSetting.description())
                .isEqualTo("setting.name, a comma-separated set where each element is one of [BLUE, GREEN, RED].");
        assertThat(enumSetting.defaultValue()).isEqualTo(Set.of(Colors.GREEN));
        assertThatThrownBy(() -> enumSetting.parse("blue, kaputt")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testNoDuplicatesConstraint() {
        var setting = (SettingImpl<List<String>>) settingBuilder("setting", listOf(STRING))
                .addConstraint(noDuplicates())
                .build();
        assertThatCode(() -> setting.validate(List.of("a", "b"), EMPTY)).doesNotThrowAnyException();
        assertThatCode(() -> setting.validate(List.of(), EMPTY)).doesNotThrowAnyException();

        assertThatThrownBy(() -> setting.validate(List.of("a", "b", "b"), EMPTY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Failed to validate '[a, b, b]' for 'setting': items should not have duplicates: a,b,b");
    }

    @Test
    void testValueDependencyConstraint() {
        var booleanSetting =
                (SettingImpl<Boolean>) settingBuilder("bool-setting", BOOL).build();
        Map<Setting<?>, Object> settings = new HashMap<>();
        Configuration simpleConfig = new Configuration() {
            @Override
            public <T> T get(Setting<T> setting) {
                return (T) settings.get(setting);
            }
        };

        var setting = (SettingImpl<Integer>) settingBuilder("setting", INT)
                .addConstraint(valueDependency(List.of(2, 3), booleanSetting))
                .build();

        // When
        settings.put(booleanSetting, java.lang.Boolean.TRUE);
        // Then
        assertThatCode(() -> setting.validate(1, simpleConfig)).doesNotThrowAnyException();
        assertThatCode(() -> setting.validate(2, simpleConfig)).doesNotThrowAnyException();

        // When
        settings.put(booleanSetting, java.lang.Boolean.FALSE);
        // Then
        assertThatCode(() -> setting.validate(1, simpleConfig)).doesNotThrowAnyException();
        assertThatThrownBy(() -> setting.validate(2, simpleConfig))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Failed to validate '2' for 'setting': 2 is not allowed since 'bool-setting' was false");
    }

    @TestFactory
    Collection<DynamicTest> testDescriptionDependency() {
        Collection<DynamicTest> tests = new ArrayList<>();
        tests.add(dynamicTest(
                "Test int dependency description",
                () -> testDescDependency(
                        INT, INT, "setting.child, an integer. If unset, the value is inherited from setting.parent.")));
        tests.add(dynamicTest(
                "Test socket dependency description",
                () -> testDescDependency(
                        SOCKET_ADDRESS,
                        SOCKET_ADDRESS,
                        "setting.child, a socket address in the format of `hostname:port`, `hostname`, or `:port`. "
                                + "If missing, it is acquired from setting.parent.")));
        tests.add(dynamicTest(
                "Test socket only hostname dependency description",
                () -> testDescDependency(
                        SOCKET_ADDRESS_ONLY_HOST_NAME,
                        SOCKET_ADDRESS,
                        "setting.child, a socket address in the format of `hostname:port`, `hostname`, or `:port`. "
                                + "If missing, it is acquired from setting.parent.")));
        tests.add(dynamicTest(
                "Test path dependency description",
                () -> testDescDependency(
                        PATH, PATH, "setting.child, a path. If relative, it is resolved from setting.parent.")));
        return tests;
    }

    private static <T> void testDescDependency(
            SettingValueParser<T> parentParser, SettingValueParser<T> childParser, String expectedDescription) {
        var parent = settingBuilder("setting.parent", parentParser).immutable().build();
        var child = settingBuilder("setting.child", childParser)
                .setDependency(parent)
                .build();

        assertThat(child.description()).isEqualTo(expectedDescription);
    }

    @Test
    void testResolutionConstraint() {
        final var nanos = resolutionSetting(ChronoUnit.NANOS);
        assertValidResolution(nanos, 1, ChronoUnit.MINUTES);
        assertValidResolution(nanos, 1, ChronoUnit.SECONDS);
        assertValidResolution(nanos, 1, ChronoUnit.NANOS);

        final var micros = resolutionSetting(ChronoUnit.MICROS);
        assertValidResolution(micros, 1, ChronoUnit.MINUTES);
        assertValidResolution(micros, 1, ChronoUnit.SECONDS);
        assertValidResolution(micros, 1, ChronoUnit.MICROS);
        assertValidResolution(micros, 1000, ChronoUnit.NANOS);
        assertInvalidResolution(micros, 999, ChronoUnit.NANOS, ChronoUnit.NANOS, ChronoUnit.MICROS);

        final var millis = resolutionSetting(ChronoUnit.MILLIS);
        assertValidResolution(millis, 1, ChronoUnit.MINUTES);
        assertValidResolution(millis, 1, ChronoUnit.SECONDS);
        assertValidResolution(millis, 1, ChronoUnit.MILLIS);
        assertInvalidResolution(millis, 999, ChronoUnit.MICROS, ChronoUnit.MICROS, ChronoUnit.MILLIS);
        assertValidResolution(millis, 1000, ChronoUnit.MICROS);
        assertInvalidResolution(millis, 1001, ChronoUnit.MICROS, ChronoUnit.MICROS, ChronoUnit.MILLIS);

        final var seconds = resolutionSetting(ChronoUnit.SECONDS);
        assertValidResolution(seconds, 1, ChronoUnit.MINUTES);
        assertValidResolution(seconds, 1, ChronoUnit.SECONDS);
        assertInvalidResolution(seconds, 999, ChronoUnit.MILLIS, ChronoUnit.MILLIS, ChronoUnit.SECONDS);
        assertValidResolution(seconds, 1000, ChronoUnit.MILLIS);
        assertInvalidResolution(seconds, 1001, ChronoUnit.MILLIS, ChronoUnit.MILLIS, ChronoUnit.SECONDS);

        final var minutes = resolutionSetting(ChronoUnit.MINUTES);
        assertValidResolution(minutes, 1, ChronoUnit.HOURS);
        assertValidResolution(minutes, 1, ChronoUnit.MINUTES);
        assertInvalidResolution(minutes, 59, ChronoUnit.SECONDS, ChronoUnit.SECONDS, ChronoUnit.MINUTES);
        assertValidResolution(minutes, 60, ChronoUnit.SECONDS);
        assertInvalidResolution(minutes, 61, ChronoUnit.SECONDS, ChronoUnit.SECONDS, ChronoUnit.MINUTES);
    }

    private static SettingImpl<Duration> resolutionSetting(ChronoUnit resolution) {
        return (SettingImpl<Duration>)
                settingBuilder("resolution." + resolution.name().toLowerCase(Locale.ROOT), DURATION)
                        .addConstraint(resolution(resolution))
                        .build();
    }

    @Test
    void testMutuallyExclusiveConstraint() {
        // given
        var setting1 = (SettingImpl<String>) settingBuilder("setting1", STRING).build();
        var name1 = "foo";
        var setting2 = (SettingImpl<String>) settingBuilder("setting2", STRING)
                .addConstraint(mutuallyExclusiveWith(setting1))
                .build();
        var name2 = "bar";

        // when
        var settings = new HashMap<Setting<?>, Object>();
        var simpleConfig = new Configuration() {
            @Override
            public <T> T get(Setting<T> setting) {
                return (T) settings.get(setting);
            }
        };
        settings.put(setting1, name1);
        settings.put(setting2, name2);

        // then
        assertThatCode(() -> setting1.validate(name1, simpleConfig)).doesNotThrowAnyException();
        assertThatThrownBy(() -> setting2.validate(name2, simpleConfig))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot be set in combination with");
    }

    private static void assertValidResolution(SettingImpl<Duration> resolutionSetting, long value, ChronoUnit unit) {
        assertThatNoException().isThrownBy(() -> resolutionSetting.validate(Duration.of(value, unit), EMPTY));
    }

    private static void assertInvalidResolution(
            SettingImpl<Duration> resolutionSetting,
            long value,
            ChronoUnit unit,
            ChronoUnit actualResolution,
            ChronoUnit expectedResolution) {
        assertThatThrownBy(() -> resolutionSetting.validate(Duration.of(value, unit), EMPTY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContainingAll(
                        "minimum allowed resolution is",
                        expectedResolution.toString(),
                        "but was",
                        actualResolution.toString());
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
