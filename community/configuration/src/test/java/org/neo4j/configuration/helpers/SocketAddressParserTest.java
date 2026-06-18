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
package org.neo4j.configuration.helpers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class SocketAddressParserTest {
    @Test
    void shouldCreateSocketAddressWithLeadingWhitespace() {
        // given
        String addressString = whitespace(1) + "localhost:9999";

        // when
        SocketAddress address = SocketAddressParser.socketAddress(addressString, SocketAddress::new);

        // then
        assertThat(address.getHostname()).isEqualTo("localhost");
        assertThat(address.getPort()).isEqualTo(9999);
    }

    @Test
    void shouldCreateSocketAddressWithTrailingWhitespace() {
        // given
        String addressString = "localhost:9999" + whitespace(2);

        // when
        SocketAddress address = SocketAddressParser.socketAddress(addressString, SocketAddress::new);

        // then
        assertThat(address.getHostname()).isEqualTo("localhost");
        assertThat(address.getPort()).isEqualTo(9999);
    }

    @Test
    void shouldFailToCreateSocketAddressWithMixedInWhitespace() {
        String addressString = "localhost" + whitespace(1) + ":9999";
        assertThatThrownBy(() -> SocketAddressParser.socketAddress(addressString, SocketAddress::new))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldFailToCreateSocketAddressWithNegativePort() {
        String addressString = "localhost:-10";
        assertThatThrownBy(() -> SocketAddressParser.socketAddress(addressString, SocketAddress::new))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldFailToCreateSocketAddressWithNonNumericPort() {
        String addressString = "localhost:bolt";
        assertThatThrownBy(() -> SocketAddressParser.socketAddress(addressString, SocketAddress::new))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldGetInvalidPortWhenMissingPort() {
        String addressString = "localhost";
        assertThat(SocketAddressParser.socketAddress(addressString, SocketAddress::new)
                        .getPort())
                .isEqualTo(-1);
    }

    @Test
    void shouldGetInvalidPortWhenMissingPortWithTrailingColon() {
        String addressString = "localhost:";
        assertThat(SocketAddressParser.socketAddress(addressString, SocketAddress::new)
                        .getPort())
                .isEqualTo(-1);
    }

    @Test
    void shouldGetInvalidPortWhenMissingPortIPv6Address() {
        SocketAddress socketAddress = SocketAddressParser.socketAddress("fe80:1:2:3:4::5", SocketAddress::new);
        assertThat(socketAddress.getPort()).isEqualTo(-1);
    }

    @Test
    void shouldGetPortWhenOnlyPortProvided() {
        String addressString = ":1";

        // behaviour should be the same regardless of whether or not a default port is included in the call to
        // socketAddress
        assertThat(SocketAddressParser.socketAddress(addressString, SocketAddress::new)
                        .getPort())
                .isEqualTo(1);
        assertThat(SocketAddressParser.socketAddress(addressString, 123, SocketAddress::new)
                        .getPort())
                .isEqualTo(1);
    }

    @Test
    void shouldGetDefaultPortWhenMissingPort() {
        String addressString = "localhost";
        assertThat(SocketAddressParser.socketAddress(addressString, 123, SocketAddress::new)
                        .getPort())
                .isEqualTo(123);
    }

    @Test
    void shouldGetDefaultPortWhenMissingPortIPv6Address() {
        String addressString = "fe80:1:2:3:4::5";
        assertThat(SocketAddressParser.socketAddress(addressString, 123, SocketAddress::new)
                        .getPort())
                .isEqualTo(123);
    }

    @Test
    void shouldGetDefaultPortWhenMissingPortWithTrailingColon() {
        String addressString = "localhost:";
        assertThat(SocketAddressParser.socketAddress(addressString, 123, SocketAddress::new)
                        .getPort())
                .isEqualTo(123);
    }

    @Test
    void shouldCreateSocketAddressWithPortZero() {
        SocketAddress socketAddress = SocketAddressParser.socketAddress("my.domain:0", SocketAddress::new);

        assertThat(socketAddress.getHostname()).isEqualTo("my.domain");
        assertThat(socketAddress.getPort()).isZero();
        assertThat(socketAddress).hasToString("my.domain:0");
    }

    @Test
    void shouldSupportDomainNameWithPort() {
        SocketAddress socketAddress = SocketAddressParser.socketAddress("my.domain:123", SocketAddress::new);

        assertThat(socketAddress.getHostname()).isEqualTo("my.domain");
        assertThat(socketAddress.getPort()).isEqualTo(123);
        assertThat(socketAddress).hasToString("my.domain:123");
    }

    @Test
    void shouldSupportWildcardWithPort() {
        SocketAddress socketAddress = SocketAddressParser.socketAddress("0.0.0.0:123", SocketAddress::new);

        assertThat(socketAddress.getHostname()).isEqualTo("0.0.0.0");
        assertThat(socketAddress.getPort()).isEqualTo(123);
        assertThat(socketAddress).hasToString("0.0.0.0:123");
        assertThat(socketAddress.isWildcard()).isTrue();
    }

    @Test
    void shouldSupportIPv6Wildcard() {
        SocketAddress socketAddress = SocketAddressParser.socketAddress("[::]:123", SocketAddress::new);

        assertThat(socketAddress.getHostname()).isEqualTo("::");
        assertThat(socketAddress.getPort()).isEqualTo(123);
        assertThat(socketAddress).hasToString("[::]:123");
        assertThat(socketAddress.isWildcard()).isTrue();
    }

    @Test
    void shouldSupportIPv6Localhost() {
        SocketAddress socketAddress = SocketAddressParser.socketAddress("[::1]:123", SocketAddress::new);

        assertThat(socketAddress.getHostname()).isEqualTo("::1");
        assertThat(socketAddress.getPort()).isEqualTo(123);
        assertThat(socketAddress).hasToString("[::1]:123");
        assertThat(socketAddress.isWildcard()).isFalse();
    }

    @Test
    void shouldSupportIPv6WithZoneId() {
        SocketAddress socketAddress = SocketAddressParser.socketAddress("[fe80::b279:2f%en0]:123", SocketAddress::new);

        assertThat(socketAddress.getHostname()).isEqualTo("fe80::b279:2f%en0");
        assertThat(socketAddress.getPort()).isEqualTo(123);
        assertThat(socketAddress).hasToString("[fe80::b279:2f%en0]:123");
        assertThat(socketAddress.isWildcard()).isFalse();
    }

    @Test
    void shouldSupportIPv6AddressWithBrackets() {
        SocketAddress socketAddress = SocketAddressParser.socketAddress("[fe80:1:2:3:4::5]:123", SocketAddress::new);

        assertThat(socketAddress.getHostname()).isEqualTo("fe80:1:2:3:4::5");
        assertThat(socketAddress.getPort()).isEqualTo(123);
        assertThat(socketAddress).hasToString("[fe80:1:2:3:4::5]:123");
        assertThat(socketAddress.isWildcard()).isFalse();
    }

    @Test
    void shouldSupportIPv6AddressWithoutBrackets() {
        SocketAddress socketAddress = SocketAddressParser.socketAddress("fe80:1:2:3:4::5:123", SocketAddress::new);

        assertThat(socketAddress.getHostname()).isEqualTo("fe80:1:2:3:4::5");
        assertThat(socketAddress.getPort()).isEqualTo(123);
        assertThat(socketAddress).hasToString("[fe80:1:2:3:4::5]:123");
        assertThat(socketAddress.isWildcard()).isFalse();
    }

    @Test
    void shouldSupportIPv6WildcardWithoutBrackets() {
        SocketAddress socketAddress = SocketAddressParser.socketAddress(":::123", SocketAddress::new);

        assertThat(socketAddress.getHostname()).isEqualTo("::");
        assertThat(socketAddress.getPort()).isEqualTo(123);
        assertThat(socketAddress.toString()).isEqualTo("[::]:123");
        assertThat(socketAddress.isWildcard()).isTrue();
    }

    @Test
    void shouldSupportIPv6SpecialAddresses() {
        SocketAddress localhost = SocketAddressParser.socketAddress("::1", SocketAddress::new);
        SocketAddress unspecified = SocketAddressParser.socketAddress("::", SocketAddress::new);

        assertThat(localhost.getHostname()).isEqualTo("::1");
        assertThat(localhost.getPort()).isLessThan(0);
        assertThat(unspecified.getHostname()).isEqualTo("::");
        assertThat(unspecified.getPort()).isLessThan(0);
    }

    @Test
    void shouldNotAllowURIs() {
        assertThatThrownBy(() -> SocketAddressParser.socketAddress("neo4j://18.117.195.94:7687", SocketAddress::new))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldAllowHostnameIPv6WithBracketsAndNoPort() {
        SocketAddress sa = SocketAddressParser.socketAddress("[fd01::9419:4c0e:be04:f0e3:4332]", SocketAddress::new);

        assertThat(sa.getHostname()).isEqualTo("fd01::9419:4c0e:be04:f0e3:4332");
        assertThat(sa.getPort()).isLessThan(0);
    }

    @Test
    void shouldAllowHostnameIPv4WithBracketsAndNoPort() {
        SocketAddress sa = SocketAddressParser.socketAddress("[127.0.0.1]", SocketAddress::new);

        assertThat(sa.getHostname()).isEqualTo("127.0.0.1");
        assertThat(sa.getPort()).isLessThan(0);
    }

    @Test
    void shouldAllowHostnameWithBracketsAndNoPort() {
        SocketAddress sa = SocketAddressParser.socketAddress("[localhost]", SocketAddress::new);

        assertThat(sa.getHostname()).isEqualTo("localhost");
        assertThat(sa.getPort()).isLessThan(0);
    }

    @Test
    void shouldNotParseHostnameIPv4WithPortInsideBrackets() {
        assertThatThrownBy(() -> SocketAddressParser.socketAddress("[127.0.0.1:80]", SocketAddress::new))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldNotParseHostnameWithPortInsideBrackets() {
        assertThatThrownBy(() -> SocketAddressParser.socketAddress("[localhost:80]", SocketAddress::new))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldParseHostnameOnlyIPv4() {
        SocketAddress sa = SocketAddressParser.socketAddressHostnameOnly("127.0.0.1", SocketAddress::new);

        assertThat(sa.getHostname()).isEqualTo("127.0.0.1");
        assertThat(sa.getPort()).isLessThan(0);
    }

    @Test
    void shouldParseHostnameOnlyIPv4WithBrackets() {
        SocketAddress sa = SocketAddressParser.socketAddressHostnameOnly("[127.0.0.1]", SocketAddress::new);

        assertThat(sa.getHostname()).isEqualTo("127.0.0.1");
        assertThat(sa.getPort()).isLessThan(0);
    }

    @Test
    void shouldNotParseHostnameOnlyIPv4WithPort() {
        assertThatThrownBy(() -> SocketAddressParser.socketAddressHostnameOnly("127.0.0.1:80", SocketAddress::new))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldParseHostnameOnly() {
        SocketAddress sa = SocketAddressParser.socketAddressHostnameOnly("localhost", SocketAddress::new);

        assertThat(sa.getHostname()).isEqualTo("localhost");
        assertThat(sa.getPort()).isLessThan(0);
    }

    @Test
    void shouldNotParseHostnameOnlyWithPort() {
        assertThatThrownBy(() -> SocketAddressParser.socketAddressHostnameOnly("localhost:80", SocketAddress::new))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldParseHostnameOnlyIPv6() {
        SocketAddress sa =
                SocketAddressParser.socketAddressHostnameOnly("fd01::9419:4c0e:be04:f0e3:433c", SocketAddress::new);

        assertThat(sa.getHostname()).isEqualTo("fd01::9419:4c0e:be04:f0e3:433c");
        assertThat(sa.getPort()).isLessThan(0);
    }

    @Test
    void shouldParseHostnameOnlyIPv6AmbiguousLast() {
        SocketAddress sa =
                SocketAddressParser.socketAddressHostnameOnly("fd01::9419:4c0e:be04:f0e3:4332", SocketAddress::new);

        assertThat(sa.getHostname()).isEqualTo("fd01::9419:4c0e:be04:f0e3:4332");
        assertThat(sa.getPort()).isLessThan(0);
    }

    @Test
    void shouldNotParseHostnameOnlyIPv6WithPort() {
        assertThatThrownBy(() -> SocketAddressParser.socketAddressHostnameOnly(
                        "[fd01::9419:4c0e:be04:f0e3]:4332", SocketAddress::new))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static String whitespace(int numberOfWhitespaces) {
        return " ".repeat(numberOfWhitespaces);
    }
}
