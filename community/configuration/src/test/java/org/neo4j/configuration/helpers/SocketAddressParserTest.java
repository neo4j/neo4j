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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class SocketAddressParserTest {
    @Test
    void shouldCreateSocketAddressWithLeadingWhitespace() {
        // given
        String addressString = whitespace(1) + "localhost:9999";

        // when
        SocketAddress address = SocketAddressParser.socketAddress(addressString, SocketAddress::new);

        // then
        assertEquals("localhost", address.getHostname());
        assertEquals(9999, address.getPort());
    }

    @Test
    void shouldCreateSocketAddressWithTrailingWhitespace() {
        // given
        String addressString = "localhost:9999" + whitespace(2);

        // when
        SocketAddress address = SocketAddressParser.socketAddress(addressString, SocketAddress::new);

        // then
        assertEquals("localhost", address.getHostname());
        assertEquals(9999, address.getPort());
    }

    @Test
    void shouldFailToCreateSocketAddressWithMixedInWhitespace() {
        String addressString = "localhost" + whitespace(1) + ":9999";
        assertThrows(
                IllegalArgumentException.class,
                () -> SocketAddressParser.socketAddress(addressString, SocketAddress::new));
    }

    @Test
    void shouldFailToCreateSocketAddressWithNegativePort() {
        String addressString = "localhost:-10";
        assertThrows(
                IllegalArgumentException.class,
                () -> SocketAddressParser.socketAddress(addressString, SocketAddress::new));
    }

    @Test
    void shouldFailToCreateSocketAddressWithNonNumericPort() {
        String addressString = "localhost:bolt";
        assertThrows(
                IllegalArgumentException.class,
                () -> SocketAddressParser.socketAddress(addressString, SocketAddress::new));
    }

    @Test
    void shouldGetInvalidPortWhenMissingPort() {
        String addressString = "localhost";
        assertEquals(
                -1,
                SocketAddressParser.socketAddress(addressString, SocketAddress::new)
                        .getPort());
    }

    @Test
    void shouldGetInvalidPortWhenMissingPortWithTrailingColon() {
        String addressString = "localhost:";
        assertEquals(
                -1,
                SocketAddressParser.socketAddress(addressString, SocketAddress::new)
                        .getPort());
    }

    @Test
    void shouldGetInvalidPortWhenMissingPortIPv6Address() {
        SocketAddress socketAddress = SocketAddressParser.socketAddress("fe80:1:2:3:4::5", SocketAddress::new);
        assertEquals(-1, socketAddress.getPort());
    }

    @Test
    void shouldGetPortWhenOnlyPortProvided() {
        String addressString = ":1";

        // behaviour should be the same regardless of whether or not a default port is included in the call to
        // socketAddress
        assertEquals(
                1,
                SocketAddressParser.socketAddress(addressString, SocketAddress::new)
                        .getPort());
        assertEquals(
                1,
                SocketAddressParser.socketAddress(addressString, 123, SocketAddress::new)
                        .getPort());
    }

    @Test
    void shouldGetDefaultPortWhenMissingPort() {
        String addressString = "localhost";
        assertEquals(
                123,
                SocketAddressParser.socketAddress(addressString, 123, SocketAddress::new)
                        .getPort());
    }

    @Test
    void shouldGetDefaultPortWhenMissingPortIPv6Address() {
        String addressString = "fe80:1:2:3:4::5";
        assertEquals(
                123,
                SocketAddressParser.socketAddress(addressString, 123, SocketAddress::new)
                        .getPort());
    }

    @Test
    void shouldGetDefaultPortWhenMissingPortWithTrailingColon() {
        String addressString = "localhost:";
        assertEquals(
                123,
                SocketAddressParser.socketAddress(addressString, 123, SocketAddress::new)
                        .getPort());
    }

    @Test
    void shouldCreateSocketAddressWithPortZero() {
        SocketAddress socketAddress = SocketAddressParser.socketAddress("my.domain:0", SocketAddress::new);

        assertEquals("my.domain", socketAddress.getHostname());
        assertEquals(0, socketAddress.getPort());
        assertEquals("my.domain:0", socketAddress.toString());
    }

    @Test
    void shouldSupportDomainNameWithPort() {
        SocketAddress socketAddress = SocketAddressParser.socketAddress("my.domain:123", SocketAddress::new);

        assertEquals("my.domain", socketAddress.getHostname());
        assertEquals(123, socketAddress.getPort());
        assertEquals("my.domain:123", socketAddress.toString());
    }

    @Test
    void shouldSupportWildcardWithPort() {
        SocketAddress socketAddress = SocketAddressParser.socketAddress("0.0.0.0:123", SocketAddress::new);

        assertEquals("0.0.0.0", socketAddress.getHostname());
        assertEquals(123, socketAddress.getPort());
        assertEquals("0.0.0.0:123", socketAddress.toString());
        assertTrue(socketAddress.isWildcard());
    }

    @Test
    void shouldSupportIPv6Wildcard() {
        SocketAddress socketAddress = SocketAddressParser.socketAddress("[::]:123", SocketAddress::new);

        assertEquals("::", socketAddress.getHostname());
        assertEquals(123, socketAddress.getPort());
        assertEquals("[::]:123", socketAddress.toString());
        assertTrue(socketAddress.isWildcard());
    }

    @Test
    void shouldSupportIPv6Localhost() {
        SocketAddress socketAddress = SocketAddressParser.socketAddress("[::1]:123", SocketAddress::new);

        assertEquals("::1", socketAddress.getHostname());
        assertEquals(123, socketAddress.getPort());
        assertEquals("[::1]:123", socketAddress.toString());
        assertFalse(socketAddress.isWildcard());
    }

    @Test
    void shouldSupportIPv6WithZoneId() {
        SocketAddress socketAddress = SocketAddressParser.socketAddress("[fe80::b279:2f%en0]:123", SocketAddress::new);

        assertEquals("fe80::b279:2f%en0", socketAddress.getHostname());
        assertEquals(123, socketAddress.getPort());
        assertEquals("[fe80::b279:2f%en0]:123", socketAddress.toString());
        assertFalse(socketAddress.isWildcard());
    }

    @Test
    void shouldSupportIPv6AddressWithBrackets() {
        SocketAddress socketAddress = SocketAddressParser.socketAddress("[fe80:1:2:3:4::5]:123", SocketAddress::new);

        assertEquals("fe80:1:2:3:4::5", socketAddress.getHostname());
        assertEquals(123, socketAddress.getPort());
        assertEquals("[fe80:1:2:3:4::5]:123", socketAddress.toString());
        assertFalse(socketAddress.isWildcard());
    }

    @Test
    void shouldSupportIPv6AddressWithoutBrackets() {
        SocketAddress socketAddress = SocketAddressParser.socketAddress("fe80:1:2:3:4::5:123", SocketAddress::new);

        assertEquals("fe80:1:2:3:4::5", socketAddress.getHostname());
        assertEquals(123, socketAddress.getPort());
        assertEquals("[fe80:1:2:3:4::5]:123", socketAddress.toString());
        assertFalse(socketAddress.isWildcard());
    }

    @Test
    void shouldSupportIPv6WildcardWithoutBrackets() {
        SocketAddress socketAddress = SocketAddressParser.socketAddress(":::123", SocketAddress::new);

        assertEquals("::", socketAddress.getHostname());
        assertEquals(123, socketAddress.getPort());
        assertEquals("[::]:123", socketAddress.toString());
        assertTrue(socketAddress.isWildcard());
    }

    @Test
    void shouldSupportIPv6SpecialAddresses() {
        SocketAddress localhost = SocketAddressParser.socketAddress("::1", SocketAddress::new);
        SocketAddress unspecified = SocketAddressParser.socketAddress("::", SocketAddress::new);

        assertEquals("::1", localhost.getHostname());
        assertTrue(localhost.getPort() < 0);
        assertEquals("::", unspecified.getHostname());
        assertTrue(unspecified.getPort() < 0);
    }

    @Test
    void shouldNotAllowURIs() {
        assertThrows(
                IllegalArgumentException.class,
                () -> SocketAddressParser.socketAddress("neo4j://18.117.195.94:7687", SocketAddress::new));
    }

    @Test
    void shouldAllowHostnameIPv6WithBracketsAndNoPort() {
        SocketAddress sa = SocketAddressParser.socketAddress("[fd01::9419:4c0e:be04:f0e3:4332]", SocketAddress::new);

        assertEquals("fd01::9419:4c0e:be04:f0e3:4332", sa.getHostname());
        assertTrue(sa.getPort() < 0);
    }

    @Test
    void shouldAllowHostnameIPv4WithBracketsAndNoPort() {
        SocketAddress sa = SocketAddressParser.socketAddress("[127.0.0.1]", SocketAddress::new);

        assertEquals("127.0.0.1", sa.getHostname());
        assertTrue(sa.getPort() < 0);
    }

    @Test
    void shouldAllowHostnameWithBracketsAndNoPort() {
        SocketAddress sa = SocketAddressParser.socketAddress("[localhost]", SocketAddress::new);

        assertEquals("localhost", sa.getHostname());
        assertTrue(sa.getPort() < 0);
    }

    @Test
    void shouldNotParseHostnameIPv4WithPortInsideBrackets() {
        assertThrows(
                IllegalArgumentException.class,
                () -> SocketAddressParser.socketAddress("[127.0.0.1:80]", SocketAddress::new));
    }

    @Test
    void shouldNotParseHostnameWithPortInsideBrackets() {
        assertThrows(
                IllegalArgumentException.class,
                () -> SocketAddressParser.socketAddress("[localhost:80]", SocketAddress::new));
    }

    @Test
    void shouldParseHostnameOnlyIPv4() {
        SocketAddress sa = SocketAddressParser.socketAddressHostnameOnly("127.0.0.1", SocketAddress::new);

        assertEquals("127.0.0.1", sa.getHostname());
        assertTrue(sa.getPort() < 0);
    }

    @Test
    void shouldParseHostnameOnlyIPv4WithBrackets() {
        SocketAddress sa = SocketAddressParser.socketAddressHostnameOnly("[127.0.0.1]", SocketAddress::new);

        assertEquals("127.0.0.1", sa.getHostname());
        assertTrue(sa.getPort() < 0);
    }

    @Test
    void shouldNotParseHostnameOnlyIPv4WithPort() {
        assertThrows(
                IllegalArgumentException.class,
                () -> SocketAddressParser.socketAddressHostnameOnly("127.0.0.1:80", SocketAddress::new));
    }

    @Test
    void shouldParseHostnameOnly() {
        SocketAddress sa = SocketAddressParser.socketAddressHostnameOnly("localhost", SocketAddress::new);

        assertEquals("localhost", sa.getHostname());
        assertTrue(sa.getPort() < 0);
    }

    @Test
    void shouldNotParseHostnameOnlyWithPort() {
        assertThrows(
                IllegalArgumentException.class,
                () -> SocketAddressParser.socketAddressHostnameOnly("localhost:80", SocketAddress::new));
    }

    @Test
    void shouldParseHostnameOnlyIPv6() {
        SocketAddress sa =
                SocketAddressParser.socketAddressHostnameOnly("fd01::9419:4c0e:be04:f0e3:433c", SocketAddress::new);

        assertEquals("fd01::9419:4c0e:be04:f0e3:433c", sa.getHostname());
        assertTrue(sa.getPort() < 0);
    }

    @Test
    void shouldParseHostnameOnlyIPv6AmbiguousLast() {
        SocketAddress sa =
                SocketAddressParser.socketAddressHostnameOnly("fd01::9419:4c0e:be04:f0e3:4332", SocketAddress::new);

        assertEquals("fd01::9419:4c0e:be04:f0e3:4332", sa.getHostname());
        assertTrue(sa.getPort() < 0);
    }

    @Test
    void shouldNotParseHostnameOnlyIPv6WithPort() {
        assertThrows(
                IllegalArgumentException.class,
                () -> SocketAddressParser.socketAddressHostnameOnly(
                        "[fd01::9419:4c0e:be04:f0e3]:4332", SocketAddress::new));
    }

    private static String whitespace(int numberOfWhitespaces) {
        return " ".repeat(numberOfWhitespaces);
    }
}
