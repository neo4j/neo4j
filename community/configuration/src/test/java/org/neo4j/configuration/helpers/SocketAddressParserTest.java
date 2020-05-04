/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.configuration.helpers;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SocketAddressParserTest
{
    @Test
    void shouldCreateSocketAddressWithLeadingWhitespace()
    {
        // given
        String addressString = whitespace( 1 ) + "localhost:9999";

        // when
        SocketAddress address = SocketAddressParser.socketAddress( addressString, SocketAddress::new );

        // then
        assertEquals( "localhost", address.getHostname() );
        assertEquals( 9999, address.getPort() );
    }

    @Test
    void shouldCreateSocketAddressWithTrailingWhitespace()
    {
        // given
        String addressString = "localhost:9999" + whitespace( 2 );

        // when
        SocketAddress address = SocketAddressParser.socketAddress( addressString, SocketAddress::new );

        // then
        assertEquals( "localhost", address.getHostname() );
        assertEquals( 9999, address.getPort() );
    }

    @Test
    void shouldFailToCreateSocketAddressWithMixedInWhitespace()
    {
        String addressString = "localhost" + whitespace( 1 ) + ":9999";
        assertThrows( IllegalArgumentException.class, () -> SocketAddressParser.socketAddress( addressString, SocketAddress::new ) );
    }

    @Test
    void shouldGetInvalidPortWhenMissingPort()
    {
        String addressString = "localhost:";
        assertEquals( -1, SocketAddressParser.socketAddress( addressString, SocketAddress::new ).getPort() );
    }

    @Test
    void shouldSupportDomainNameWithPort()
    {
        SocketAddress socketAddress = SocketAddressParser.socketAddress( "my.domain:123", SocketAddress::new );

        assertEquals( "my.domain", socketAddress.getHostname() );
        assertEquals( 123, socketAddress.getPort() );
        assertEquals( "my.domain:123", socketAddress.toString() );
    }

    @Test
    void shouldSupportWildcardWithPort()
    {
        SocketAddress socketAddress = SocketAddressParser.socketAddress( "0.0.0.0:123", SocketAddress::new );

        assertEquals( "0.0.0.0", socketAddress.getHostname() );
        assertEquals( 123, socketAddress.getPort() );
        assertEquals( "0.0.0.0:123", socketAddress.toString() );
        assertTrue( socketAddress.isWildcard() );
    }

    @Test
    void shouldSupportPortOnly()
    {
        SocketAddress socketAddress = SocketAddressParser.deriveSocketAddress( "setting.name", ":123",
                "my.domain", 456, SocketAddress::new );

        assertEquals( "my.domain", socketAddress.getHostname() );
        assertEquals( 123, socketAddress.getPort() );
        assertEquals( "my.domain:123", socketAddress.toString() );
        assertFalse( socketAddress.isWildcard() );
    }

    @Test
    void shouldSupportDefaultValue()
    {
        SocketAddress socketAddress = SocketAddressParser.deriveSocketAddress( "setting.name", null,
                "my.domain", 456, SocketAddress::new );

        assertEquals( "my.domain", socketAddress.getHostname() );
        assertEquals( 456, socketAddress.getPort() );
        assertEquals( "my.domain:456", socketAddress.toString() );
        assertFalse( socketAddress.isWildcard() );
    }

    @Test
    void shouldSupportDefaultWildcard()
    {
        SocketAddress socketAddress = SocketAddressParser.deriveSocketAddress( "setting.name", null,
                "0.0.0.0", 456, SocketAddress::new );

        assertEquals( "0.0.0.0", socketAddress.getHostname() );
        assertEquals( 456, socketAddress.getPort() );
        assertEquals( "0.0.0.0:456", socketAddress.toString() );
        assertTrue( socketAddress.isWildcard() );
    }

    @Test
    void shouldSupportDefaultIPv6Wildcard()
    {
        SocketAddress socketAddress = SocketAddressParser.deriveSocketAddress( "setting.name", null,
                "::", 456, SocketAddress::new );

        assertEquals( "::", socketAddress.getHostname() );
        assertEquals( 456, socketAddress.getPort() );
        assertEquals( "[::]:456", socketAddress.toString() );
        assertTrue( socketAddress.isWildcard() );
    }

    @Test
    void shouldSupportDefaultIPv6Value()
    {
        SocketAddress socketAddress = SocketAddressParser.deriveSocketAddress( "setting.name", null,
                "fe80:1:2::4", 456, SocketAddress::new );

        assertEquals( "fe80:1:2::4", socketAddress.getHostname() );
        assertEquals( 456, socketAddress.getPort() );
        assertEquals( "[fe80:1:2::4]:456", socketAddress.toString() );
        assertFalse( socketAddress.isWildcard() );
    }

    @Test
    void shouldNotUseDefaultsWhenSettingValueSupplied()
    {
        SocketAddress socketAddress = SocketAddressParser.deriveSocketAddress( "setting.name", "[fe80:3:4::6]:456",
                "fe80:1:2::4", 123, SocketAddress::new );

        assertEquals( "fe80:3:4::6", socketAddress.getHostname() );
        assertEquals( 456, socketAddress.getPort() );
        assertEquals( "[fe80:3:4::6]:456", socketAddress.toString() );
        assertFalse( socketAddress.isWildcard() );
    }

    @Test
    void shouldSupportIPv6Wildcard()
    {
        SocketAddress socketAddress = SocketAddressParser.socketAddress( "[::]:123", SocketAddress::new );

        assertEquals( "::", socketAddress.getHostname() );
        assertEquals( 123, socketAddress.getPort() );
        assertEquals( "[::]:123", socketAddress.toString() );
        assertTrue( socketAddress.isWildcard() );
    }

    @Test
    void shouldSupportIPv6Localhost()
    {
        SocketAddress socketAddress = SocketAddressParser.socketAddress( "[::1]:123", SocketAddress::new );

        assertEquals( "::1", socketAddress.getHostname() );
        assertEquals( 123, socketAddress.getPort() );
        assertEquals( "[::1]:123", socketAddress.toString() );
        assertFalse( socketAddress.isWildcard() );
    }

    @Test
    void shouldSupportIPv6WithZoneId()
    {
        SocketAddress socketAddress = SocketAddressParser.socketAddress( "[fe80::b279:2f%en0]:123", SocketAddress::new );

        assertEquals( "fe80::b279:2f%en0", socketAddress.getHostname() );
        assertEquals( 123, socketAddress.getPort() );
        assertEquals( "[fe80::b279:2f%en0]:123", socketAddress.toString() );
        assertFalse( socketAddress.isWildcard() );
    }

    @Test
    void shouldSupportIPv6AddressWithBrackets()
    {
        SocketAddress socketAddress = SocketAddressParser.socketAddress( "[fe80:1:2:3:4::5]:123", SocketAddress::new );

        assertEquals( "fe80:1:2:3:4::5", socketAddress.getHostname() );
        assertEquals( 123, socketAddress.getPort() );
        assertEquals( "[fe80:1:2:3:4::5]:123", socketAddress.toString() );
        assertFalse( socketAddress.isWildcard() );
    }

    @Test
    void shouldSupportIPv6AddressWithoutBrackets()
    {
        SocketAddress socketAddress = SocketAddressParser.socketAddress( "fe80:1:2:3:4::5:123", SocketAddress::new );

        assertEquals( "fe80:1:2:3:4::5", socketAddress.getHostname() );
        assertEquals( 123, socketAddress.getPort() );
        assertEquals( "[fe80:1:2:3:4::5]:123", socketAddress.toString() );
        assertFalse( socketAddress.isWildcard() );
    }

    @Test
    void shouldSupportIPv6WildcardWithoutBrackets()
    {
        SocketAddress socketAddress = SocketAddressParser.socketAddress( ":::123", SocketAddress::new );

        assertEquals( "::", socketAddress.getHostname() );
        assertEquals( 123, socketAddress.getPort() );
        assertEquals( "[::]:123", socketAddress.toString() );
        assertTrue( socketAddress.isWildcard() );
    }

    @Test
    void shouldSupportIPv6SpecialAddresses()
    {
        SocketAddress localhost = SocketAddressParser.socketAddress( "::1", SocketAddress::new );
        SocketAddress unspecified = SocketAddressParser.socketAddress( "::", SocketAddress::new );

        assertEquals( "::1", localhost.getHostname() );
        assertTrue( localhost.getPort() < 0 );
        assertEquals( "::", unspecified.getHostname() );
        assertTrue( unspecified.getPort() < 0 );
    }

    private static String whitespace( int numberOfWhitespaces )
    {
        return " ".repeat( numberOfWhitespaces );
    }
}
