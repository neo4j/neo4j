/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.helpers;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SocketAddressParserTest
{
    @Test
    public void shouldCreateAdvertisedSocketAddressWithLeadingWhitespace()
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
    public void shouldCreateAdvertisedSocketAddressWithTrailingWhitespace()
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
    public void shouldFailToCreateSocketAddressWithMixedInWhitespace()
    {
        String addressString = "localhost" + whitespace( 1 ) + ":9999";
        try
        {
            SocketAddressParser.socketAddress( addressString, SocketAddress::new );
            fail( "Should have thrown an exception" );
        }
        catch ( IllegalArgumentException e )
        {
            // expected
        }
    }

    @Test
    public void shouldFailToCreateSocketWithTrailingNonNumbers()
    {
        String addressString = "localhost:9999abc";
        try
        {
            SocketAddressParser.socketAddress( addressString, SocketAddress::new );
            fail( "Should have thrown an exception" );
        }
        catch ( IllegalArgumentException e )
        {
            // expected
        }
    }

    @Test
    public void shouldFailOnMissingPort()
    {
        String addressString = "localhost:";
        try
        {
            SocketAddressParser.socketAddress( addressString, SocketAddress::new );
            fail( "Should have thrown an exception" );
        }
        catch ( IllegalArgumentException e )
        {
            // expected
        }
    }

    @Test
    public void shouldSupportDomainNameWithPort()
    {
        SocketAddress socketAddress = SocketAddressParser.socketAddress( "my.domain:123", SocketAddress::new );

        assertEquals( "my.domain", socketAddress.getHostname() );
        assertEquals( 123, socketAddress.getPort() );
        assertEquals( "my.domain:123", socketAddress.toString() );
    }

    @Test
    public void shouldSupportWildcardWithPort()
    {
        SocketAddress socketAddress = SocketAddressParser.socketAddress( "0.0.0.0:123", SocketAddress::new );

        assertEquals( "0.0.0.0", socketAddress.getHostname() );
        assertEquals( 123, socketAddress.getPort() );
        assertEquals( "0.0.0.0:123", socketAddress.toString() );
        assertTrue( socketAddress.isWildcard() );
    }

    @Test
    public void shouldSupportPortOnly()
    {
        SocketAddress socketAddress = SocketAddressParser.deriveSocketAddress( "setting.name", ":123",
                "my.domain", 456, SocketAddress::new );

        assertEquals( "my.domain", socketAddress.getHostname() );
        assertEquals( 123, socketAddress.getPort() );
        assertEquals( "my.domain:123", socketAddress.toString() );
        assertFalse( socketAddress.isWildcard() );
    }

    @Test
    public void shouldSupportDefaultValue()
    {
        SocketAddress socketAddress = SocketAddressParser.deriveSocketAddress( "setting.name", null,
                "my.domain", 456, SocketAddress::new );

        assertEquals( "my.domain", socketAddress.getHostname() );
        assertEquals( 456, socketAddress.getPort() );
        assertEquals( "my.domain:456", socketAddress.toString() );
        assertFalse( socketAddress.isWildcard() );
    }

    @Test
    public void shouldSupportDefaultWildcard()
    {
        SocketAddress socketAddress = SocketAddressParser.deriveSocketAddress( "setting.name", null,
                "0.0.0.0", 456, SocketAddress::new );

        assertEquals( "0.0.0.0", socketAddress.getHostname() );
        assertEquals( 456, socketAddress.getPort() );
        assertEquals( "0.0.0.0:456", socketAddress.toString() );
        assertTrue( socketAddress.isWildcard() );
    }

    @Test
    public void shouldSupportDefaultIPv6Wildcard()
    {
        SocketAddress socketAddress = SocketAddressParser.deriveSocketAddress( "setting.name", null,
                "::", 456, SocketAddress::new );

        assertEquals( "::", socketAddress.getHostname() );
        assertEquals( 456, socketAddress.getPort() );
        assertEquals( "[::]:456", socketAddress.toString() );
        assertTrue( socketAddress.isWildcard() );
    }

    @Test
    public void shouldSupportDefaultIPv6Value()
    {
        SocketAddress socketAddress = SocketAddressParser.deriveSocketAddress( "setting.name", null,
                "fe80:1:2::4", 456, SocketAddress::new );

        assertEquals( "fe80:1:2::4", socketAddress.getHostname() );
        assertEquals( 456, socketAddress.getPort() );
        assertEquals( "[fe80:1:2::4]:456", socketAddress.toString() );
        assertFalse( socketAddress.isWildcard() );
    }

    @Test
    public void shouldNotUseDefaultsWhenSettingValueSupplied()
    {
        SocketAddress socketAddress = SocketAddressParser.deriveSocketAddress( "setting.name", "[fe80:3:4::6]:456",
                "fe80:1:2::4", 123, SocketAddress::new );

        assertEquals( "fe80:3:4::6", socketAddress.getHostname() );
        assertEquals( 456, socketAddress.getPort() );
        assertEquals( "[fe80:3:4::6]:456", socketAddress.toString() );
        assertFalse( socketAddress.isWildcard() );
    }

    @Test
    public void shouldSupportIPv6Wildcard()
    {
        SocketAddress socketAddress = SocketAddressParser.socketAddress( "[::]:123", SocketAddress::new );

        assertEquals( "::", socketAddress.getHostname() );
        assertEquals( 123, socketAddress.getPort() );
        assertEquals( "[::]:123", socketAddress.toString() );
        assertTrue( socketAddress.isWildcard() );
    }

    @Test
    public void shouldSupportIPv6Localhost()
    {
        SocketAddress socketAddress = SocketAddressParser.socketAddress( "[::1]:123", SocketAddress::new );

        assertEquals( "::1", socketAddress.getHostname() );
        assertEquals( 123, socketAddress.getPort() );
        assertEquals( "[::1]:123", socketAddress.toString() );
        assertFalse( socketAddress.isWildcard() );
    }

    @Test
    public void shouldSupportIPv6WithZoneId()
    {
        SocketAddress socketAddress = SocketAddressParser.socketAddress( "[fe80::b279:2f%en0]:123", SocketAddress::new );

        assertEquals( "fe80::b279:2f%en0", socketAddress.getHostname() );
        assertEquals( 123, socketAddress.getPort() );
        assertEquals( "[fe80::b279:2f%en0]:123", socketAddress.toString() );
        assertFalse( socketAddress.isWildcard() );
    }

    @Test
    public void shouldSupportIPv6AddressWithBrackets()
    {
        SocketAddress socketAddress = SocketAddressParser.socketAddress( "[fe80:1:2:3:4::5]:123", SocketAddress::new );

        assertEquals( "fe80:1:2:3:4::5", socketAddress.getHostname() );
        assertEquals( 123, socketAddress.getPort() );
        assertEquals( "[fe80:1:2:3:4::5]:123", socketAddress.toString() );
        assertFalse( socketAddress.isWildcard() );
    }

    @Test
    public void shouldSupportIPv6AddressWithoutBrackets()
    {
        SocketAddress socketAddress = SocketAddressParser.socketAddress( "fe80:1:2:3:4::5:123", SocketAddress::new );

        assertEquals( "fe80:1:2:3:4::5", socketAddress.getHostname() );
        assertEquals( 123, socketAddress.getPort() );
        assertEquals( "[fe80:1:2:3:4::5]:123", socketAddress.toString() );
        assertFalse( socketAddress.isWildcard() );
    }

    @Test
    public void shouldSupportIPv6WildcardWithoutBrackets()
    {
        SocketAddress socketAddress = SocketAddressParser.socketAddress( ":::123", SocketAddress::new );

        assertEquals( "::", socketAddress.getHostname() );
        assertEquals( 123, socketAddress.getPort() );
        assertEquals( "[::]:123", socketAddress.toString() );
        assertTrue( socketAddress.isWildcard() );
    }

    private String whitespace( int numberOfWhitespaces )
    {
        StringBuilder sb = new StringBuilder();

        for ( int i = 0; i < numberOfWhitespaces; i++ )
        {
            sb.append( " " );
        }

        return sb.toString();
    }
}
