/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;

public class SocketAddressFormatTest
{
    @Test
    public void shouldCreateAdvertisedSocketAddressWithLeadingWhitespace() throws Exception
    {
        // given
        String addressString = whitespace( 1 ) + "localhost:9999";

        // when
        SocketAddress address = SocketAddressFormat.socketAddress( addressString, SocketAddress::new );

        // then
        assertEquals( "localhost", address.getHostname() );
        assertEquals( 9999, address.getPort() );
    }

    @Test
    public void shouldCreateAdvertisedSocketAddressWithTrailingWhitespace() throws Exception
    {
        // given
        String addressString = "localhost:9999" + whitespace( 1 );

        // when
        SocketAddress address = SocketAddressFormat.socketAddress( addressString, SocketAddress::new );

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
            SocketAddressFormat.socketAddress( addressString, SocketAddress::new );
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
            SocketAddressFormat.socketAddress( addressString, SocketAddress::new );
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
            SocketAddressFormat.socketAddress( addressString, SocketAddress::new );
            fail( "Should have thrown an exception" );
        }
        catch ( IllegalArgumentException e )
        {
            // expected
        }
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
