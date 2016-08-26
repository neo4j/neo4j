/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.coreedge.messaging.address;

import org.junit.Test;

import static java.lang.String.format;

import static junit.framework.TestCase.fail;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

public class AdvertisedSocketAddressTest
{
    @Test
    public void shouldCreateAdvertisedSocketAddressWithLeadingWhitespace() throws Exception
    {
        // given
        AdvertisedSocketAddress address = new AdvertisedSocketAddress( whitespace( 1 ) + "localhost:9999" );

        // when
        String string = address.toString();

        // then
        assertFalse( string.contains( whitespace( 1 ) ) );
    }

    @Test
    public void shouldCreateAdvertisedSocketAddressWithTrailingWhitespace() throws Exception
    {
        // given
        AdvertisedSocketAddress address = new AdvertisedSocketAddress( "localhost:9999" + whitespace( 1 ) );

        // when
        String string = address.toString();

        // then
        assertFalse( string.contains( whitespace( 1 ) ) );
    }

    @Test
    public void shouldFailToCreateSocketAddressWithMixedInWhitespace()
    {
        String address = "localhost:" + whitespace( 1 ) + "9999";
        try
        {
            new AdvertisedSocketAddress( address );
            fail( "Should have thrown an exception" );
        }
        catch ( IllegalArgumentException e )
        {
            assertThat( e.getMessage(),
                    containsString( format( "Cannot initialize AdvertisedSocketAddress for %s. Whitespace " +
                            "characters cause unresolvable ambiguity.", address ) ) );
        }
    }

    @Test
    public void shouldFailOnArgumentThatIsNotHostnameColonPort()
    {
        String address = "localhost:";
        try
        {
            new AdvertisedSocketAddress( address );
            fail( "Should have thrown an exception" );
        }
        catch ( IllegalArgumentException e )
        {
            assertThat( e.getMessage(),
                    containsString( format( "AdvertisedSocketAddress can only be created with hostname:port. " +
                            "%s is not acceptable", address ) ) );
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
