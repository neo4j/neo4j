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
package org.neo4j.kernel.api.security;

import org.junit.jupiter.api.Test;

import java.util.Map;

import org.neo4j.string.UTF8;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.internal.helpers.collection.MapUtil.map;

class AuthTokenTest
{
    @Test
    void shouldMakeBasicAuthToken()
    {
        Map<String, Object> token = AuthToken.newBasicAuthToken( "me", "my secret" );
        assertThat( token.get( AuthToken.PRINCIPAL ) ).as( "Should have correct username" ).isEqualTo( "me" );
        assertThat( token.get( AuthToken.CREDENTIALS ) ).as( "Should have correct password" ).isEqualTo( UTF8.encode( "my secret" ) );
        assertThat( token.get( AuthToken.SCHEME_KEY ) ).as( "Should have correct scheme" ).isEqualTo( AuthToken.BASIC_SCHEME );
        assertThat( token.get( AuthToken.REALM_KEY ) ).as( "Should have no realm" ).isNull();
    }

    @Test
    void shouldMakeBasicAuthTokenWithRealm()
    {
        Map<String, Object> token = AuthToken.newBasicAuthToken( "me", "my secret", "my realm" );
        assertThat( token.get( AuthToken.PRINCIPAL ) ).as( "Should have correct username" ).isEqualTo( "me" );
        assertThat( token.get( AuthToken.CREDENTIALS ) ).as( "Should have correct password" ).isEqualTo( UTF8.encode( "my secret" ) );
        assertThat( token.get( AuthToken.SCHEME_KEY ) ).as( "Should have correct scheme" ).isEqualTo( AuthToken.BASIC_SCHEME );
        assertThat( token.get( AuthToken.REALM_KEY ) ).as( "Should have correct realm" ).isEqualTo( "my realm" );
    }

    @Test
    void shouldMakeCustomAuthTokenAndBasicScheme()
    {
        Map<String, Object> token = AuthToken.newCustomAuthToken( "me", "my secret", "my realm", "basic" );
        assertThat( token.get( AuthToken.PRINCIPAL ) ).as( "Should have correct username" ).isEqualTo( "me" );
        assertThat( token.get( AuthToken.CREDENTIALS ) ).as( "Should have correct password" ).isEqualTo( UTF8.encode( "my secret" ) );
        assertThat( token.get( AuthToken.SCHEME_KEY ) ).as( "Should have correct scheme" ).isEqualTo( AuthToken.BASIC_SCHEME );
        assertThat( token.get( AuthToken.REALM_KEY ) ).as( "Should have correctno realm" ).isEqualTo( "my realm" );
    }

    @Test
    void shouldMakeCustomAuthTokenAndCustomcScheme()
    {
        Map<String, Object> token = AuthToken.newCustomAuthToken( "me", "my secret", "my realm", "my scheme" );
        assertThat( token.get( AuthToken.PRINCIPAL ) ).as( "Should have correct username" ).isEqualTo( "me" );
        assertThat( token.get( AuthToken.CREDENTIALS ) ).as( "Should have correct password" ).isEqualTo( UTF8.encode( "my secret" ) );
        assertThat( token.get( AuthToken.SCHEME_KEY ) ).as( "Should have correct scheme" ).isEqualTo( "my scheme" );
        assertThat( token.get( AuthToken.REALM_KEY ) ).as( "Should have correct realm" ).isEqualTo( "my realm" );
    }

    @Test
    void shouldMakeCustomAuthTokenAndCustomcSchemeWithParameters()
    {
        Map<String, Object> token = AuthToken.newCustomAuthToken( "me", "my secret", "my realm", "my scheme", map("a", "A", "b", "B") );
        assertThat( token.get( AuthToken.PRINCIPAL ) ).as( "Should have correct username" ).isEqualTo( "me" );
        assertThat( token.get( AuthToken.CREDENTIALS ) ).as( "Should have correct password" ).isEqualTo( UTF8.encode( "my secret" ) );
        assertThat( token.get( AuthToken.SCHEME_KEY ) ).as( "Should have correct scheme" ).isEqualTo( "my scheme" );
        assertThat( token.get( AuthToken.REALM_KEY ) ).as( "Should have correct realm" ).isEqualTo( "my realm" );
        assertThat( token.get( AuthToken.PARAMETERS ) ).as( "Should have correct parameters" ).isEqualTo( map( "a", "A", "b", "B" ) );
    }
}
