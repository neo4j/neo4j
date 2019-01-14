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

import org.junit.Test;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.neo4j.helpers.collection.MapUtil.map;

public class AuthTokenTest
{
    @Test
    public void shouldMakeBasicAuthToken()
    {
        Map<String, Object> token = AuthToken.newBasicAuthToken( "me", "my secret" );
        assertThat("Should have correct username", token.get(AuthToken.PRINCIPAL), equalTo("me"));
        assertThat("Should have correct password", token.get(AuthToken.CREDENTIALS), equalTo("my secret"));
        assertThat("Should have correct scheme", token.get(AuthToken.SCHEME_KEY), equalTo(AuthToken.BASIC_SCHEME));
        assertThat("Should have no realm", token.get(AuthToken.REALM_KEY), nullValue());
    }

    @Test
    public void shouldMakeBasicAuthTokenWithRealm()
    {
        Map<String, Object> token = AuthToken.newBasicAuthToken( "me", "my secret", "my realm" );
        assertThat("Should have correct username", token.get(AuthToken.PRINCIPAL), equalTo("me"));
        assertThat("Should have correct password", token.get(AuthToken.CREDENTIALS), equalTo("my secret"));
        assertThat("Should have correct scheme", token.get(AuthToken.SCHEME_KEY), equalTo(AuthToken.BASIC_SCHEME));
        assertThat("Should have correct realm", token.get(AuthToken.REALM_KEY), equalTo( "my realm" ));
    }

    @Test
    public void shouldMakeCustomAuthTokenAndBasicScheme()
    {
        Map<String, Object> token = AuthToken.newCustomAuthToken( "me", "my secret", "my realm", "basic" );
        assertThat("Should have correct username", token.get(AuthToken.PRINCIPAL), equalTo("me"));
        assertThat("Should have correct password", token.get(AuthToken.CREDENTIALS), equalTo("my secret"));
        assertThat("Should have correct scheme", token.get(AuthToken.SCHEME_KEY), equalTo(AuthToken.BASIC_SCHEME));
        assertThat("Should have correctno realm", token.get(AuthToken.REALM_KEY), equalTo( "my realm" ));
    }

    @Test
    public void shouldMakeCustomAuthTokenAndCustomcScheme()
    {
        Map<String, Object> token = AuthToken.newCustomAuthToken( "me", "my secret", "my realm", "my scheme" );
        assertThat("Should have correct username", token.get(AuthToken.PRINCIPAL), equalTo("me"));
        assertThat("Should have correct password", token.get(AuthToken.CREDENTIALS), equalTo("my secret"));
        assertThat("Should have correct scheme", token.get(AuthToken.SCHEME_KEY), equalTo("my scheme"));
        assertThat("Should have correct realm", token.get(AuthToken.REALM_KEY), equalTo( "my realm" ));
    }

    @Test
    public void shouldMakeCustomAuthTokenAndCustomcSchemeWithParameters()
    {
        Map<String, Object> token = AuthToken.newCustomAuthToken( "me", "my secret", "my realm", "my scheme", map("a", "A", "b", "B") );
        assertThat("Should have correct username", token.get(AuthToken.PRINCIPAL), equalTo("me"));
        assertThat("Should have correct password", token.get(AuthToken.CREDENTIALS), equalTo("my secret"));
        assertThat("Should have correct scheme", token.get(AuthToken.SCHEME_KEY), equalTo("my scheme"));
        assertThat("Should have correct realm", token.get(AuthToken.REALM_KEY), equalTo( "my realm" ));
        assertThat("Should have correct parameters", token.get(AuthToken.PARAMETERS), equalTo( map("a", "A", "b", "B") ));
    }
}
