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
package org.neo4j.server.security.auth;

import org.junit.jupiter.api.Test;

import java.util.Map;

import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.server.security.auth.SecurityTestUtils.password;
import static org.neo4j.test.AuthTokenUtil.authTokenMatcher;

class ShiroAuthTokenTest
{
    private static final String USERNAME = "myuser";
    private static final String PASSWORD = "mypw123";

    @Test
    void shouldSupportBasicAuthToken() throws Exception
    {
        ShiroAuthToken token = new ShiroAuthToken( AuthToken.newBasicAuthToken( USERNAME, PASSWORD ) );
        testBasicAuthToken( token, USERNAME, PASSWORD, AuthToken.BASIC_SCHEME );
        assertThat( "Token map should have only expected values", token.getAuthTokenMap(),
                authTokenMatcher( map( AuthToken.PRINCIPAL, USERNAME, AuthToken.CREDENTIALS, PASSWORD, AuthToken.SCHEME_KEY,
                        AuthToken.BASIC_SCHEME ) ) );
        testTokenSupportsRealm( token, true, "unknown", "native", "ldap" );
    }

    @Test
    void shouldSupportBasicAuthTokenWithEmptyRealm() throws Exception
    {
        ShiroAuthToken token = new ShiroAuthToken( AuthToken.newBasicAuthToken( USERNAME, PASSWORD, "" ) );
        testBasicAuthToken( token, USERNAME, PASSWORD, AuthToken.BASIC_SCHEME );
        assertThat( "Token map should have only expected values", token.getAuthTokenMap(),
                authTokenMatcher( map( AuthToken.PRINCIPAL, USERNAME, AuthToken.CREDENTIALS, PASSWORD, AuthToken.SCHEME_KEY,
                        AuthToken.BASIC_SCHEME, AuthToken.REALM_KEY, "" ) ) );
        testTokenSupportsRealm( token, true, "unknown", "native", "ldap" );
    }

    @Test
    void shouldSupportBasicAuthTokenWithNullRealm() throws Exception
    {
        ShiroAuthToken token = new ShiroAuthToken( AuthToken.newBasicAuthToken( USERNAME, PASSWORD, null ) );
        testBasicAuthToken( token, USERNAME, PASSWORD, AuthToken.BASIC_SCHEME );
        assertThat( "Token map should have only expected values", token.getAuthTokenMap(),
                authTokenMatcher( map( AuthToken.PRINCIPAL, USERNAME, AuthToken.CREDENTIALS, PASSWORD, AuthToken.SCHEME_KEY,
                        AuthToken.BASIC_SCHEME, AuthToken.REALM_KEY, null ) ) );
        testTokenSupportsRealm( token, true, "unknown", "native", "ldap" );
    }

    @Test
    void shouldSupportBasicAuthTokenWithWildcardRealm() throws Exception
    {
        ShiroAuthToken token = new ShiroAuthToken( AuthToken.newBasicAuthToken( USERNAME, PASSWORD, "*" ) );
        testBasicAuthToken( token, USERNAME, PASSWORD, AuthToken.BASIC_SCHEME );
        assertThat( "Token map should have only expected values", token.getAuthTokenMap(),
                authTokenMatcher( map( AuthToken.PRINCIPAL, USERNAME, AuthToken.CREDENTIALS, PASSWORD, AuthToken.SCHEME_KEY,
                        AuthToken.BASIC_SCHEME, AuthToken.REALM_KEY, "*" ) ) );
        testTokenSupportsRealm( token, true, "unknown", "native", "ldap" );
    }

    @Test
    void shouldSupportBasicAuthTokenWithSpecificRealm() throws Exception
    {
        String realm = "ldap";
        ShiroAuthToken token = new ShiroAuthToken( AuthToken.newBasicAuthToken( USERNAME, PASSWORD, realm ) );
        testBasicAuthToken( token, USERNAME, PASSWORD, AuthToken.BASIC_SCHEME );
        assertThat( "Token map should have only expected values", token.getAuthTokenMap(),
                authTokenMatcher( map( AuthToken.PRINCIPAL, USERNAME, AuthToken.CREDENTIALS, PASSWORD, AuthToken.SCHEME_KEY,
                        AuthToken.BASIC_SCHEME, AuthToken.REALM_KEY, "ldap" ) ) );
        testTokenSupportsRealm( token, true, realm );
        testTokenSupportsRealm( token, false, "unknown", "native" );
    }

    @Test
    void shouldSupportCustomAuthTokenWithSpecificRealm() throws Exception
    {
        String realm = "ldap";
        ShiroAuthToken token =
                new ShiroAuthToken( AuthToken.newCustomAuthToken( USERNAME, PASSWORD, realm, AuthToken.BASIC_SCHEME ) );
        testBasicAuthToken( token, USERNAME, PASSWORD, AuthToken.BASIC_SCHEME );
        assertThat( "Token map should have only expected values", token.getAuthTokenMap(),
                authTokenMatcher( map( AuthToken.PRINCIPAL, USERNAME, AuthToken.CREDENTIALS, PASSWORD, AuthToken.SCHEME_KEY,
                        AuthToken.BASIC_SCHEME, AuthToken.REALM_KEY, "ldap" ) ) );
        testTokenSupportsRealm( token, true, realm );
        testTokenSupportsRealm( token, false, "unknown", "native" );
    }

    @Test
    void shouldSupportCustomAuthTokenWithSpecificRealmAndParameters() throws Exception
    {
        String realm = "ldap";
        Map<String,Object> params = map( "a", "A", "b", "B" );
        ShiroAuthToken token =
                new ShiroAuthToken(
                        AuthToken.newCustomAuthToken( USERNAME, PASSWORD, realm, AuthToken.BASIC_SCHEME, params ) );
        testBasicAuthToken( token, USERNAME, PASSWORD, AuthToken.BASIC_SCHEME );
        assertThat( "Token map should have only expected values", token.getAuthTokenMap(),
                authTokenMatcher( map( AuthToken.PRINCIPAL, USERNAME, AuthToken.CREDENTIALS, PASSWORD, AuthToken.SCHEME_KEY,
                        AuthToken.BASIC_SCHEME, AuthToken.REALM_KEY, "ldap",
                        "parameters", params ) ) );
        testTokenSupportsRealm( token, true, realm );
        testTokenSupportsRealm( token, false, "unknown", "native" );
    }

    @Test
    void shouldHaveStringRepresentationWithNullRealm() throws Exception
    {
        ShiroAuthToken token = new ShiroAuthToken( AuthToken.newBasicAuthToken( USERNAME, PASSWORD, null ) );
        testBasicAuthToken( token, USERNAME, PASSWORD, AuthToken.BASIC_SCHEME );

        String stringRepresentation = token.toString();
        assertThat( stringRepresentation, containsString( "realm='null'" ) );
    }

    private void testTokenSupportsRealm( ShiroAuthToken token, boolean supports, String... realms )
    {
        for ( String realm : realms )
        {
            assertThat( "Token should support '" + realm + "' realm", token.supportsRealm( realm ),
                    equalTo( supports ) );
        }
    }

    private void testBasicAuthToken( ShiroAuthToken token, String username, String password, String scheme )
            throws InvalidAuthTokenException
    {
        assertThat( "Token should have basic scheme", token.getScheme(), equalTo( scheme ) );
        assertThat( "Token have correct principal", token.getPrincipal(), equalTo( username ) );
        assertThat( "Token have correct credentials", token.getCredentials(), equalTo( password( password ) ) );
    }
}
