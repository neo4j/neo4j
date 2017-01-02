/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.server.security.enterprise.auth;

import org.junit.Test;

import java.util.Map;

import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.neo4j.helpers.collection.MapUtil.map;

public class ShiroAuthTokenTest
{
    private final String USERNAME = "myuser";
    private final String PASSWORD = "mypw123";

    @Test
    public void shouldSupportBasicAuthToken() throws Exception
    {
        ShiroAuthToken token = new ShiroAuthToken( AuthToken.newBasicAuthToken( USERNAME, PASSWORD ) );
        testBasicAuthToken( token, USERNAME, PASSWORD, AuthToken.BASIC_SCHEME );
        assertThat( "Token map should have only expected values", token.getAuthTokenMap(),
                equalTo( map( AuthToken.PRINCIPAL, USERNAME, AuthToken.CREDENTIALS, PASSWORD, AuthToken.SCHEME_KEY,
                        AuthToken.BASIC_SCHEME ) ) );
        testTokenSupportsRealm( token, true, "unknown", "native", "ldap" );
    }

    @Test
    public void shouldSupportBasicAuthTokenWithWildcardRealm() throws Exception
    {
        ShiroAuthToken token = new ShiroAuthToken( AuthToken.newBasicAuthToken( USERNAME, PASSWORD, "*" ) );
        testBasicAuthToken( token, USERNAME, PASSWORD, AuthToken.BASIC_SCHEME );
        assertThat( "Token map should have only expected values", token.getAuthTokenMap(),
                equalTo( map( AuthToken.PRINCIPAL, USERNAME, AuthToken.CREDENTIALS, PASSWORD, AuthToken.SCHEME_KEY,
                        AuthToken.BASIC_SCHEME, AuthToken.REALM_KEY, "*" ) ) );
        testTokenSupportsRealm( token, true, "unknown", "native", "ldap" );
    }

    @Test
    public void shouldSupportBasicAuthTokenWithSpecificRealm() throws Exception
    {
        String realm = "ldap";
        ShiroAuthToken token = new ShiroAuthToken( AuthToken.newBasicAuthToken( USERNAME, PASSWORD, realm ) );
        testBasicAuthToken( token, USERNAME, PASSWORD, AuthToken.BASIC_SCHEME );
        assertThat( "Token map should have only expected values", token.getAuthTokenMap(),
                equalTo( map( AuthToken.PRINCIPAL, USERNAME, AuthToken.CREDENTIALS, PASSWORD, AuthToken.SCHEME_KEY,
                        AuthToken.BASIC_SCHEME, AuthToken.REALM_KEY, "ldap" ) ) );
        testTokenSupportsRealm( token, true, realm );
        testTokenSupportsRealm( token, false, "unknown", "native" );
    }

    @Test
    public void shouldSupportCustomAuthTokenWithSpecificRealm() throws Exception
    {
        String realm = "ldap";
        ShiroAuthToken token =
                new ShiroAuthToken( AuthToken.newCustomAuthToken( USERNAME, PASSWORD, realm, AuthToken.BASIC_SCHEME ) );
        testBasicAuthToken( token, USERNAME, PASSWORD, AuthToken.BASIC_SCHEME );
        assertThat( "Token map should have only expected values", token.getAuthTokenMap(),
                equalTo( map( AuthToken.PRINCIPAL, USERNAME, AuthToken.CREDENTIALS, PASSWORD, AuthToken.SCHEME_KEY,
                        AuthToken.BASIC_SCHEME, AuthToken.REALM_KEY, "ldap" ) ) );
        testTokenSupportsRealm( token, true, realm );
        testTokenSupportsRealm( token, false, "unknown", "native" );
    }

    @Test
    public void shouldSupportCustomAuthTokenWithSpecificRealmAndParameters() throws Exception
    {
        String realm = "ldap";
        Map<String,Object> params = map( "a", "A", "b", "B" );
        ShiroAuthToken token =
                new ShiroAuthToken(
                        AuthToken.newCustomAuthToken( USERNAME, PASSWORD, realm, AuthToken.BASIC_SCHEME, params ) );
        testBasicAuthToken( token, USERNAME, PASSWORD, AuthToken.BASIC_SCHEME );
        assertThat( "Token map should have only expected values", token.getAuthTokenMap(),
                equalTo( map( AuthToken.PRINCIPAL, USERNAME, AuthToken.CREDENTIALS, PASSWORD, AuthToken.SCHEME_KEY,
                        AuthToken.BASIC_SCHEME, AuthToken.REALM_KEY, "ldap",
                        "parameters", params ) ) );
        testTokenSupportsRealm( token, true, realm );
        testTokenSupportsRealm( token, false, "unknown", "native" );
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
        assertThat( "Token have correct credentials", token.getCredentials(), equalTo( password ) );
    }
}
