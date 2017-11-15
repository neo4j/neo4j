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
package org.neo4j.server.security.enterprise.auth.integration.bolt;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.time.Clock;
import java.util.Map;

import org.neo4j.bolt.security.auth.AuthenticationException;
import org.neo4j.bolt.security.auth.BasicAuthentication;
import org.neo4j.server.security.auth.InMemoryUserRepository;
import org.neo4j.server.security.auth.RateLimitedAuthenticationStrategy;
import org.neo4j.server.security.enterprise.auth.MultiRealmAuthManagerRule;
import org.neo4j.server.security.enterprise.auth.MultiRealmAuthManagerRule.FullSecurityLog;

import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.test.assertion.Assert.assertException;

public class BoltInitChangePasswordTest
{
    @Rule
    public MultiRealmAuthManagerRule authManagerRule = new MultiRealmAuthManagerRule( new InMemoryUserRepository(),
            new RateLimitedAuthenticationStrategy( Clock.systemUTC(), 3 ) );
    private BasicAuthentication authentication;

    @Before
    public void setup() throws Throwable
    {
        authentication = new BasicAuthentication( authManagerRule.getManager(), authManagerRule.getManager() );
        authManagerRule.getManager().getUserManager().newUser( "neo4j", "123", true );
    }

    @Test
    public void shouldLogInitPasswordChange() throws Throwable
    {
        authentication.authenticate( authToken( "neo4j", "123", "secret" ) );

        FullSecurityLog fullLog = authManagerRule.getFullSecurityLog();
        fullLog.assertHasLine( "neo4j", "logged in (password change required)" );
        fullLog.assertHasLine( "neo4j", "changed password" );
    }

    @Test
    public void shouldLogFailedInitPasswordChange() throws Throwable
    {
        assertException( () -> authentication.authenticate( authToken( "neo4j", "123", "123" ) ),
                AuthenticationException.class, "Old password and new password cannot be the same." );

        FullSecurityLog fullLog = authManagerRule.getFullSecurityLog();
        fullLog.assertHasLine( "neo4j", "logged in (password change required)" );
        fullLog.assertHasLine( "neo4j", "tried to change password: Old password and new password cannot be the same." );
    }

    private Map<String,Object> authToken( String username, String password, String newPassword )
    {
        return map( "principal", username, "credentials", password,
                "new_credentials", newPassword, "scheme", "basic" );
    }
}
