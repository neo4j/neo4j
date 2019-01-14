/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.server.security.enterprise.auth.integration.bolt;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.time.Clock;
import java.util.Map;

import org.neo4j.bolt.security.auth.AuthenticationException;
import org.neo4j.bolt.security.auth.BasicAuthentication;
import org.neo4j.kernel.configuration.Config;
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
            new RateLimitedAuthenticationStrategy( Clock.systemUTC(), Config.defaults() ) );
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
    public void shouldLogFailedInitPasswordChange()
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
