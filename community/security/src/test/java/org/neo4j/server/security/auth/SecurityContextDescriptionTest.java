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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.cypher.internal.security.SecureHasher;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.impl.api.security.OverriddenAccessMode;
import org.neo4j.kernel.impl.api.security.RestrictedAccessMode;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.server.security.systemgraph.BasicSystemGraphRealm;
import org.neo4j.server.security.systemgraph.SecurityGraphInitializer;
import org.neo4j.server.security.systemgraph.SystemGraphRealmHelper;
import org.neo4j.time.Clocks;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.neo4j.server.security.auth.SecurityTestUtils.authToken;
import static org.neo4j.server.security.auth.SecurityTestUtils.credentialFor;

class SecurityContextDescriptionTest
{
    private SecurityContext context;

    @BeforeEach
    void setup() throws Throwable
    {
        SystemGraphRealmHelper realmHelper = spy( new SystemGraphRealmHelper( null, new SecureHasher() ) );
        BasicSystemGraphRealm realm = new BasicSystemGraphRealm( SecurityGraphInitializer.NO_OP, realmHelper,
                new RateLimitedAuthenticationStrategy( Clocks.systemClock(), Config.defaults() ) );
        User user =  new User.Builder( "johan", credentialFor( "bar" ) ).build();
        doReturn( user ).when( realmHelper ).getUser( "johan" );
        context = realm.login( authToken( "johan", "bar" ) ).authorize( LoginContext.IdLookup.EMPTY, GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
    }

    @Test
    void shouldMakeNiceDescription()
    {
        assertThat( context.description(), equalTo( "user 'johan' with FULL" ) );
    }

    @Test
    void shouldMakeNiceDescriptionWithMode()
    {
        SecurityContext modified = context.withMode( AccessMode.Static.WRITE );
        assertThat( modified.description(), equalTo( "user 'johan' with WRITE" ) );
    }

    @Test
    void shouldMakeNiceDescriptionRestricted()
    {
        SecurityContext restricted =
                context.withMode( new RestrictedAccessMode( context.mode(), AccessMode.Static.READ ) );
        assertThat( restricted.description(), equalTo( "user 'johan' with FULL restricted to READ" ) );
    }

    @Test
    void shouldMakeNiceDescriptionOverridden()
    {
        SecurityContext overridden =
                context.withMode( new OverriddenAccessMode( context.mode(), AccessMode.Static.READ ) );
        assertThat( overridden.description(), equalTo( "user 'johan' with FULL overridden by READ" ) );
    }

    @Test
    void shouldMakeNiceDescriptionAuthDisabled()
    {
        SecurityContext disabled = SecurityContext.AUTH_DISABLED;
        assertThat( disabled.description(), equalTo( "AUTH_DISABLED with FULL" ) );
    }

    @Test
    void shouldMakeNiceDescriptionAuthDisabledAndRestricted()
    {
        SecurityContext disabled = SecurityContext.AUTH_DISABLED;
        SecurityContext restricted =
                disabled.withMode( new RestrictedAccessMode( disabled.mode(), AccessMode.Static.READ ) );
        assertThat( restricted.description(), equalTo( "AUTH_DISABLED with FULL restricted to READ" ) );
    }
}
