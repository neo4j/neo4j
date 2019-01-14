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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.security.OverriddenAccessMode;
import org.neo4j.kernel.impl.api.security.RestrictedAccessMode;
import org.neo4j.time.Clocks;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.neo4j.server.security.auth.SecurityTestUtils.authToken;

public class SecurityContextDescriptionTest
{
    private BasicAuthManager manager;
    private SecurityContext context;

    @Before
    public void setup() throws Throwable
    {
        manager =
            new BasicAuthManager(
                    new InMemoryUserRepository(),
                    new BasicPasswordPolicy(),
                    Clocks.systemClock(),
                    new InMemoryUserRepository(),
                    Config.defaults() );
        manager.init();
        manager.start();
        manager.newUser( "johan", "bar", false );
        context = manager.login( authToken( "johan", "bar" ) ).authorize( s -> -1 );
    }

    @After
    public void teardown() throws Throwable
    {
        manager.stop();
        manager.shutdown();
    }

    @Test
    public void shouldMakeNiceDescription()
    {
        assertThat( context.description(), equalTo( "user 'johan' with FULL" ) );
    }

    @Test
    public void shouldMakeNiceDescriptionWithMode()
    {
        SecurityContext modified = context.withMode( AccessMode.Static.WRITE );
        assertThat( modified.description(), equalTo( "user 'johan' with WRITE" ) );
    }

    @Test
    public void shouldMakeNiceDescriptionRestricted()
    {
        SecurityContext restricted =
                context.withMode( new RestrictedAccessMode( context.mode(), AccessMode.Static.READ ) );
        assertThat( restricted.description(), equalTo( "user 'johan' with FULL restricted to READ" ) );
    }

    @Test
    public void shouldMakeNiceDescriptionOverridden()
    {
        SecurityContext overridden =
                context.withMode( new OverriddenAccessMode( context.mode(), AccessMode.Static.READ ) );
        assertThat( overridden.description(), equalTo( "user 'johan' with FULL overridden by READ" ) );
    }

    @Test
    public void shouldMakeNiceDescriptionAuthDisabled()
    {
        SecurityContext disabled = SecurityContext.AUTH_DISABLED;
        assertThat( disabled.description(), equalTo( "AUTH_DISABLED with FULL" ) );
    }

    @Test
    public void shouldMakeNiceDescriptionAuthDisabledAndRestricted()
    {
        SecurityContext disabled = SecurityContext.AUTH_DISABLED;
        SecurityContext restricted =
                disabled.withMode( new RestrictedAccessMode( disabled.mode(), AccessMode.Static.READ ) );
        assertThat( restricted.description(), equalTo( "AUTH_DISABLED with FULL restricted to READ" ) );
    }

}
