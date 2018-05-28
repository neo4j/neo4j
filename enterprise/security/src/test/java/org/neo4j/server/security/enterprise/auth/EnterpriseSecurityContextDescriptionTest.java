/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.server.security.enterprise.auth;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.time.Clock;

import org.neo4j.kernel.api.security.AccessMode;
import org.neo4j.kernel.enterprise.api.security.EnterpriseSecurityContext;
import org.neo4j.kernel.impl.api.security.OverriddenAccessMode;
import org.neo4j.kernel.impl.api.security.RestrictedAccessMode;
import org.neo4j.server.security.auth.InMemoryUserRepository;
import org.neo4j.server.security.auth.RateLimitedAuthenticationStrategy;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.neo4j.server.security.auth.SecurityTestUtils.authToken;
import static org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLISHER;

public class EnterpriseSecurityContextDescriptionTest
{
    @Rule
    public MultiRealmAuthManagerRule authManagerRule = new MultiRealmAuthManagerRule( new InMemoryUserRepository(),
            new RateLimitedAuthenticationStrategy( Clock.systemUTC(), 3 ) );

    private EnterpriseSecurityContext context;
    private EnterpriseUserManager manager;

    @Before
    public void setUp() throws Throwable
    {
        authManagerRule.getManager().start();
        manager = authManagerRule.getManager().getUserManager();
        manager.newUser( "mats", "foo", false );
        context = authManagerRule.getManager().login( authToken( "mats", "foo" ) );
    }

    @Test
    public void shouldMakeNiceDescriptionWithoutRoles() throws Throwable
    {
        assertThat( context.description(), equalTo( "user 'mats' with no roles" ) );
    }

    @Test
    public void shouldMakeNiceDescriptionWithRoles() throws Throwable
    {
        manager.newRole( "role1", "mats" );
        manager.addRoleToUser( PUBLISHER, "mats" );

        assertThat( context.description(), equalTo( "user 'mats' with roles [publisher,role1]" ) );
    }

    @Test
    public void shouldMakeNiceDescriptionFrozen() throws Throwable
    {
        manager.newRole( "role1", "mats" );
        manager.addRoleToUser( PUBLISHER, "mats" );

        EnterpriseSecurityContext frozen = context.freeze();
        assertThat( frozen.description(), equalTo( "user 'mats' with roles [publisher,role1]" ) );
    }

    @Test
    public void shouldMakeNiceDescriptionWithMode() throws Throwable
    {
        manager.newRole( "role1", "mats" );
        manager.addRoleToUser( PUBLISHER, "mats" );

        EnterpriseSecurityContext modified = context.withMode( AccessMode.Static.CREDENTIALS_EXPIRED );
        assertThat( modified.description(), equalTo( "user 'mats' with CREDENTIALS_EXPIRED" ) );
    }

    @Test
    public void shouldMakeNiceDescriptionRestricted() throws Throwable
    {
        manager.newRole( "role1", "mats" );
        manager.addRoleToUser( PUBLISHER, "mats" );

        EnterpriseSecurityContext restricted =
                context.withMode( new RestrictedAccessMode( context.mode(), AccessMode.Static.READ ) );
        assertThat( restricted.description(), equalTo( "user 'mats' with roles [publisher,role1] restricted to READ" ) );
    }

    @Test
    public void shouldMakeNiceDescriptionOverridden() throws Throwable
    {
        manager.newRole( "role1", "mats" );
        manager.addRoleToUser( PUBLISHER, "mats" );

        EnterpriseSecurityContext overridden =
                context.withMode( new OverriddenAccessMode( context.mode(), AccessMode.Static.READ ) );
        assertThat( overridden.description(), equalTo( "user 'mats' with roles [publisher,role1] overridden by READ" ) );
    }

    @Test
    public void shouldMakeNiceDescriptionAuthDisabled() throws Throwable
    {
        EnterpriseSecurityContext disabled = EnterpriseSecurityContext.AUTH_DISABLED;
        assertThat( disabled.description(), equalTo( "AUTH_DISABLED with FULL" ) );
    }

    @Test
    public void shouldMakeNiceDescriptionAuthDisabledAndRestricted() throws Throwable
    {
        EnterpriseSecurityContext disabled = EnterpriseSecurityContext.AUTH_DISABLED;
        EnterpriseSecurityContext restricted =
                disabled.withMode( new RestrictedAccessMode( disabled.mode(), AccessMode.Static.READ ) );
        assertThat( restricted.description(), equalTo( "AUTH_DISABLED with FULL restricted to READ" ) );
    }
}
