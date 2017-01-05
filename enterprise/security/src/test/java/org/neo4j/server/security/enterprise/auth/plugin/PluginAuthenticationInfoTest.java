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
package org.neo4j.server.security.enterprise.auth.plugin;

import org.apache.shiro.crypto.hash.SimpleHash;
import org.junit.Test;
import org.mockito.Matchers;

import java.util.List;

import org.neo4j.server.security.enterprise.auth.SecureHasher;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthenticationInfo;
import org.neo4j.server.security.enterprise.auth.plugin.spi.CacheableAuthenticationInfo;
import org.neo4j.server.security.enterprise.auth.plugin.spi.CustomCacheableAuthenticationInfo;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PluginAuthenticationInfoTest
{
    @Test
    public void shouldCreateCorrectAuthenticationInfo()
    {
        PluginAuthenticationInfo internalAuthInfo =
                PluginAuthenticationInfo.createCacheable( AuthenticationInfo.of( "thePrincipal" ), "theRealm", null );

        assertThat( (List<String>)internalAuthInfo.getPrincipals().asList(), containsInAnyOrder( "thePrincipal" ) );
    }

    @Test
    public void shouldCreateCorrectAuthenticationInfoFromCacheable()
    {
        SecureHasher hasher = mock( SecureHasher.class );
        when( hasher.hash( Matchers.any() ) ).thenReturn( new SimpleHash( "some-hash" ) );

        PluginAuthenticationInfo internalAuthInfo =
                PluginAuthenticationInfo.createCacheable(
                        CacheableAuthenticationInfo.of( "thePrincipal", new byte[]{1} ),
                        "theRealm",
                        hasher
                );

        assertThat( (List<String>)internalAuthInfo.getPrincipals().asList(), containsInAnyOrder( "thePrincipal" ) );
    }

    @Test
    public void shouldCreateCorrectAuthenticationInfoFromCustomCacheable()
    {
        SecureHasher hasher = mock( SecureHasher.class );
        when( hasher.hash( Matchers.any() ) ).thenReturn( new SimpleHash( "some-hash" ) );

        PluginAuthenticationInfo internalAuthInfo =
                PluginAuthenticationInfo.createCacheable(
                        CustomCacheableAuthenticationInfo.of( "thePrincipal", ignoredAuthToken -> true ),
                        "theRealm",
                        hasher
                );

        assertThat( (List<String>)internalAuthInfo.getPrincipals().asList(), containsInAnyOrder( "thePrincipal" ) );
    }
}
