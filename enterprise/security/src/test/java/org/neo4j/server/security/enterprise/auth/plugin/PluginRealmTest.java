/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.junit.Test;

import java.time.Clock;
import java.util.Collection;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.server.security.enterprise.auth.SecureHasher;
import org.neo4j.server.security.enterprise.auth.plugin.api.AuthToken;
import org.neo4j.server.security.enterprise.auth.plugin.api.AuthenticationException;
import org.neo4j.server.security.enterprise.auth.plugin.api.RealmOperations;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthInfo;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthPlugin;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthenticationInfo;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthenticationPlugin;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthorizationInfo;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthorizationPlugin;
import org.neo4j.server.security.enterprise.auth.plugin.spi.RealmLifecycle;
import org.neo4j.server.security.enterprise.log.SecurityLog;

import static java.lang.String.format;
import static org.mockito.Mockito.mock;
import static org.neo4j.logging.AssertableLogProvider.inLog;

public class PluginRealmTest
{
    private static final RealmOperations IGNORED = null;

    private Config config = mock( Config.class );
    private AssertableLogProvider log = new AssertableLogProvider();
    private SecurityLog securityLog = new SecurityLog( log.getLog( this.getClass() ) );

    @Test
    public void shouldLogToSecurityLogFromAuthPlugin() throws Throwable
    {
        PluginRealm pluginRealm = new PluginRealm( new LoggingAuthPlugin(), config, securityLog, Clock.systemUTC(),
                mock( SecureHasher.class ) );
        pluginRealm.initialize( IGNORED );
        assertLogged( "LoggingAuthPlugin" );
    }

    @Test
    public void shouldLogToSecurityLogFromAuthenticationPlugin() throws Throwable
    {
        PluginRealm pluginRealm = new PluginRealm(
                new LoggingAuthenticationPlugin(),
                null,
                config, securityLog, Clock.systemUTC(), mock( SecureHasher.class ) );
        pluginRealm.initialize( IGNORED );
        assertLogged( "LoggingAuthenticationPlugin" );
    }

    @Test
    public void shouldLogToSecurityLogFromAuthorizationPlugin() throws Throwable
    {
        PluginRealm pluginRealm = new PluginRealm(
                null,
                new LoggingAuthorizationPlugin(),
                config, securityLog, Clock.systemUTC(), mock( SecureHasher.class ) );
        pluginRealm.initialize( IGNORED );
        assertLogged( "LoggingAuthorizationPlugin" );
    }

    private void assertLogged( String name )
    {
        log.assertExactly(
                inLog( this.getClass() ).info( format( "{plugin-%s} info line", name ) ),
                inLog( this.getClass() ).warn( format( "{plugin-%s} warn line", name ) ),
                inLog( this.getClass() ).error( format( "{plugin-%s} error line", name ) )
            );
    }

    private class LoggingAuthPlugin extends TestAuthPlugin
    {
        @Override
        public void initialize( RealmOperations realmOperations ) throws Throwable
        {
            logLines( realmOperations );
        }
    }

    private class LoggingAuthenticationPlugin extends TestAuthenticationPlugin
    {
        @Override
        public void initialize( RealmOperations realmOperations ) throws Throwable
        {
            logLines( realmOperations );
        }
    }

    private class LoggingAuthorizationPlugin extends TestAuthorizationPlugin
    {
        @Override
        public void initialize( RealmOperations realmOperations ) throws Throwable
        {
            logLines( realmOperations );
        }
    }

    private static void logLines( RealmOperations realmOperations ) throws Throwable
    {
        RealmOperations.Log log = realmOperations.log();
        if ( log.isDebugEnabled() )
        {
            log.debug( "debug line" );
        }
        log.info( "info line" );
        log.warn( "warn line" );
        log.error( "error line" );
    }
}
