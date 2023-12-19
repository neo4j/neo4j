/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.server.security.enterprise.auth.plugin;

import org.junit.Test;

import java.time.Clock;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.server.security.enterprise.auth.SecureHasher;
import org.neo4j.server.security.enterprise.auth.plugin.api.AuthProviderOperations;
import org.neo4j.server.security.enterprise.log.SecurityLog;

import static java.lang.String.format;
import static org.mockito.Mockito.mock;
import static org.neo4j.logging.AssertableLogProvider.inLog;

public class PluginRealmTest
{
    private Config config = mock( Config.class );
    private AssertableLogProvider log = new AssertableLogProvider();
    private SecurityLog securityLog = new SecurityLog( log.getLog( this.getClass() ) );

    @Test
    public void shouldLogToSecurityLogFromAuthPlugin() throws Throwable
    {
        PluginRealm pluginRealm = new PluginRealm( new LoggingAuthPlugin(), config, securityLog, Clock.systemUTC(),
                mock( SecureHasher.class ) );
        pluginRealm.initialize();
        assertLogged( "LoggingAuthPlugin" );
    }

    @Test
    public void shouldLogToSecurityLogFromAuthenticationPlugin() throws Throwable
    {
        PluginRealm pluginRealm = new PluginRealm(
                new LoggingAuthenticationPlugin(),
                null,
                config, securityLog, Clock.systemUTC(), mock( SecureHasher.class ) );
        pluginRealm.initialize( );
        assertLogged( "LoggingAuthenticationPlugin" );
    }

    @Test
    public void shouldLogToSecurityLogFromAuthorizationPlugin() throws Throwable
    {
        PluginRealm pluginRealm = new PluginRealm(
                null,
                new LoggingAuthorizationPlugin(),
                config, securityLog, Clock.systemUTC(), mock( SecureHasher.class ) );
        pluginRealm.initialize();
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
        public void initialize( AuthProviderOperations api )
        {
            logLines( api );
        }
    }

    private class LoggingAuthenticationPlugin extends TestAuthenticationPlugin
    {
        @Override
        public void initialize( AuthProviderOperations api )
        {
            logLines( api );
        }
    }

    private class LoggingAuthorizationPlugin extends TestAuthorizationPlugin
    {
        @Override
        public void initialize( AuthProviderOperations api )
        {
            logLines( api );
        }
    }

    private static void logLines( AuthProviderOperations api )
    {
        AuthProviderOperations.Log log = api.log();
        if ( log.isDebugEnabled() )
        {
            log.debug( "debug line" );
        }
        log.info( "info line" );
        log.warn( "warn line" );
        log.error( "error line" );
    }
}
