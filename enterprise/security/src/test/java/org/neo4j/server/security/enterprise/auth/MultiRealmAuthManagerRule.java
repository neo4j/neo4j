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

import org.apache.shiro.cache.MemoryConstrainedCacheManager;
import org.junit.After;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.StringWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.neo4j.kernel.api.security.SecurityContext;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.logging.FormattedLog;
import org.neo4j.logging.Log;
import org.neo4j.server.security.auth.AuthenticationStrategy;
import org.neo4j.server.security.auth.BasicPasswordPolicy;
import org.neo4j.server.security.auth.InMemoryUserRepository;
import org.neo4j.server.security.auth.UserRepository;
import org.neo4j.server.security.enterprise.log.SecurityLog;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class MultiRealmAuthManagerRule implements TestRule
{
    private UserRepository users;
    private AuthenticationStrategy authStrategy;
    private MultiRealmAuthManager manager;
    private SecurityLog securityLog;
    private StringWriter securityLogWriter;

    public MultiRealmAuthManagerRule(
            UserRepository users,
            AuthenticationStrategy authStrategy )
    {
        this.users = users;
        this.authStrategy = authStrategy;
    }

    private void setupAuthManager( AuthenticationStrategy authStrategy ) throws Throwable
    {
        FormattedLog.Builder builder = FormattedLog.withUTCTimeZone();
        securityLogWriter = new StringWriter();
        Log log = builder.toWriter( securityLogWriter );

        securityLog = new SecurityLog( log );
        InternalFlatFileRealm internalFlatFileRealm =
                new InternalFlatFileRealm(
                        users,
                        new InMemoryRoleRepository(),
                        new BasicPasswordPolicy(),
                        authStrategy,
                        mock( JobScheduler.class ),
                        new InMemoryUserRepository(),
                        new InMemoryUserRepository()
                    );

        manager = new MultiRealmAuthManager( internalFlatFileRealm, Collections.singleton( internalFlatFileRealm ),
                new MemoryConstrainedCacheManager(), securityLog, true );
        manager.init();
    }

    public EnterpriseAuthAndUserManager getManager()
    {
        return manager;
    }

    public SecurityContext makeSecurityContext( ShiroSubject shiroSubject )
    {
        return new StandardEnterpriseSecurityContext( manager, shiroSubject );
    }

    @Override
    public Statement apply( final Statement base, final Description description )
    {
        return new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                try
                {
                    setupAuthManager( authStrategy );
                    base.evaluate();
                }
                catch ( Throwable t )
                {
                    fail( "Got unexpected exception " + t );
                }
                finally
                {
                    try
                    {
                        tearDown();
                    }
                    catch ( Throwable t )
                    {
                        throw new RuntimeException( "Failed to shut down MultiRealmAuthManager", t );
                    }
                }
            }
        };
    }

    @After
    public void tearDown() throws Throwable
    {
        manager.stop();
        manager.shutdown();
    }

    public FullSecurityLog getFullSecurityLog()
    {
        return new FullSecurityLog( securityLogWriter.getBuffer().toString().split( "\n" ) );
    }

    public static class FullSecurityLog
    {
        List<String> lines;

        private FullSecurityLog( String[] logLines )
        {
            lines = Arrays.asList( logLines );
        }

        public void assertHasLine( String subject, String msg )
        {
            assertThat( lines, hasItem( containsString( "[" + subject + "]: " + msg ) ) );
        }
    }
}
