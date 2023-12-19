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

import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.logging.FormattedLog;
import org.neo4j.logging.Log;
import org.neo4j.scheduler.JobScheduler;
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
                new MemoryConstrainedCacheManager(), securityLog, true, false, Collections.emptyMap() );
        manager.init();
    }

    public EnterpriseAuthAndUserManager getManager()
    {
        return manager;
    }

    public LoginContext makeLoginContext( ShiroSubject shiroSubject )
    {
        return new StandardEnterpriseLoginContext( manager, shiroSubject );
    }

    @Override
    public Statement apply( final Statement base, final Description description )
    {
        return new Statement()
        {
            @Override
            public void evaluate()
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
