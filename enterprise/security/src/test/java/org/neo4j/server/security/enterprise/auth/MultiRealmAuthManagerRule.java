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
package org.neo4j.server.security.enterprise.auth;

import org.apache.shiro.cache.MemoryConstrainedCacheManager;
import org.junit.After;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.util.Collections;

import org.neo4j.kernel.api.security.AuthSubject;
import org.neo4j.kernel.impl.enterprise.SecurityLog;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.server.security.auth.AuthenticationStrategy;
import org.neo4j.server.security.auth.BasicPasswordPolicy;
import org.neo4j.server.security.auth.UserRepository;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.neo4j.logging.AssertableLogProvider.inLog;

public class MultiRealmAuthManagerRule implements TestRule
{
    private UserRepository users;
    private AuthenticationStrategy authStrategy;
    private MultiRealmAuthManager manager;
    private EnterpriseUserManager userManager;
    private AssertableLogProvider logProvider;
    private SecurityLog securityLog;

    public MultiRealmAuthManagerRule(
            UserRepository users,
            AuthenticationStrategy authStrategy
    )
    {
        this.users = users;
        this.authStrategy = authStrategy;

        logProvider = new AssertableLogProvider();
    }

    private void setupAuthManager( AuthenticationStrategy authStrategy ) throws Throwable
    {
        Log log = logProvider.getLog( this.getClass() );
        securityLog = new SecurityLog( log );
        InternalFlatFileRealm internalFlatFileRealm =
                new InternalFlatFileRealm(
                        users,
                        new InMemoryRoleRepository(),
                        new BasicPasswordPolicy(),
                        authStrategy,
                        mock( JobScheduler.class )
                    );

        manager = new MultiRealmAuthManager( internalFlatFileRealm, Collections.singleton( internalFlatFileRealm ),
                new MemoryConstrainedCacheManager(), securityLog, true );
        manager.init();

        userManager = manager.getUserManager();
    }

    public EnterpriseAuthAndUserManager getManager()
    {
        return manager;
    }

    public AuthSubject makeSubject( ShiroSubject shiroSubject )
    {
        return new StandardEnterpriseAuthSubject( manager, shiroSubject, securityLog );
    }

    @Override
    public Statement apply( final Statement base, final Description description )
    {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
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

    public void assertExactlyInfoInLog( String message )
    {
        logProvider.assertExactly( inLog( this.getClass() ).info( message ) );
    }

    public void assertExactlyInfoInLog( String message, Object... values )
    {
        logProvider.assertExactly( inLog( this.getClass() ).info( message, values ) );
    }

    public void assertExactlyWarnInLog( String message )
    {
        logProvider.assertExactly( inLog( this.getClass() ).warn( message ) );
    }

    public void assertExactlyWarnInLog( String message, Object... values )
    {
        logProvider.assertExactly( inLog( this.getClass() ).warn( message, values ) );
    }

    public void assertExactlyErrorInLog( String message )
    {
        logProvider.assertExactly( inLog( this.getClass() ).error( message ) );
    }

    public void assertExactlyErrorInLog( String message, Object... values )
    {
        logProvider.assertExactly( inLog( this.getClass() ).error( message, values ) );
    }
}
