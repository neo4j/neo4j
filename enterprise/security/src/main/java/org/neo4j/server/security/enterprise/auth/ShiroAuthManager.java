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

import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.ExpiredCredentialsException;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.apache.shiro.cache.ehcache.EhCacheManager;
import org.apache.shiro.mgt.DefaultSecurityManager;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.subject.Subject;

import java.io.IOException;
import java.time.Clock;

import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.kernel.api.security.AuthSubject;
import org.neo4j.kernel.api.security.AuthenticationResult;
import org.neo4j.kernel.api.security.exception.IllegalCredentialsException;
import org.neo4j.server.security.auth.AuthenticationStrategy;
import org.neo4j.server.security.auth.BasicAuthManager;
import org.neo4j.server.security.auth.PasswordPolicy;
import org.neo4j.server.security.auth.RateLimitedAuthenticationStrategy;
import org.neo4j.server.security.auth.User;
import org.neo4j.server.security.auth.UserRepository;

public class ShiroAuthManager extends BasicAuthManager
{
    private final SecurityManager securityManager;
    private final EhCacheManager cacheManager;
    private final FileUserRealm realm;

    public ShiroAuthManager( UserRepository userRepository, PasswordPolicy passwordPolicy, AuthenticationStrategy authStrategy, boolean authEnabled )
    {
        super( userRepository, passwordPolicy, authStrategy, authEnabled );

        realm = new FileUserRealm( userRepository );
        // TODO: Maybe MemoryConstrainedCacheManager is good enough if we do not need timeToLiveSeconds? It would be one less dependency.
        //       Or we could try to reuse Hazelcast which is already a dependency, but we would need to write some glue code.
        cacheManager = new EhCacheManager();
        securityManager = new DefaultSecurityManager( realm );
    }

    public ShiroAuthManager( UserRepository users, PasswordPolicy passwordPolicy, AuthenticationStrategy authStrategy )
    {
        this( users, passwordPolicy, authStrategy, true );
    }

    public ShiroAuthManager( UserRepository users, PasswordPolicy passwordPolicy, Clock clock, boolean authEnabled )
    {
        this( users, passwordPolicy, new RateLimitedAuthenticationStrategy( clock, 3 ), authEnabled );
    }

    @Override
    public void init() throws Throwable
    {
        super.init();

        cacheManager.init();
        realm.setCacheManager( cacheManager );
        realm.init();
    }

    @Override
    public void start() throws Throwable
    {
        users.start();

        if ( authEnabled && realm.numberOfUsers() == 0 )
        {
            realm.newUser( "neo4j", DEFAULT_GROUP, "neo4j", true );
        }
    }

    @Override
    public void shutdown() throws Throwable
    {
        super.shutdown();

        realm.setCacheManager( null );
        cacheManager.destroy();
    }

    @Override
    public User newUser( String username, String initialPassword, boolean requirePasswordChange ) throws IOException,
            IllegalCredentialsException
    {
        assertAuthEnabled();
        return realm.newUser( username, DEFAULT_GROUP, initialPassword, requirePasswordChange );
    }

    public User newUser( String username, String group, String initialPassword, boolean requirePasswordChange ) throws
            IOException,
            IllegalCredentialsException
    {
        assertAuthEnabled();
        return realm.newUser( username, group, initialPassword, requirePasswordChange );
    }

    public AuthSubject login( String username, String password )
    {
        assertAuthEnabled();

        Subject subject = new Subject.Builder(securityManager).buildSubject();

        UsernamePasswordToken token = new UsernamePasswordToken( username, password );
        AuthenticationResult result = AuthenticationResult.SUCCESS;

        if ( !authStrategy.isAuthenticationPermitted( username ) )
        {
            result = AuthenticationResult.TOO_MANY_ATTEMPTS;
        }
        else
        {
            try
            {
                subject.login( token );
            }
            catch ( ExpiredCredentialsException e )
            {
                result = AuthenticationResult.PASSWORD_CHANGE_REQUIRED;
            }
            catch ( AuthenticationException e )
            {
                result = AuthenticationResult.FAILURE;
            }
            authStrategy.updateWithAuthenticationResult( result, username );
        }
        return new ShiroAuthSubject(this, subject, result);
    }

    @Override
    public void setPassword( AuthSubject authSubject, String username, String password ) throws IOException,
            IllegalCredentialsException
    {
        ShiroAuthSubject shiroAuthSubject = ShiroAuthSubject.castOrFail( authSubject );

        if ( !shiroAuthSubject.doesUsernameMatch( username ) )
        {
            throw new AuthorizationViolationException( "Invalid attempt to change the password for user " + username );
        }

        passwordPolicy.validatePassword( password );

        setUserPassword( username, password );
    }
}
