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
import org.apache.shiro.mgt.DefaultSecurityManager;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.subject.Subject;

import java.io.IOException;
import java.time.Clock;

import org.neo4j.kernel.api.security.AuthSubject;
import org.neo4j.kernel.api.security.AuthenticationResult;
import org.neo4j.kernel.api.security.exception.IllegalCredentialsException;
import org.neo4j.server.security.auth.AuthenticationStrategy;
import org.neo4j.server.security.auth.BasicAuthManager;
import org.neo4j.server.security.auth.RateLimitedAuthenticationStrategy;
import org.neo4j.server.security.auth.User;
import org.neo4j.server.security.auth.UserRepository;

public class ShiroAuthManager extends BasicAuthManager
{
    private final SecurityManager securityManager;
    private final FileUserRealm realm;

    public ShiroAuthManager( UserRepository userRepository, AuthenticationStrategy authStrategy, boolean authEnabled )
    {
        super( userRepository, authStrategy, authEnabled );

        realm = new FileUserRealm( userRepository );
        // TODO: Do not forget realm.setCacheManager(...) before going into production...
        securityManager = new DefaultSecurityManager( realm );
    }

    public ShiroAuthManager( UserRepository users, AuthenticationStrategy authStrategy )
    {
        this( users, authStrategy, true );
    }

    public ShiroAuthManager( UserRepository users, Clock clock, boolean authEnabled )
    {
        this( users, new RateLimitedAuthenticationStrategy( clock, 3 ), authEnabled );
    }

    @Override
    public void start() throws Throwable
    {
        if ( authEnabled && realm.numberOfUsers() == 0 )
        {
            realm.newUser( "neo4j", DEFAULT_GROUP, "neo4j", true );
        }
    }

    @Override
    public AuthenticationResult authenticate( String username, String password )
    {
        AuthSubject subject = login( username, password );

        return subject.getAuthenticationResult();
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
}
