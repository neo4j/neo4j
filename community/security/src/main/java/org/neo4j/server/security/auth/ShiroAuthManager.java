/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.ExpiredCredentialsException;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.apache.shiro.mgt.DefaultSecurityManager;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.ThreadContext;

import java.time.Clock;

import org.neo4j.kernel.api.security.AuthenticationResult;

public class ShiroAuthManager extends BasicAuthManager
{
    private final SecurityManager securityManager;
    private final FileUserRealm realm;

    public ShiroAuthManager( FileUserRepository userRepository, Clock clock, boolean authEnabled )
    {
        super( userRepository, clock, authEnabled );

        realm = new FileUserRealm( userRepository );
        //  : Do not forget realm.setCacheManager(...) before going into production...
        securityManager = new DefaultSecurityManager( realm );
    }

    @Override
    public void start() throws Throwable
    {
        if ( authEnabled && realm.numberOfUsers() == 0 )
        {
            realm.newUser( "neo4j", "neo4j", true );
        }
    }

    @Override
    public AuthenticationResult authenticate( String username, String password )
    {
        assertAuthEnabled();

        ThreadContext.bind(securityManager);

        Subject subject = new Subject.Builder(securityManager).buildSubject();
        ThreadContext.bind(subject);

        UsernamePasswordToken token = new UsernamePasswordToken( username, password );
        try
        {
            subject.login( token );
        }
        catch ( ExpiredCredentialsException e )
        {
            return AuthenticationResult.PASSWORD_CHANGE_REQUIRED;
        }
        catch ( AuthenticationException e )
        {
            return AuthenticationResult.FAILURE;
        }
        return AuthenticationResult.SUCCESS;
    }
}
