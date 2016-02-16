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
import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.ExpiredCredentialsException;
import org.apache.shiro.authc.SimpleAccount;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.apache.shiro.authc.credential.CredentialsMatcher;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.realm.AuthorizingRealm;
import org.apache.shiro.subject.PrincipalCollection;

import java.io.IOException;

import org.neo4j.kernel.api.security.exception.IllegalCredentialsException;

/**
 * Shiro realm wrapping FileUserRepository
 */
public class FileUserRealm extends AuthorizingRealm
{
    private final FileUserRepository userRepository;

    private final CredentialsMatcher credentialsMatcher = new CredentialsMatcher()
    {
        @Override
        public boolean doCredentialsMatch( AuthenticationToken token, AuthenticationInfo info )
        {
            UsernamePasswordToken usernamePasswordToken = (UsernamePasswordToken) token;
            String infoUserName = (String) info.getPrincipals().getPrimaryPrincipal();
            Credential infoCredential = (Credential) info.getCredentials();

            boolean userNameMatches = infoUserName.equals( usernamePasswordToken.getUsername() );
            boolean credentialsMatches =
                    infoCredential.matchesPassword( new String( usernamePasswordToken.getPassword() ) );

            return userNameMatches && credentialsMatches;
        }
    };

    public FileUserRealm( FileUserRepository userRepository )
    {
        this.userRepository = userRepository;
        setCredentialsMatcher( credentialsMatcher );
    }

    @Override
    protected AuthorizationInfo doGetAuthorizationInfo( PrincipalCollection principals )
    {
        return null;
    }

    @Override
    protected AuthenticationInfo doGetAuthenticationInfo( AuthenticationToken token ) throws AuthenticationException
    {
        UsernamePasswordToken usernamePasswordToken = (UsernamePasswordToken) token;

        User user = userRepository.findByName( usernamePasswordToken.getUsername() );

        // TODO: This will not work if AuthenticationInfo is cached
        if (user.passwordChangeRequired())
        {
            throw new ExpiredCredentialsException("Password change required");
        }

        return new SimpleAccount( user.name(), user.credentials(), getName() );
    }

    int numberOfUsers()
    {
        return userRepository.numberOfUsers();
    }

    User newUser( String username, String initialPassword, boolean requirePasswordChange ) throws
            IOException, IllegalCredentialsException
    {
        assertValidName( username );

        User user = new User.Builder()
                .withName( username )
                .withCredentials( Credential.forPassword( initialPassword ) )
                .withRequiredPasswordChange( requirePasswordChange )
                .build();
        userRepository.create( user );

        return user;
    }

    private void assertValidName( String name )
    {
        if ( !userRepository.isValidName( name ) )
        {
            throw new IllegalArgumentException(
                    "User name contains illegal characters. Please use simple ascii characters and numbers." );
        }
    }
}
