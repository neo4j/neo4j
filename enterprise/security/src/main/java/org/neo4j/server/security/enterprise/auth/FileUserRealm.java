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
import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.ExpiredCredentialsException;
import org.apache.shiro.authc.SimpleAuthenticationInfo;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.apache.shiro.authc.credential.CredentialsMatcher;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.authz.Permission;
import org.apache.shiro.authz.SimpleAuthorizationInfo;
import org.apache.shiro.authz.SimpleRole;
import org.apache.shiro.authz.permission.RolePermissionResolver;
import org.apache.shiro.realm.AuthorizingRealm;
import org.apache.shiro.subject.PrincipalCollection;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.neo4j.kernel.api.security.exception.IllegalCredentialsException;
import org.neo4j.server.security.auth.Credential;
import org.neo4j.server.security.auth.User;
import org.neo4j.server.security.auth.UserRepository;
import org.neo4j.server.security.auth.exception.ConcurrentModificationException;

/**
 * Shiro realm wrapping FileUserRepository
 */
public class FileUserRealm extends AuthorizingRealm
{
    private final UserRepository userRepository;
    private final RoleRepository roleRepository;

    private final CredentialsMatcher credentialsMatcher =
            ( AuthenticationToken token, AuthenticationInfo info ) ->
            {
                UsernamePasswordToken usernamePasswordToken = (UsernamePasswordToken) token;
                String infoUserName = (String) info.getPrincipals().getPrimaryPrincipal();
                Credential infoCredential = (Credential) info.getCredentials();

                boolean userNameMatches = infoUserName.equals( usernamePasswordToken.getUsername() );
                boolean credentialsMatches =
                        infoCredential.matchesPassword( new String( usernamePasswordToken.getPassword() ) );

                return userNameMatches && credentialsMatches;
            };

    private final RolePermissionResolver rolePermissionResolver = new RolePermissionResolver()
    {
        @Override
        public Collection<Permission> resolvePermissionsInRole( String roleString )
        {
            SimpleRole role = roles.get( roleString );
            if ( role != null )
            {
                return role.getPermissions();
            }
            else
            {
                return Collections.EMPTY_LIST;
            }
        }
    };

    private final Map<String, SimpleRole> roles;

    public FileUserRealm( UserRepository userRepository, RoleRepository roleRepository )
    {
        super();

        this.userRepository = userRepository;
        this.roleRepository = roleRepository;
        setCredentialsMatcher( credentialsMatcher );
        setRolePermissionResolver( rolePermissionResolver );

        roles = new PredefinedRolesBuilder().buildRoles();
    }

    @Override
    protected AuthorizationInfo doGetAuthorizationInfo( PrincipalCollection principals )
    {
        User user = userRepository.findByName( (String) principals.getPrimaryPrincipal() );

        Set<String> roles = roleRepository.findByUsername( user.name() );
        return new SimpleAuthorizationInfo( roles );
    }

    @Override
    protected AuthenticationInfo doGetAuthenticationInfo( AuthenticationToken token ) throws AuthenticationException
    {
        UsernamePasswordToken usernamePasswordToken = (UsernamePasswordToken) token;

        User user = userRepository.findByName( usernamePasswordToken.getUsername() );

        if ( user == null )
        {
            throw new AuthenticationException( "User " + usernamePasswordToken.getUsername() + " does not exist" );
        }

        // TODO: This will not work if AuthenticationInfo is cached,
        // unless you always do SecurityManager.logout properly (which will invalidate the cache)
        // For REST we may need to connect HttpSessionListener.sessionDestroyed with logout
        if ( user.passwordChangeRequired() )
        {
            throw new ExpiredCredentialsException( "Password change required" );
        }

        return new SimpleAuthenticationInfo( user.name(), user.credentials(), getName() );
    }

    int numberOfUsers()
    {
        return userRepository.numberOfUsers();
    }

    User newUser( String username, String initialPassword, boolean requirePasswordChange )
            throws IOException, IllegalCredentialsException
    {
        assertValidUsername( username );

        User user = new User.Builder()
                .withName( username )
                .withCredentials( Credential.forPassword( initialPassword ) )
                .withRequiredPasswordChange( requirePasswordChange )
                .build();
        userRepository.create( user );

        return user;
    }

    RoleRecord newRole( String roleName, String... users ) throws IOException
    {
        assertValidRoleName( roleName );
        for (String username : users)
        {
            assertValidUsername( username );
        }

        SortedSet<String> userSet = new TreeSet<String>( Arrays.asList( users ) );

        RoleRecord role = new RoleRecord.Builder().withName( roleName ).withUsers( userSet ).build();
        roleRepository.create( role );

        return role;
    }

    void addUserToRole( String username, String roleName ) throws IOException
    {
        assertValidUsername( username );
        assertValidRoleName( roleName );

        synchronized ( this )
        {
            User user = userRepository.findByName( username );
            if ( user == null )
            {
                throw new IllegalArgumentException( "User " + username + " does not exist." );
            }

            RoleRecord role = roleRepository.findByName( roleName );
            if ( role == null )
            {
                throw new IllegalArgumentException( "Role " + roleName + " does not exist." );
            }
            else
            {
                RoleRecord newRole = role.augment().withUser( username ).build();
                try
                {
                    roleRepository.update( role, newRole );
                }
                catch ( ConcurrentModificationException e )
                {
                    // Try again
                    addUserToRole( username, roleName );
                }
            }
        }
    }

    void removeUserFromRole( String username, String rolename ) throws IOException
    {
        // TODO
    }

    boolean deleteUser( String username ) throws IOException
    {
        boolean result = false;
        synchronized ( this )
        {
            User user = userRepository.findByName( username );
            if ( user != null && userRepository.delete( user ) )
            {
                removeUserFromAllRoles( username );
                result = true;
            }
        }
        return result;
    }

    private void removeUserFromAllRoles( String username ) throws IOException
    {
        try
        {
            roleRepository.removeUserFromAllRoles( username );
        }
        catch ( ConcurrentModificationException e )
        {
            // Try again
            removeUserFromAllRoles( username );
        }
    }

    private void assertValidUsername( String name )
    {
        if ( !userRepository.isValidName( name ) )
        {
            throw new IllegalArgumentException(
                    "User name contains illegal characters. Please use simple ascii characters and numbers." );
        }
    }

    private void assertValidRoleName( String name )
    {
        if ( !roleRepository.isValidName( name ) )
        {
            throw new IllegalArgumentException(
                    "Role name contains illegal characters. Please use simple ascii characters and numbers." );
        }
    }
}
