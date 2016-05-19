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
    private final GroupRepository groupRepository;

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

    public FileUserRealm( UserRepository userRepository, GroupRepository groupRepository )
    {
        super();

        this.userRepository = userRepository;
        this.groupRepository = groupRepository;
        setCredentialsMatcher( credentialsMatcher );
        setRolePermissionResolver( rolePermissionResolver );

        roles = new PredefinedRolesBuilder().buildRoles();
    }

    @Override
    protected AuthorizationInfo doGetAuthorizationInfo( PrincipalCollection principals )
    {
        User user = userRepository.findByName( (String) principals.getPrimaryPrincipal() );

        Set<String> roles = groupRepository.findByUsername( user.name() );
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

    User newUser( String username, String initialPassword, boolean requirePasswordChange ) throws
            IOException, IllegalCredentialsException, ConcurrentModificationException
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

    GroupRecord newGroup( String groupName, String... users ) throws
            IOException, IllegalCredentialsException, ConcurrentModificationException
    {
        assertValidName( groupName );

        SortedSet<String> userSet = new TreeSet<String>( Arrays.asList( users ) );
        GroupRecord group = new GroupRecord.Builder().withName( groupName ).withUsers( userSet ).build();
        groupRepository.create( group );

        return group;
    }

    private void addUserToGroup( User user, String groupName )
            throws IOException, IllegalCredentialsException, ConcurrentModificationException
    {
        GroupRecord group = groupRepository.findByName( groupName );
        if ( group == null )
        {
            GroupRecord newGroup = new GroupRecord( groupName, user.name() );
            groupRepository.create( newGroup );
        }
        else
        {
            GroupRecord newGroup = group.augment().withUser( user.name() ).build();
            groupRepository.update( group, newGroup );
        }
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
