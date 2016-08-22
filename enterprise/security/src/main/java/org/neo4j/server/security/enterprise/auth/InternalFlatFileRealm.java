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
import org.apache.shiro.authc.DisabledAccountException;
import org.apache.shiro.authc.ExcessiveAttemptsException;
import org.apache.shiro.authc.IncorrectCredentialsException;
import org.apache.shiro.authc.UnknownAccountException;
import org.apache.shiro.authc.credential.AllowAllCredentialsMatcher;
import org.apache.shiro.authc.pam.UnsupportedTokenException;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.authz.Permission;
import org.apache.shiro.authz.SimpleAuthorizationInfo;
import org.apache.shiro.authz.SimpleRole;
import org.apache.shiro.authz.permission.RolePermissionResolver;
import org.apache.shiro.realm.AuthorizingRealm;
import org.apache.shiro.subject.PrincipalCollection;
import org.apache.shiro.subject.SimplePrincipalCollection;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.kernel.api.security.AuthSubject;
import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.kernel.api.security.AuthenticationResult;
import org.neo4j.kernel.api.security.exception.InvalidArgumentsException;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.server.security.auth.AuthenticationStrategy;
import org.neo4j.server.security.auth.Credential;
import org.neo4j.server.security.auth.PasswordPolicy;
import org.neo4j.server.security.auth.User;
import org.neo4j.server.security.auth.UserRepository;
import org.neo4j.server.security.auth.exception.ConcurrentModificationException;

/**
 * Shiro realm wrapping FileUserRepository and FileRoleRepository
 */
public class InternalFlatFileRealm extends AuthorizingRealm implements RealmLifecycle, EnterpriseUserManager
{
    /**
     * This flag is used in the same way as User.PASSWORD_CHANGE_REQUIRED, but it's
     * placed here because of user suspension not being a part of community edition
     */
    public static final String IS_SUSPENDED = "is_suspended";

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
                return Collections.emptyList();
            }
        }
    };

    private final UserRepository userRepository;
    private final RoleRepository roleRepository;
    private final PasswordPolicy passwordPolicy;
    private final AuthenticationStrategy authenticationStrategy;
    private final boolean authenticationEnabled;
    private final boolean authorizationEnabled;
    private final Map<String,SimpleRole> roles;

    public InternalFlatFileRealm( UserRepository userRepository, RoleRepository roleRepository,
            PasswordPolicy passwordPolicy, AuthenticationStrategy authenticationStrategy,
            boolean authenticationEnabled, boolean authorizationEnabled )
    {
        super();

        this.userRepository = userRepository;
        this.roleRepository = roleRepository;
        this.passwordPolicy = passwordPolicy;
        this.authenticationStrategy = authenticationStrategy;
        this.authenticationEnabled = authenticationEnabled;
        this.authorizationEnabled = authorizationEnabled;
        setCredentialsMatcher( new AllowAllCredentialsMatcher() );
        setRolePermissionResolver( rolePermissionResolver );

        roles = new PredefinedRolesBuilder().buildRoles();
    }

    public InternalFlatFileRealm( UserRepository userRepository, RoleRepository roleRepository,
            PasswordPolicy passwordPolicy, AuthenticationStrategy authenticationStrategy )
    {
        this( userRepository, roleRepository, passwordPolicy, authenticationStrategy, true, true );
    }

    @Override
    public void initialize() throws Throwable
    {
        userRepository.init();
        roleRepository.init();
    }

    @Override
    public void start() throws Throwable
    {
        userRepository.start();
        roleRepository.start();

        ensureDefaultUsers();
        ensureDefaultRoles();
    }

    /* Adds neo4j user if no users exist */
    private void ensureDefaultUsers() throws IOException, InvalidArgumentsException
    {
        if ( authenticationEnabled || authorizationEnabled )
        {
            if ( numberOfUsers() == 0 )
            {
                newUser( "neo4j", "neo4j", true );
            }
        }
    }

    /* Builds all predefined roles if no roles exist. Adds 'neo4j' to admin role if no admin is assigned */
    private void ensureDefaultRoles() throws IOException, InvalidArgumentsException
    {
        if ( authenticationEnabled || authorizationEnabled )
        {
            if ( numberOfRoles() == 0 )
            {
                for ( String role : roles.keySet() )
                {
                    newRole( role );
                }
            }
            if ( this.getUsernamesForRole( PredefinedRolesBuilder.ADMIN ).size() == 0 )
            {
                if ( getAllUsernames().contains( "neo4j" ) )
                {
                    addRoleToUser( PredefinedRolesBuilder.ADMIN, "neo4j" );
                }
            }
        }
    }

    @Override
    public void stop() throws Throwable
    {
        userRepository.stop();
        roleRepository.stop();
    }

    @Override
    public void shutdown() throws Throwable
    {
        userRepository.shutdown();
        roleRepository.shutdown();
        setCacheManager( null );
    }

    @Override
    public boolean supports( AuthenticationToken token )
    {
        try
        {
            if ( token instanceof ShiroAuthToken )
            {
                return ((ShiroAuthToken) token).getScheme().equals( "basic" );
            }
            return false;
        }
        catch( InvalidAuthTokenException e )
        {
            return false;
        }
    }

    @Override
    protected AuthorizationInfo doGetAuthorizationInfo( PrincipalCollection principals ) throws AuthenticationException
    {
        if ( !authorizationEnabled )
        {
            return null;
        }

        String username = (String) getAvailablePrincipal( principals );
        if ( username == null )
        {
            return null;
        }

        User user = userRepository.getUserByName( username );
        if ( user == null )
        {
            return null;
        }

        if ( user.passwordChangeRequired() || user.hasFlag( IS_SUSPENDED ) )
        {
            return new SimpleAuthorizationInfo();
        }
        else
        {
            Set<String> roles = roleRepository.getRoleNamesByUsername( user.name() );
            return new SimpleAuthorizationInfo( roles );
        }
    }

    @Override
    protected AuthenticationInfo doGetAuthenticationInfo( AuthenticationToken token ) throws AuthenticationException
    {
        if ( !authenticationEnabled )
        {
            return null;
        }

        ShiroAuthToken shiroAuthToken = (ShiroAuthToken) token;

        String username;
        String password;
        try
        {
            username = AuthToken.safeCast( AuthToken.PRINCIPAL, shiroAuthToken.getAuthTokenMap() );
            password = AuthToken.safeCast( AuthToken.CREDENTIALS, shiroAuthToken.getAuthTokenMap() );
        }
        catch ( InvalidAuthTokenException e )
        {
            throw new UnsupportedTokenException( e );
        }

        User user = userRepository.getUserByName( username );
        if ( user == null )
        {
            throw new UnknownAccountException();
        }

        AuthenticationResult result = authenticationStrategy.authenticate( user, password );

        switch ( result )
        {
        case FAILURE:
            throw new IncorrectCredentialsException();
        case TOO_MANY_ATTEMPTS:
            throw new ExcessiveAttemptsException();
        default:
            break;
        }

        // TODO: This will not work if AuthenticationInfo is cached,
        // unless you always do SecurityManager.logout properly (which will invalidate the cache)
        // For REST we may need to connect HttpSessionListener.sessionDestroyed with logout
        if ( user.hasFlag( InternalFlatFileRealm.IS_SUSPENDED ) )
        {
            throw new DisabledAccountException( "User '" + user.name() + "' is suspended." );
        }

        if ( user.passwordChangeRequired() )
        {
            result = AuthenticationResult.PASSWORD_CHANGE_REQUIRED;
        }

        return new ShiroAuthenticationInfo( user.name(), user.credentials(), getName(), result );
    }

    int numberOfUsers()
    {
        return userRepository.numberOfUsers();
    }

    int numberOfRoles()
    {
        return roleRepository.numberOfRoles();
    }

    @Override
    public User newUser( String username, String initialPassword, boolean requirePasswordChange )
            throws IOException, InvalidArgumentsException
    {
        assertValidUsername( username );

        passwordPolicy.validatePassword( initialPassword );

        User user = new User.Builder()
                .withName( username )
                .withCredentials( Credential.forPassword( initialPassword ) )
                .withRequiredPasswordChange( requirePasswordChange )
                .build();
        userRepository.create( user );

        return user;
    }

    @Override
    public RoleRecord newRole( String roleName, String... usernames ) throws IOException, InvalidArgumentsException
    {
        assertValidRoleName( roleName );
        for ( String username : usernames )
        {
            assertValidUsername( username );
        }

        SortedSet<String> userSet = new TreeSet<>( Arrays.asList( usernames ) );

        RoleRecord role = new RoleRecord.Builder().withName( roleName ).withUsers( userSet ).build();
        roleRepository.create( role );

        return role;
    }

    @Override
    public RoleRecord getRole( String roleName ) throws InvalidArgumentsException
    {
        RoleRecord role = roleRepository.getRoleByName( roleName );
        if ( role == null )
        {
            throw new InvalidArgumentsException( "Role '" + roleName + "' does not exist." );
        }
        return role;
    }

    @Override
    public void addRoleToUser( String roleName, String username ) throws IOException, InvalidArgumentsException
    {
        assertValidRoleName( roleName );
        assertValidUsername( username );

        synchronized ( this )
        {
            getUser( username );
            RoleRecord role = getRole( roleName );
            RoleRecord newRole = role.augment().withUser( username ).build();
            try
            {
                roleRepository.update( role, newRole );
            }
            catch ( ConcurrentModificationException e )
            {
                // Try again
                addRoleToUser( roleName, username );
            }
        }
        clearCachedAuthorizationInfoForUser( username );
    }

    @Override
    public void removeRoleFromUser( String roleName, String username ) throws IOException, InvalidArgumentsException
    {
        assertValidRoleName( roleName );
        assertValidUsername( username );

        synchronized ( this )
        {
            getUser( username );
            RoleRecord role = getRole( roleName );

            RoleRecord newRole = role.augment().withoutUser( username ).build();
            try
            {
                roleRepository.update( role, newRole );
            }
            catch ( ConcurrentModificationException e )
            {
                // Try again
                removeRoleFromUser( roleName, username );
            }
        }
        clearCachedAuthorizationInfoForUser( username );
    }

    @Override
    public boolean deleteUser( String username ) throws IOException, InvalidArgumentsException
    {
        boolean result = false;
        synchronized ( this )
        {
            User user = getUser( username );
            if ( userRepository.delete( user ) )
            {
                removeUserFromAllRoles( username );
                result = true;
            }
            else
            {
                // We should not get here, but if we do the assert will fail and give a nice error msg
                getUser( username );
            }
        }
        clearCacheForUser( username );
        return result;
    }

    @Override
    public User getUser( String username ) throws InvalidArgumentsException
    {
        User u = userRepository.getUserByName( username );
        if ( u == null )
        {
            throw new InvalidArgumentsException( "User '" + username + "' does not exist." );
        }
        return u;
    }

    @Override
    public void setUserPassword( String username, String password ) throws IOException, InvalidArgumentsException
    {
        User existingUser = getUser( username );

        passwordPolicy.validatePassword( password );

        if ( existingUser.credentials().matchesPassword( password ) )
        {
            throw new InvalidArgumentsException( "Old password and new password cannot be the same." );
        }

        try
        {
            User updatedUser = existingUser.augment()
                    .withCredentials( Credential.forPassword( password ) )
                    .withRequiredPasswordChange( false )
                    .build();
            userRepository.update( existingUser, updatedUser );
        } catch ( ConcurrentModificationException e )
        {
            // try again
            setUserPassword( username, password );
        }

        clearCacheForUser( username );
    }

    @Override
    public void suspendUser( String username ) throws IOException, InvalidArgumentsException
    {
        // This method is not synchronized as it only modifies the UserRepository, which is synchronized in itself
        // If user is modified between getUserByName and update, we get ConcurrentModificationException and try again
        User user = getUser( username );
        if ( !user.hasFlag( IS_SUSPENDED ) )
        {
            User suspendedUser = user.augment().withFlag( IS_SUSPENDED ).build();
            try
            {
                userRepository.update( user, suspendedUser );
            }
            catch ( ConcurrentModificationException e )
            {
                // Try again
                suspendUser( username );
            }
        }
        clearCacheForUser( username );
    }

    @Override
    public void activateUser( String username ) throws IOException, InvalidArgumentsException
    {
        // This method is not synchronized as it only modifies the UserRepository, which is synchronized in itself
        // If user is modified between getUserByName and update, we get ConcurrentModificationException and try again
        User user = getUser( username );
        if ( user.hasFlag( IS_SUSPENDED ) )
        {
            User activatedUser = user.augment().withoutFlag( IS_SUSPENDED ).build();
            try
            {
                userRepository.update( user, activatedUser );
            }
            catch ( ConcurrentModificationException e )
            {
                // Try again
                activateUser( username );
            }
        }
        clearCacheForUser( username );
    }

    @Override
    public Set<String> getAllRoleNames()
    {
        return roleRepository.getAllRoleNames();
    }

    @Override
    public Set<String> getRoleNamesForUser( String username ) throws InvalidArgumentsException
    {
        getUser( username );
        return roleRepository.getRoleNamesByUsername( username );
    }

    @Override
    public Set<String> getUsernamesForRole( String roleName ) throws InvalidArgumentsException
    {
        RoleRecord role = getRole( roleName );
        return role.users();
    }

    @Override
    public Set<String> getAllUsernames()
    {
        return userRepository.getAllUsernames();
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

    private void assertValidUsername( String name ) throws InvalidArgumentsException
    {
        if ( name.isEmpty() )
        {
            throw new InvalidArgumentsException( "The provided user name is empty." );
        }
        if ( !userRepository.isValidUsername( name ) )
        {
            throw new InvalidArgumentsException(
                    "User name '" + name +
                    "' contains illegal characters. Use simple ascii characters and numbers." );
        }
    }

    private void assertValidRoleName( String name ) throws InvalidArgumentsException
    {
        if ( name.isEmpty() )
        {
            throw new InvalidArgumentsException( "The provided role name is empty." );
        }
        if ( !roleRepository.isValidRoleName( name ) )
        {
            throw new InvalidArgumentsException(
                    "Role name '" + name +
                    "' contains illegal characters. Use simple ascii characters and numbers." );
        }
    }

    private void clearCachedAuthorizationInfoForUser( String username )
    {
        clearCachedAuthorizationInfo( new SimplePrincipalCollection( username, this.getName() ) );
    }

    private void clearCacheForUser( String username )
    {
        clearCache( new SimplePrincipalCollection( username, this.getName() ) );
    }
}
