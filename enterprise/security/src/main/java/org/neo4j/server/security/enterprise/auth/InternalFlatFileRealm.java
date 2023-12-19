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
import org.apache.shiro.authz.SimpleAuthorizationInfo;
import org.apache.shiro.realm.AuthorizingRealm;
import org.apache.shiro.subject.PrincipalCollection;
import org.apache.shiro.subject.SimplePrincipalCollection;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.neo4j.commandline.admin.security.SetDefaultAdminCommand;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.kernel.api.security.PasswordPolicy;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.kernel.impl.security.Credential;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.server.security.auth.AuthenticationStrategy;
import org.neo4j.server.security.auth.ListSnapshot;
import org.neo4j.server.security.auth.UserRepository;
import org.neo4j.server.security.auth.exception.ConcurrentModificationException;
import org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import org.neo4j.server.security.enterprise.configuration.SecuritySettings;

import static java.lang.String.format;
import static java.util.Collections.emptySet;

/**
 * Shiro realm wrapping FileUserRepository and FileRoleRepository
 */
public class InternalFlatFileRealm extends AuthorizingRealm implements RealmLifecycle, EnterpriseUserManager,
        ShiroAuthorizationInfoProvider
{
    /**
     * This flag is used in the same way as User.PASSWORD_CHANGE_REQUIRED, but it's
     * placed here because of user suspension not being a part of community edition
     */
    static final String IS_SUSPENDED = "is_suspended";

    private static int MAX_READ_ATTEMPTS = 10;

    private final UserRepository userRepository;
    private final RoleRepository roleRepository;
    private final UserRepository initialUserRepository;
    private final UserRepository defaultAdminRepository;
    private final PasswordPolicy passwordPolicy;
    private final AuthenticationStrategy authenticationStrategy;
    private final boolean authenticationEnabled;
    private final boolean authorizationEnabled;
    private final JobScheduler jobScheduler;
    private volatile JobScheduler.JobHandle reloadJobHandle;

    public InternalFlatFileRealm( UserRepository userRepository, RoleRepository roleRepository,
            PasswordPolicy passwordPolicy, AuthenticationStrategy authenticationStrategy,
            JobScheduler jobScheduler, UserRepository initialUserRepository,
            UserRepository defaultAdminRepository )
    {
        this( userRepository,roleRepository, passwordPolicy, authenticationStrategy, true, true,
                jobScheduler, initialUserRepository, defaultAdminRepository );
    }

    InternalFlatFileRealm( UserRepository userRepository, RoleRepository roleRepository,
            PasswordPolicy passwordPolicy, AuthenticationStrategy authenticationStrategy,
            boolean authenticationEnabled, boolean authorizationEnabled, JobScheduler jobScheduler,
            UserRepository initialUserRepository, UserRepository defaultAdminRepository )
    {
        super();

        setName( SecuritySettings.NATIVE_REALM_NAME );
        this.userRepository = userRepository;
        this.roleRepository = roleRepository;
        this.initialUserRepository = initialUserRepository;
        this.defaultAdminRepository = defaultAdminRepository;
        this.passwordPolicy = passwordPolicy;
        this.authenticationStrategy = authenticationStrategy;
        this.authenticationEnabled = authenticationEnabled;
        this.authorizationEnabled = authorizationEnabled;
        this.jobScheduler = jobScheduler;
        setAuthenticationCachingEnabled( false ); // NOTE: If this is ever changed to true it is not secure to use
                                                  // AllowAllCredentialsMatcher anymore
        setAuthorizationCachingEnabled( false );
        setCredentialsMatcher( new AllowAllCredentialsMatcher() ); // Since we do not cache authentication info we can
                                                                   // disable the credentials matcher
        setRolePermissionResolver( PredefinedRolesBuilder.rolePermissionResolver );
    }

    @Override
    public void initialize() throws Throwable
    {
        initialUserRepository.init();
        defaultAdminRepository.init();
        userRepository.init();
        roleRepository.init();
    }

    @Override
    public void start() throws Throwable
    {
        initialUserRepository.start();
        defaultAdminRepository.start();
        userRepository.start();
        roleRepository.start();

        Set<String> addedDefaultUsers = ensureDefaultUsers();
        ensureDefaultRoles( addedDefaultUsers );

        scheduleNextFileReload();
    }

    protected void scheduleNextFileReload()
    {
        reloadJobHandle = jobScheduler.schedule(
                JobScheduler.Groups.nativeSecurity,
                this::readFilesFromDisk,
                10, TimeUnit.SECONDS );
    }

    private void readFilesFromDisk()
    {
        try
        {
            readFilesFromDisk( MAX_READ_ATTEMPTS, new LinkedList<>() );
        }
        finally
        {
            scheduleNextFileReload();
        }
    }

    private void readFilesFromDisk( int attemptLeft, java.util.List<String> failures )
    {
        if ( attemptLeft < 0 )
        {
            throw new RuntimeException( "Unable to load valid flat file repositories! Attempts failed with:\n\t" +
                    String.join( "\n\t", failures ) );
        }

        try
        {
            final boolean valid;
            final boolean needsUpdate;
            synchronized ( this )
            {
                ListSnapshot<User> users = userRepository.getPersistedSnapshot();
                ListSnapshot<RoleRecord> roles = roleRepository.getPersistedSnapshot();

                needsUpdate = users.fromPersisted() || roles.fromPersisted();
                valid = needsUpdate && RoleRepository.validate( users.values(), roles.values() );

                if ( valid )
                {
                    if ( users.fromPersisted() )
                    {
                        userRepository.setUsers( users );
                    }
                    if ( roles.fromPersisted() )
                    {
                        roleRepository.setRoles( roles );
                    }
                }
            }
            if ( needsUpdate && !valid )
            {
                failures.add( "Role-auth file combination not valid." );
                Thread.sleep( 10 );
                readFilesFromDisk( attemptLeft - 1, failures );
            }
        }
        catch ( IOException | IllegalStateException | InterruptedException | InvalidArgumentsException e )
        {
            failures.add( e.getMessage() );
            readFilesFromDisk( attemptLeft - 1, failures );
        }
    }

    /* Adds neo4j user if no users exist */
    private Set<String> ensureDefaultUsers() throws Throwable
    {
        if ( authenticationEnabled || authorizationEnabled )
        {
            if ( userRepository.numberOfUsers() == 0 )
            {
                User neo4j = newUser( INITIAL_USER_NAME, "neo4j", true );
                if ( initialUserRepository.numberOfUsers() > 0 )
                {
                    User initUser = initialUserRepository.getUserByName( INITIAL_USER_NAME );
                    if ( initUser != null )
                    {
                        userRepository.update( neo4j, initUser );
                    }
                }
                return Collections.singleton( INITIAL_USER_NAME );
            }
        }
        return Collections.emptySet();
    }

    /* Builds all predefined roles if no roles exist. Adds 'neo4j' to admin role if no admin is assigned */
    private void ensureDefaultRoles( Set<String> addedDefaultUsers ) throws IOException, InvalidArgumentsException
    {
        if ( authenticationEnabled || authorizationEnabled )
        {
            List<String> newAdmins = new LinkedList<>( addedDefaultUsers );

            if ( numberOfRoles() == 0 )
            {
                if ( newAdmins.isEmpty() )
                {
                    Set<String> usernames = userRepository.getAllUsernames();
                    if ( defaultAdminRepository.numberOfUsers() > 1 )
                    {
                        throw new InvalidArgumentsException(
                                "No roles defined, and multiple users defined as default admin user." +
                                        " Please use `neo4j-admin " + SetDefaultAdminCommand.COMMAND_NAME +
                                        "` to select a valid admin." );
                    }
                    else if ( defaultAdminRepository.numberOfUsers() == 1 )
                    {
                        // We currently support only one default admin
                        String newAdminUsername = defaultAdminRepository.getAllUsernames().iterator().next();
                        if ( userRepository.getUserByName( newAdminUsername ) == null )
                        {
                            throw new InvalidArgumentsException(
                                    "No roles defined, and default admin user '" + newAdminUsername +
                                            "' does not exist. Please use `neo4j-admin " +
                                            SetDefaultAdminCommand.COMMAND_NAME + "` to select a valid admin." );
                        }
                        newAdmins.add( newAdminUsername );
                    }
                    else if ( usernames.size() == 1 )
                    {
                        newAdmins.add( usernames.iterator().next() );
                    }
                    else if ( usernames.contains( INITIAL_USER_NAME ) )
                    {
                        newAdmins.add( INITIAL_USER_NAME );
                    }
                    else
                    {
                        throw new InvalidArgumentsException(
                                "No roles defined, and cannot determine which user should be admin. " +
                                        "Please use `neo4j-admin " + SetDefaultAdminCommand.COMMAND_NAME +
                                        "` to select an " + "admin." );
                    }
                }

                for ( String role : PredefinedRolesBuilder.roles.keySet() )
                {
                    newRole( role );
                }
            }

            for ( String username : newAdmins )
            {
                addRoleToUser( PredefinedRoles.ADMIN, username );
            }
        }
    }

    @Override
    public void stop() throws Throwable
    {
        initialUserRepository.stop();
        defaultAdminRepository.stop();
        userRepository.stop();
        roleRepository.stop();

        if ( reloadJobHandle != null )
        {
            reloadJobHandle.cancel( true );
            reloadJobHandle = null;
        }
    }

    @Override
    public void shutdown() throws Throwable
    {
        initialUserRepository.shutdown();
        defaultAdminRepository.shutdown();
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
                ShiroAuthToken shiroAuthToken = (ShiroAuthToken) token;
                return shiroAuthToken.getScheme().equals( AuthToken.BASIC_SCHEME ) &&
                       (shiroAuthToken.supportsRealm( AuthToken.NATIVE_REALM ));
            }
            return false;
        }
        catch ( InvalidAuthTokenException e )
        {
            return false;
        }
    }

    @Override
    protected AuthorizationInfo doGetAuthorizationInfo( PrincipalCollection principals )
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

        if ( user.hasFlag( InternalFlatFileRealm.IS_SUSPENDED ) )
        {
            throw new DisabledAccountException( "User '" + user.name() + "' is suspended." );
        }

        if ( user.passwordChangeRequired() )
        {
            result = AuthenticationResult.PASSWORD_CHANGE_REQUIRED;
        }

        // NOTE: We do not cache the authentication info using the Shiro cache manager,
        // so all authentication request will go through this method.
        // Hence the credentials matcher is set to AllowAllCredentialsMatcher,
        // and we do not need to store hashed credentials in the AuthenticationInfo.
        return new ShiroAuthenticationInfo( user.name(), getName(), result );
    }

    @Override
    public AuthorizationInfo getAuthorizationInfoSnapshot( PrincipalCollection principalCollection )
    {
        return getAuthorizationInfo( principalCollection );
    }

    private int numberOfUsers()
    {
        return userRepository.numberOfUsers();
    }

    private int numberOfRoles()
    {
        return roleRepository.numberOfRoles();
    }

    @Override
    public User newUser( String username, String initialPassword, boolean requirePasswordChange )
            throws IOException, InvalidArgumentsException
    {
        userRepository.assertValidUsername( username );
        passwordPolicy.validatePassword( initialPassword );

        User user = new User.Builder()
                .withName( username )
                .withCredentials( Credential.forPassword( initialPassword ) )
                .withRequiredPasswordChange( requirePasswordChange )
                .build();
        synchronized ( this )
        {
            userRepository.create( user );
        }

        return user;
    }

    @Override
    public RoleRecord newRole( String roleName, String... usernames ) throws IOException, InvalidArgumentsException
    {
        roleRepository.assertValidRoleName( roleName );
        for ( String username : usernames )
        {
            userRepository.assertValidUsername( username );
        }

        SortedSet<String> userSet = new TreeSet<>( Arrays.asList( usernames ) );
        RoleRecord role = new RoleRecord.Builder().withName( roleName ).withUsers( userSet ).build();

        synchronized ( this )
        {
            for ( String username : usernames )
            {
                getUser( username ); // assert that user exists
            }
            roleRepository.create( role );
        }

        return role;
    }

    @Override
    public boolean deleteRole( String roleName ) throws IOException, InvalidArgumentsException
    {
        assertNotPredefinedRoleName( roleName );

        boolean result = false;
        synchronized ( this )
        {
            RoleRecord role = getRole( roleName );  // asserts role name exists
            if ( roleRepository.delete( role ) )
            {
                result = true;
            }
            else
            {
                // We should not get here, but if we do the assert will fail and give a nice error msg
                getRole( roleName );
            }
        }
        return result;
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
    public RoleRecord silentlyGetRole( String roleName )
    {
        return roleRepository.getRoleByName( roleName );
    }

    @Override
    public void addRoleToUser( String roleName, String username ) throws IOException, InvalidArgumentsException
    {
        roleRepository.assertValidRoleName( roleName );
        userRepository.assertValidUsername( username );

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
        roleRepository.assertValidRoleName( roleName );
        userRepository.assertValidUsername( username );

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
        synchronized ( this )
        {
            User user = getUser( username );    // throws if user does not exists
            removeUserFromAllRoles( username ); // performed first to always maintain auth-roles repo consistency
            userRepository.delete( user );      // this will not fail as we know the user exists in this lock
                                                // assuming no one messes with the user and role repositories
                                                // outside this instance
        }
        clearCacheForUser( username );
        return true;
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
    public User silentlyGetUser( String username )
    {
        return userRepository.getUserByName( username );
    }

    @Override
    public void setUserPassword( String username, String password, boolean requirePasswordChange )
            throws IOException, InvalidArgumentsException
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
                    .withRequiredPasswordChange( requirePasswordChange )
                    .build();
            synchronized ( this )
            {
                userRepository.update( existingUser, updatedUser );
            }
        }
        catch ( ConcurrentModificationException e )
        {
            // try again
            setUserPassword( username, password, requirePasswordChange );
        }

        clearCacheForUser( username );
    }

    @Override
    public void suspendUser( String username ) throws IOException, InvalidArgumentsException
    {
        User user = getUser( username );
        if ( !user.hasFlag( IS_SUSPENDED ) )
        {
            User suspendedUser = user.augment().withFlag( IS_SUSPENDED ).build();
            try
            {
                synchronized ( this )
                {
                    userRepository.update( user, suspendedUser );
                }
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
    public void activateUser( String username, boolean requirePasswordChange )
            throws IOException, InvalidArgumentsException
    {
        User user = getUser( username );
        if ( user.hasFlag( IS_SUSPENDED ) )
        {
            User activatedUser = user.augment()
                    .withoutFlag( IS_SUSPENDED )
                    .withRequiredPasswordChange( requirePasswordChange )
                    .build();
            try
            {
                synchronized ( this )
                {
                    userRepository.update( user, activatedUser );
                }
            }
            catch ( ConcurrentModificationException e )
            {
                // Try again
                activateUser( username, requirePasswordChange );
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
    public Set<String> silentlyGetRoleNamesForUser( String username )
    {
        return roleRepository.getRoleNamesByUsername( username );
    }

    @Override
    public Set<String> getUsernamesForRole( String roleName ) throws InvalidArgumentsException
    {
        RoleRecord role = getRole( roleName );
        return role.users();
    }

    @Override
    public Set<String> silentlyGetUsernamesForRole( String roleName )
    {
        RoleRecord role = silentlyGetRole( roleName );
        return role == null ? emptySet() : role.users();
    }

    @Override
    public Set<String> getAllUsernames()
    {
        return userRepository.getAllUsernames();
    }

    // this is only used from already synchronized code blocks
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

    private void assertNotPredefinedRoleName( String roleName ) throws InvalidArgumentsException
    {
        if ( roleName != null && PredefinedRolesBuilder.roles.keySet().contains( roleName ) )
        {
            throw new InvalidArgumentsException(
                    format( "'%s' is a predefined role and can not be deleted.", roleName ) );
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
