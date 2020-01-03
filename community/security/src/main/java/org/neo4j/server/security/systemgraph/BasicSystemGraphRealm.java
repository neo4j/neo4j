/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.server.security.systemgraph;

import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.DisabledAccountException;
import org.apache.shiro.authc.ExcessiveAttemptsException;
import org.apache.shiro.authc.IncorrectCredentialsException;
import org.apache.shiro.authc.UnknownAccountException;
import org.apache.shiro.authc.credential.CredentialsMatcher;
import org.apache.shiro.authc.pam.UnsupportedTokenException;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.realm.AuthorizingRealm;
import org.apache.shiro.subject.PrincipalCollection;

import java.util.Map;
import java.util.regex.Pattern;

import org.neo4j.cypher.internal.security.FormatException;
import org.neo4j.cypher.internal.security.SecureHasher;
import org.neo4j.cypher.internal.security.SystemGraphCredential;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.security.AuthProviderFailedException;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.kernel.impl.security.Credential;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.server.security.auth.AuthenticationStrategy;
import org.neo4j.server.security.auth.BasicLoginContext;
import org.neo4j.server.security.auth.ShiroAuthToken;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.kernel.api.security.AuthToken.invalidToken;
import static org.neo4j.kernel.database.DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID;

/**
 * Shiro realm using a Neo4j graph to store users
 */
public class BasicSystemGraphRealm extends AuthorizingRealm implements AuthManager, CredentialsMatcher
{
    private final SecurityGraphInitializer systemGraphInitializer;
    private final DatabaseManager<?> databaseManager;
    private final SecureHasher secureHasher;
    private final AuthenticationStrategy authenticationStrategy;
    private final boolean authenticationEnabled;

    /**
     * This flag is used in the same way as User.PASSWORD_CHANGE_REQUIRED, but it's
     * placed here because of user suspension not being a part of community edition
     */
    public static final String IS_SUSPENDED = "is_suspended";

    public BasicSystemGraphRealm(
            SecurityGraphInitializer systemGraphInitializer,
            DatabaseManager<?> databaseManager,
            SecureHasher secureHasher,
            AuthenticationStrategy authenticationStrategy,
            boolean authenticationEnabled )
    {
        super();

        this.systemGraphInitializer = systemGraphInitializer;
        this.databaseManager = databaseManager;
        this.secureHasher = secureHasher;
        this.authenticationStrategy = authenticationStrategy;
        this.authenticationEnabled = authenticationEnabled;

        setAuthenticationCachingEnabled( true );
        setCredentialsMatcher( this );
    }

    @Override
    public void start() throws Exception
    {
        systemGraphInitializer.initializeSecurityGraph();
    }

    @Override
    public void stop()
    {
    }

    @Override
    public void shutdown()
    {
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
    public AuthenticationInfo doGetAuthenticationInfo( AuthenticationToken token ) throws AuthenticationException
    {
        if ( !authenticationEnabled )
        {
            return null;
        }

        ShiroAuthToken shiroAuthToken = (ShiroAuthToken) token;

        String username;
        try
        {
            username = AuthToken.safeCast( AuthToken.PRINCIPAL, shiroAuthToken.getAuthTokenMap() );
            // This is only checked here to check for InvalidAuthToken
            AuthToken.safeCastCredentials( AuthToken.CREDENTIALS, shiroAuthToken.getAuthTokenMap() );
        }
        catch ( InvalidAuthTokenException e )
        {
            throw new UnsupportedTokenException( e );
        }

        User user;
        try
        {
            user = getUser( username );
        }
        catch ( InvalidArgumentsException | FormatException e )
        {
            throw new UnknownAccountException();
        }

        // Stash the user record in the AuthenticationInfo that will be cached.
        // The credentials will then be checked when Shiro calls doCredentialsMatch()
        return new SystemGraphAuthenticationInfo( user, getName() /* Realm name */ );
    }

    @Override
    public boolean doCredentialsMatch( AuthenticationToken token, AuthenticationInfo info )
    {
        // We assume that the given info originated from this class, so we can get the user record from it
        SystemGraphAuthenticationInfo ourInfo = (SystemGraphAuthenticationInfo) info;
        User user = ourInfo.getUserRecord();

        // Get the password from the token
        byte[] password;
        try
        {
            ShiroAuthToken shiroAuthToken = (ShiroAuthToken) token;
            password = AuthToken.safeCastCredentials( AuthToken.CREDENTIALS, shiroAuthToken.getAuthTokenMap() );
        }
        catch ( InvalidAuthTokenException e )
        {
            throw new UnsupportedTokenException( e );
        }

        // Authenticate using our strategy (i.e. with rate limiting)
        AuthenticationResult result = authenticationStrategy.authenticate( user, password );

        // Map failures to exceptions
        switch ( result )
        {
        case SUCCESS:
            break;
        case PASSWORD_CHANGE_REQUIRED:
            break;
        case FAILURE:
            throw new IncorrectCredentialsException();
        case TOO_MANY_ATTEMPTS:
            throw new ExcessiveAttemptsException();
        default:
            throw new AuthenticationException();
        }

        // We also need to look at the user record flags
        if ( user.hasFlag( IS_SUSPENDED ) )
        {
            throw new DisabledAccountException( "User '" + user.name() + "' is suspended." );
        }

        if ( user.passwordChangeRequired() )
        {
            result = AuthenticationResult.PASSWORD_CHANGE_REQUIRED;
        }

        // Ok, if no exception was thrown by now it was a match.
        // Modify the given AuthenticationInfo with the final result and return with success.
        ourInfo.setAuthenticationResult( result );
        return true;
    }

    @Override
    protected Object getAuthenticationCacheKey( AuthenticationToken token )
    {
        Object principal = token != null ? token.getPrincipal() : null;
        return principal != null ? principal : "";
    }

    @Override
    protected Object getAuthenticationCacheKey( PrincipalCollection principals )
    {
        Object principal = getAvailablePrincipal( principals );
        return principal == null ? "" : principal;
    }

    @Override
    protected AuthorizationInfo doGetAuthorizationInfo( PrincipalCollection principals )
    {
        // Community should correspond to enterprise with authorization disabled
        return null;
    }

    public User getUser( String username ) throws InvalidArgumentsException, FormatException
    {
        InvalidArgumentsException userDontExists = new InvalidArgumentsException( "User '" + username + "' does not exist." );
        try ( Transaction tx = getSystemDb().beginTx() )
        {
            Node userNode = tx.findNode( Label.label( "User" ), "name", username );

            if ( userNode == null )
            {
                throw userDontExists;
            }

            Credential credential = SystemGraphCredential.deserialize( (String) userNode.getProperty( "credentials" ), secureHasher );
            boolean requirePasswordChange = (boolean) userNode.getProperty( "passwordChangeRequired" );
            boolean suspended = (boolean) userNode.getProperty( "suspended" );
            tx.commit();

            User.Builder builder = new User.Builder( username, credential ).withRequiredPasswordChange( requirePasswordChange );
            builder = suspended ? builder.withFlag( IS_SUSPENDED ) : builder.withoutFlag( IS_SUSPENDED );
            return builder.build();
        }
        catch ( NotFoundException n )
        {
            // Can occur if the user was dropped by another thread after the null check.
            throw userDontExists;
        }
    }

    // Allow all ascii from '!' to '~', apart from ',' and ':' which are used as separators in flat file
    private static final Pattern usernamePattern = Pattern.compile( "^[\\x21-\\x2B\\x2D-\\x39\\x3B-\\x7E]+$" );

    public static void assertValidUsername( String username ) throws InvalidArgumentsException
    {
        if ( username == null || username.isEmpty() )
        {
            throw new InvalidArgumentsException( "The provided username is empty." );
        }
        if ( !usernamePattern.matcher( username ).matches() )
        {
            throw new InvalidArgumentsException(
                    "Username '" + username + "' contains illegal characters. Use ascii characters that are not ',', ':' or whitespaces." );
        }
    }

    @Override
    public LoginContext login( Map<String,Object> authToken ) throws InvalidAuthTokenException
    {
        try
        {
            assertValidScheme( authToken );

            String username = AuthToken.safeCast( AuthToken.PRINCIPAL, authToken );
            byte[] password = AuthToken.safeCastCredentials( AuthToken.CREDENTIALS, authToken );

            try
            {
                User user = getUser( username );
                AuthenticationResult result = authenticationStrategy.authenticate( user, password );
                if ( result == AuthenticationResult.SUCCESS && user.passwordChangeRequired() )
                {
                    result = AuthenticationResult.PASSWORD_CHANGE_REQUIRED;
                }
                return new BasicLoginContext( user, result );
            }
            catch ( InvalidArgumentsException | FormatException e )
            {
                return new BasicLoginContext( null, AuthenticationResult.FAILURE );
            }
        }
        finally
        {
            AuthToken.clearCredentials( authToken );
        }
    }

    @Override
    public void log( String message, SecurityContext securityContext )
    {
    }

    private void assertValidScheme( Map<String,Object> token ) throws InvalidAuthTokenException
    {
        String scheme = AuthToken.safeCast( AuthToken.SCHEME_KEY, token );
        if ( scheme.equals( "none" ) )
        {
            throw invalidToken( ", scheme 'none' is only allowed when auth is disabled." );
        }
        if ( !scheme.equals( AuthToken.BASIC_SCHEME ) )
        {
            throw invalidToken( ", scheme '" + scheme + "' is not supported." );
        }
    }

    protected GraphDatabaseService getSystemDb()
    {
        return databaseManager.getDatabaseContext( NAMED_SYSTEM_DATABASE_ID ).orElseThrow(
                () -> new AuthProviderFailedException( "No database called `" + SYSTEM_DATABASE_NAME + "` was found." ) ).databaseFacade();
    }
}
