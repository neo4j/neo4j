/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.server.security.enterprise.auth;

import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.ExcessiveAttemptsException;
import org.apache.shiro.authc.pam.ModularRealmAuthenticator;
import org.apache.shiro.authc.pam.UnsupportedTokenException;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.cache.Cache;
import org.apache.shiro.cache.CacheManager;
import org.apache.shiro.mgt.DefaultSecurityManager;
import org.apache.shiro.mgt.DefaultSessionStorageEvaluator;
import org.apache.shiro.mgt.DefaultSubjectDAO;
import org.apache.shiro.mgt.SubjectDAO;
import org.apache.shiro.realm.AuthenticatingRealm;
import org.apache.shiro.realm.AuthorizingRealm;
import org.apache.shiro.realm.CachingRealm;
import org.apache.shiro.realm.Realm;
import org.apache.shiro.subject.PrincipalCollection;
import org.apache.shiro.util.Initializable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.IntPredicate;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.graphdb.security.AuthProviderFailedException;
import org.neo4j.graphdb.security.AuthProviderTimeoutException;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext;
import org.neo4j.server.security.enterprise.log.SecurityLog;

import static org.neo4j.helpers.Strings.escape;
import static org.neo4j.kernel.api.security.AuthToken.invalidToken;

class MultiRealmAuthManager implements EnterpriseAuthAndUserManager
{
    private final EnterpriseUserManager userManager;
    private final Collection<Realm> realms;
    private final DefaultSecurityManager securityManager;
    private final CacheManager cacheManager;
    private final SecurityLog securityLog;
    private final boolean logSuccessfulLogin;
    private final boolean propertyAuthorization;
    private final Map<String,List<String>> roleToPropertyBlacklist;

    MultiRealmAuthManager( EnterpriseUserManager userManager, Collection<Realm> realms, CacheManager cacheManager,
            SecurityLog securityLog, boolean logSuccessfulLogin, boolean propertyAuthorization, Map<String,List<String>> roleToPropertyBlacklist )
    {
        this.userManager = userManager;
        this.realms = realms;
        this.cacheManager = cacheManager;

        securityManager = new DefaultSecurityManager( realms );
        this.securityLog = securityLog;
        this.logSuccessfulLogin = logSuccessfulLogin;
        this.propertyAuthorization = propertyAuthorization;
        this.roleToPropertyBlacklist = roleToPropertyBlacklist;
        securityManager.setSubjectFactory( new ShiroSubjectFactory() );
        ((ModularRealmAuthenticator) securityManager.getAuthenticator())
                .setAuthenticationStrategy( new ShiroAuthenticationStrategy() );

        securityManager.setSubjectDAO( createSubjectDAO() );
    }

    private SubjectDAO createSubjectDAO()
    {
        DefaultSubjectDAO subjectDAO = new DefaultSubjectDAO();
        DefaultSessionStorageEvaluator sessionStorageEvaluator = new DefaultSessionStorageEvaluator();
        sessionStorageEvaluator.setSessionStorageEnabled( false );
        subjectDAO.setSessionStorageEvaluator( sessionStorageEvaluator );
        return subjectDAO;
    }

    @Override
    public EnterpriseLoginContext login( Map<String,Object> authToken ) throws InvalidAuthTokenException
    {
        EnterpriseLoginContext securityContext;

        ShiroAuthToken token = new ShiroAuthToken( authToken );
        assertValidScheme( token );

        try
        {
            securityContext = new StandardEnterpriseLoginContext(
                    this, (ShiroSubject) securityManager.login( null, token ) );
            AuthenticationResult authenticationResult = securityContext.subject().getAuthenticationResult();
            if ( authenticationResult == AuthenticationResult.SUCCESS )
            {
                if ( logSuccessfulLogin )
                {
                    securityLog.info( securityContext.subject(), "logged in" );
                }
            }
            else if ( authenticationResult == AuthenticationResult.PASSWORD_CHANGE_REQUIRED )
            {
                securityLog.info( securityContext.subject(), "logged in (password change required)" );
            }
            else
            {
                String errorMessage = ((StandardEnterpriseLoginContext.NeoShiroSubject) securityContext.subject())
                        .getAuthenticationFailureMessage();
                securityLog.error( "[%s]: failed to log in: %s", escape( token.getPrincipal().toString() ), errorMessage );
            }
            // No need to keep full Shiro authentication info around on the subject
            ((StandardEnterpriseLoginContext.NeoShiroSubject) securityContext.subject()).clearAuthenticationInfo();
        }
        catch ( UnsupportedTokenException e )
        {
            securityLog.error( "Unknown user failed to log in: %s", e.getMessage() );
            Throwable cause = e.getCause();
            if ( cause != null && cause instanceof InvalidAuthTokenException )
            {
                throw new InvalidAuthTokenException( cause.getMessage() + ": " + token );
            }
            throw invalidToken( ": " + token );
        }
        catch ( ExcessiveAttemptsException e )
        {
            // NOTE: We only get this with single (internal) realm authentication
            securityContext = new StandardEnterpriseLoginContext( this,
                    new ShiroSubject( securityManager, AuthenticationResult.TOO_MANY_ATTEMPTS ) );
            securityLog.error( "[%s]: failed to log in: too many failed attempts",
                    escape( token.getPrincipal().toString() ) );
        }
        catch ( AuthenticationException e )
        {
            if ( e.getCause() != null && e.getCause() instanceof AuthProviderTimeoutException )
            {
                Throwable cause = e.getCause().getCause();
                securityLog.error( "[%s]: failed to log in: auth server timeout%s",
                        escape( token.getPrincipal().toString() ),
                        cause != null && cause.getMessage() != null ? " (" + cause.getMessage() + ")" : "" );
                throw new AuthProviderTimeoutException( e.getCause().getMessage(), e.getCause() );
            }
            else if ( e.getCause() != null && e.getCause() instanceof AuthProviderFailedException )
            {
                Throwable cause = e.getCause().getCause();
                securityLog.error( "[%s]: failed to log in: auth server connection refused%s",
                        escape( token.getPrincipal().toString() ),
                        cause != null && cause.getMessage() != null ? " (" + cause.getMessage() + ")" : "" );
                throw new AuthProviderFailedException( e.getCause().getMessage(), e.getCause() );
            }
            securityContext = new StandardEnterpriseLoginContext( this,
                    new ShiroSubject( securityManager, AuthenticationResult.FAILURE ) );
            Throwable cause = e.getCause();
            Throwable causeCause = e.getCause() != null ? e.getCause().getCause() : null;
            String errorMessage = String.format( "invalid principal or credentials%s%s",
                    cause != null && cause.getMessage() != null ? " (" + cause.getMessage() + ")" : "",
                    causeCause != null && causeCause.getMessage() != null ? " (" + causeCause.getMessage() + ")" : "" );
            securityLog.error( "[%s]: failed to log in: %s", escape( token.getPrincipal().toString() ), errorMessage );
        }

        return securityContext;
    }

    private void assertValidScheme( ShiroAuthToken token ) throws InvalidAuthTokenException
    {
        String scheme = token.getSchemeSilently();
        if ( scheme == null )
        {
            throw invalidToken( "missing key `scheme`: " + token );
        }
        else if ( scheme.equals( "none" ) )
        {
            throw invalidToken( "scheme='none' only allowed when auth is disabled: " + token );
        }
    }

    @Override
    public void init() throws Throwable
    {
        for ( Realm realm : realms )
        {
            if ( realm instanceof Initializable )
            {
                ((Initializable) realm).init();
            }
            if ( realm instanceof CachingRealm )
            {
                ((CachingRealm) realm).setCacheManager( cacheManager );
            }
            if ( realm instanceof RealmLifecycle )
            {
                ((RealmLifecycle) realm).initialize();
            }
        }
    }

    @Override
    public void start() throws Throwable
    {
        for ( Realm realm : realms )
        {
            if ( realm instanceof RealmLifecycle )
            {
                ((RealmLifecycle) realm).start();
            }
        }
    }

    @Override
    public void stop() throws Throwable
    {
        for ( Realm realm : realms )
        {
            if ( realm instanceof RealmLifecycle )
            {
                ((RealmLifecycle) realm).stop();
            }
        }
    }

    @Override
    public void shutdown() throws Throwable
    {
        for ( Realm realm : realms )
        {
            if ( realm instanceof CachingRealm )
            {
                ((CachingRealm) realm).setCacheManager( null );
            }
            if ( realm instanceof RealmLifecycle )
            {
                ((RealmLifecycle) realm).shutdown();
            }
        }
    }

    @Override
    public EnterpriseUserManager getUserManager( AuthSubject authSubject, boolean isUserManager )
    {
        return new PersonalUserManager( userManager, authSubject, securityLog, isUserManager );
    }

    @Override
    public EnterpriseUserManager getUserManager()
    {
        return userManager;
    }

    @Override
    public void clearAuthCache()
    {
        for ( Realm realm : realms )
        {
            if ( realm instanceof AuthenticatingRealm )
            {
                Cache<Object,AuthenticationInfo> cache = ((AuthenticatingRealm) realm).getAuthenticationCache();
                if ( cache != null )
                {
                    cache.clear();
                }
            }
            if ( realm instanceof AuthorizingRealm )
            {
                Cache<Object,AuthorizationInfo> cache = ((AuthorizingRealm) realm).getAuthorizationCache();
                if ( cache != null )
                {
                    cache.clear();
                }
            }
        }
    }

    public Collection<AuthorizationInfo> getAuthorizationInfo( PrincipalCollection principalCollection )
    {
        List<AuthorizationInfo> infoList = new ArrayList<>( 1 );
        for ( Realm realm : realms )
        {
            if ( realm instanceof ShiroAuthorizationInfoProvider )
            {
                AuthorizationInfo info = ((ShiroAuthorizationInfoProvider) realm)
                        .getAuthorizationInfoSnapshot( principalCollection );
                if ( info != null )
                {
                    infoList.add( info );
                }
            }
        }
        return infoList;
    }

    IntPredicate getPropertyPermissions( Set<String> roles, Function<String, Integer> tokenLookup )
    {
        if ( propertyAuthorization )
        {
            PrimitiveIntSet blackListed = Primitive.intSet();
            for ( String role : roles )
            {
                if ( roleToPropertyBlacklist.containsKey( role ) )
                {
                    assert roleToPropertyBlacklist.get( role ) != null : "Blacklist has to contain properties";
                    for ( String propName : roleToPropertyBlacklist.get( role ) )
                    {

                        try
                        {
                            blackListed.add( tokenLookup.apply( propName ) );
                        }
                        catch ( Exception e )
                        {
                            securityLog.error( "Error in setting up property permissions, '" + propName + "' is not a valid property name." );
                        }
                    }
                }
            }
            return property -> !blackListed.contains( property );
        }
        else
        {
            return property -> true;
        }
    }
}
