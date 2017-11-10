/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.neo4j.graphdb.security.AuthProviderFailedException;
import org.neo4j.graphdb.security.AuthProviderTimeoutException;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.kernel.enterprise.api.security.EnterpriseSecurityContext;
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

    MultiRealmAuthManager( EnterpriseUserManager userManager, Collection<Realm> realms, CacheManager cacheManager,
            SecurityLog securityLog, boolean logSuccessfulLogin )
    {
        this.userManager = userManager;
        this.realms = realms;
        this.cacheManager = cacheManager;

        securityManager = new DefaultSecurityManager( realms );
        this.securityLog = securityLog;
        this.logSuccessfulLogin = logSuccessfulLogin;
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
    public EnterpriseSecurityContext login( Map<String,Object> authToken ) throws InvalidAuthTokenException
    {
        EnterpriseSecurityContext securityContext;

        ShiroAuthToken token = new ShiroAuthToken( authToken );
        assertValidScheme( token );

        try
        {
            securityContext = new StandardEnterpriseSecurityContext(
                    this, (ShiroSubject) securityManager.login( null, token ) );
            AuthenticationResult authenticationResult = securityContext.subject().getAuthenticationResult();
            if ( authenticationResult == AuthenticationResult.SUCCESS )
            {
                if ( logSuccessfulLogin )
                {
                    securityLog.info( securityContext, "logged in" );
                }
            }
            else if ( authenticationResult == AuthenticationResult.PASSWORD_CHANGE_REQUIRED )
            {
                securityLog.info( securityContext, "logged in (password change required)" );
            }
            else
            {
                String errorMessage = ((StandardEnterpriseSecurityContext.NeoShiroSubject) securityContext.subject())
                        .getAuthenticationFailureMessage();
                securityLog.error( "[%s]: failed to log in: %s", escape( token.getPrincipal().toString() ), errorMessage );
            }
            // No need to keep full Shiro authentication info around on the subject
            ((StandardEnterpriseSecurityContext.NeoShiroSubject) securityContext.subject()).clearAuthenticationInfo();
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
            securityContext = new StandardEnterpriseSecurityContext( this,
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
            securityContext = new StandardEnterpriseSecurityContext( this,
                    new ShiroSubject( securityManager, AuthenticationResult.FAILURE ) );
            Throwable cause = e.getCause();
            Throwable causeCause = e.getCause() != null ? e.getCause().getCause() : null;
            securityLog.error( "[%s]: failed to log in: invalid principal or credentials%s%s",
                    escape( token.getPrincipal().toString() ),
                    cause != null && cause.getMessage() != null ? " (" + cause.getMessage() + ")" : "",
                    causeCause != null && causeCause.getMessage() != null ? " (" + causeCause.getMessage() + ")" : "" );
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
    public EnterpriseUserManager getUserManager( SecurityContext securityContext )
    {
        return new PersonalUserManager( userManager, securityContext, securityLog );
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
}
