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
import org.apache.shiro.authc.ExcessiveAttemptsException;
import org.apache.shiro.authc.pam.ModularRealmAuthenticator;
import org.apache.shiro.authc.pam.UnsupportedTokenException;
import org.apache.shiro.cache.CacheManager;
import org.apache.shiro.mgt.DefaultSecurityManager;
import org.apache.shiro.mgt.DefaultSessionStorageEvaluator;
import org.apache.shiro.mgt.DefaultSubjectDAO;
import org.apache.shiro.mgt.SubjectDAO;
import org.apache.shiro.realm.CachingRealm;
import org.apache.shiro.realm.Realm;
import org.apache.shiro.util.Initializable;

import java.util.Collection;
import java.util.Map;

import org.neo4j.kernel.api.security.AuthenticationResult;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.kernel.enterprise.api.security.EnterpriseAuthSubject;
import org.neo4j.kernel.impl.enterprise.SecurityLog;
import org.neo4j.server.security.auth.UserManagerSupplier;

class MultiRealmAuthManager implements EnterpriseAuthManager, UserManagerSupplier
{
    private final EnterpriseUserManager userManager;
    private final Collection<Realm> realms;
    private final DefaultSecurityManager securityManager;
    private final CacheManager cacheManager;
    private final SecurityLog securityLog;

    MultiRealmAuthManager( EnterpriseUserManager userManager, Collection<Realm> realms, CacheManager cacheManager,
            SecurityLog securityLog )
    {
        this.userManager = userManager;
        this.realms = realms;
        this.cacheManager = cacheManager;

        securityManager = new DefaultSecurityManager( realms );
        this.securityLog = securityLog;
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
    public boolean supports( final Map<String,Object> authToken )
    {
        final ShiroAuthToken token = new ShiroAuthToken( authToken );
        return realms.stream().anyMatch( realm -> realm.supports( token ) );
    }

    @Override
    public EnterpriseAuthSubject login( Map<String,Object> authToken ) throws InvalidAuthTokenException
    {
        EnterpriseAuthSubject subject;

        ShiroAuthToken token = new ShiroAuthToken( authToken );

        try
        {
            subject = new StandardEnterpriseAuthSubject( this, (ShiroSubject) securityManager.login( null, token ) );
            securityLog.info( subject, "logged in" );
        }
        catch ( UnsupportedTokenException e )
        {
            throw new InvalidAuthTokenException( e.getCause().getMessage() );
        }
        catch ( ExcessiveAttemptsException e )
        {
            // NOTE: We only get this with single (internal) realm authentication
            subject = new StandardEnterpriseAuthSubject( this,
                    new ShiroSubject( securityManager, AuthenticationResult.TOO_MANY_ATTEMPTS ) );
            securityLog.info( subject, "Login fail for user `%s` - too many failed attempts." );
        }
        catch ( AuthenticationException e )
        {
            subject = new StandardEnterpriseAuthSubject( this,
                    new ShiroSubject( securityManager, AuthenticationResult.FAILURE ) );
            securityLog.info( subject, "Login fail for user `%s`" );
        }

        return subject;
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
    public EnterpriseUserManager getUserManager()
    {
        return userManager;
    }
}
