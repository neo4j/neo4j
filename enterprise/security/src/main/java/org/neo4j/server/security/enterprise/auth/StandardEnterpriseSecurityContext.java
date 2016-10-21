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

import org.apache.shiro.authz.AuthorizationInfo;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.security.AccessMode;
import org.neo4j.kernel.api.security.AuthSubject;
import org.neo4j.kernel.api.security.AuthenticationResult;
import org.neo4j.kernel.enterprise.api.security.EnterpriseSecurityContext;

class StandardEnterpriseSecurityContext implements EnterpriseSecurityContext
{
    private static final String SCHEMA_READ_WRITE = "schema:read,write";
    private static final String READ_WRITE = "data:read,write";
    private static final String READ = "data:read";

    private final MultiRealmAuthManager authManager;
    private final ShiroSubject shiroSubject;
    private final NeoShiroSubject neoShiroSubject;

    StandardEnterpriseSecurityContext( MultiRealmAuthManager authManager, ShiroSubject shiroSubject )
    {
        this.authManager = authManager;
        this.shiroSubject = shiroSubject;
        this.neoShiroSubject = new NeoShiroSubject();
    }

    public EnterpriseUserManager getUserManager()
    {
        return authManager.getUserManager( this );
    }

    @Override
    public boolean isAdmin()
    {
        return shiroSubject.isAuthenticated() && shiroSubject.isPermitted( "*" );
    }

    @Override
    public AuthSubject subject()
    {
        return neoShiroSubject;
    }

    @Override
    public AccessMode mode()
    {
        boolean isAuthenticated = shiroSubject.isAuthenticated();
        return new StandardAccessMode(
                isAuthenticated && shiroSubject.isPermitted( READ ),
                isAuthenticated && shiroSubject.isPermitted( READ_WRITE ),
                isAuthenticated && shiroSubject.isPermitted( SCHEMA_READ_WRITE ),
                shiroSubject.getAuthenticationResult() == AuthenticationResult.PASSWORD_CHANGE_REQUIRED,
                authManager.getAuthorizationInfo( shiroSubject.getPrincipals() )
            );
    }

    @Override
    public String toString()
    {
        return defaultString( "enterprise-security-context" );
    }

    @Override
    public EnterpriseSecurityContext freeze()
    {
        return new Frozen( neoShiroSubject, mode(), isAdmin() );
    }

    @Override
    public EnterpriseSecurityContext withMode( AccessMode mode )
    {
        return new Frozen( neoShiroSubject, mode, isAdmin() );
    }

    private static class StandardAccessMode implements AccessMode
    {
        private final boolean allowsReads;
        private final boolean allowsWrites;
        private final boolean allowsSchemaWrites;
        private final boolean passwordChangeRequired;
        private Collection<AuthorizationInfo> authorizationInfoSnapshot;

        StandardAccessMode( boolean allowsReads, boolean allowsWrites, boolean allowsSchemaWrites,
                boolean passwordChangeRequired, Collection<AuthorizationInfo> authorizationInfo )
        {
            this.allowsReads = allowsReads;
            this.allowsWrites = allowsWrites;
            this.allowsSchemaWrites = allowsSchemaWrites;
            this.passwordChangeRequired = passwordChangeRequired;
            authorizationInfoSnapshot = authorizationInfo;
        }

        @Override
        public boolean allowsReads()
        {
            return allowsReads;
        }

        @Override
        public boolean allowsWrites()
        {
            return allowsWrites;
        }

        @Override
        public boolean allowsSchemaWrites()
        {
            return allowsSchemaWrites;
        }

        @Override
        public boolean allowsProcedureWith( String[] roleNames ) throws InvalidArgumentsException
        {
            Set<String> roles = roleNames();
            for ( int i = 0; i < roleNames.length; i++ )
            {
                if ( roles.contains( roleNames[i] ) )
                {
                    return true;
                }
            }
            return false;
        }

        @Override
        public AuthorizationViolationException onViolation( String msg )
        {
            if ( passwordChangeRequired )
            {
                return AccessMode.Static.CREDENTIALS_EXPIRED.onViolation( msg );
            }
            else
            {
                return new AuthorizationViolationException( msg );
            }
        }

        @Override
        public String name()
        {
            Set<String> roles = new TreeSet<>( roleNames() );
            return roles.isEmpty() ? "no roles" : "roles [" + String.join( ",", roles ) + "]";
        }

        private Set<String> roleNames()
        {
            return authorizationInfoSnapshot.stream()
                    .flatMap( authInfo -> {
                        Collection<String> roles = authInfo.getRoles();
                        return roles == null ? Stream.empty() : roles.stream();
                    } )
                    .collect( Collectors.toSet() );
        }
    }

    private class NeoShiroSubject implements AuthSubject
    {

        @Override
        public String username()
        {
            Object principal = shiroSubject.getPrincipal();
            if ( principal != null )
            {
                return principal.toString();
            }
            else
            {
                return ""; // Should never clash with a valid username
            }
        }

        @Override
        public void logout()
        {
            shiroSubject.logout();
        }

        @Override
        public AuthenticationResult getAuthenticationResult()
        {
            return shiroSubject.getAuthenticationResult();
        }

        @Override
        public void setPassword( String password, boolean requirePasswordChange )
                throws IOException, InvalidArgumentsException
        {
            getUserManager().setUserPassword( (String) shiroSubject.getPrincipal(), password, requirePasswordChange );
            // Make user authenticated if successful
            setPasswordChangeNoLongerRequired();
        }

        @Override
        public void setPasswordChangeNoLongerRequired()
        {
            if ( getAuthenticationResult() == AuthenticationResult.PASSWORD_CHANGE_REQUIRED )
            {
                shiroSubject.setAuthenticationResult( AuthenticationResult.SUCCESS );
            }
        }

        @Override
        public boolean hasUsername( String username )
        {
            Object principal = shiroSubject.getPrincipal();
            return principal != null && username != null && username.equals( principal );
        }
    }
}
