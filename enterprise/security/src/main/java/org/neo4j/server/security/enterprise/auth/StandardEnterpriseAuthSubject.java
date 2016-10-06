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

import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.security.Allowance;
import org.neo4j.kernel.api.security.AuthSubject;
import org.neo4j.kernel.api.security.AuthenticationResult;
import org.neo4j.kernel.enterprise.api.security.EnterpriseAuthSubject;

class StandardEnterpriseAuthSubject implements EnterpriseAuthSubject
{
    private static final String SCHEMA_READ_WRITE = "schema:read,write";
    private static final String READ_WRITE = "data:read,write";
    private static final String READ = "data:read";

    private final MultiRealmAuthManager authManager;
    private final ShiroSubject shiroSubject;
    private Collection<AuthorizationInfo> authorizationInfoSnapshot;

    static StandardEnterpriseAuthSubject castOrFail( AuthSubject authSubject )
    {
        return EnterpriseAuthSubject.castOrFail( StandardEnterpriseAuthSubject.class, authSubject );
    }

    StandardEnterpriseAuthSubject( MultiRealmAuthManager authManager, ShiroSubject shiroSubject )
    {
        this.authManager = authManager;
        this.shiroSubject = shiroSubject;
    }

    @Override
    public void ensureUserExistsWithName( String username ) throws InvalidArgumentsException
    {
        getUserManager().getUser( username );
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
        getUserManager().setUserPassword( shiroSubject.getPrincipal().toString(), password, requirePasswordChange );
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
    public boolean allowsProcedureWith( String[] roleNames ) throws InvalidArgumentsException
    {
        for ( AuthorizationInfo info : authorizationInfoSnapshot )
        {
            Collection<String> roles = info.getRoles();
            if ( roles != null )
            {
                for ( int i = 0; i < roleNames.length; i++ )
                {
                    if ( roles.contains( roleNames[i] ) )
                    {
                        return true;
                    }
                }
            }
        }
        return false;
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
    public boolean hasUsername( String username )
    {
        Object principal = shiroSubject.getPrincipal();
        return principal != null && username != null && username.equals( principal );
    }

    @Override
    public Allowance allows()
    {
        return new StandardAllowance(
                shiroSubject.isAuthenticated() && shiroSubject.isPermitted( READ ),
                shiroSubject.isAuthenticated() && shiroSubject.isPermitted( READ_WRITE ),
                shiroSubject.isAuthenticated() && shiroSubject.isPermitted( SCHEMA_READ_WRITE ),
                shiroSubject.getAuthenticationResult() == AuthenticationResult.PASSWORD_CHANGE_REQUIRED
            );
    }

    @Override
    public String name()
    {
        String username = username();
        if ( username.isEmpty() )
        {
            return "<missing_principal>";
        }
        return username;
    }

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

    private static class StandardAllowance implements Allowance
    {
        private final boolean allowsReads;
        private final boolean allowsWrites;
        private final boolean allowsSchemaWrites;
        private final boolean passwordChangeRequired;

        StandardAllowance( boolean allowsReads, boolean allowsWrites, boolean allowsSchemaWrites, boolean
                passwordChangeRequired )
        {
            this.allowsReads = allowsReads;
            this.allowsWrites = allowsWrites;
            this.allowsSchemaWrites = allowsSchemaWrites;
            this.passwordChangeRequired = passwordChangeRequired;
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
        public AuthorizationViolationException onViolation( String msg )
        {
            if ( passwordChangeRequired )
            {
                return Allowance.Static.CREDENTIALS_EXPIRED.onViolation( msg );
            }
            else
            {
                return new AuthorizationViolationException( msg );
            }
        }

        @Override
        public String name()
        {
            return "";
        }
    }
}
