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

import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.session.Session;
import org.apache.shiro.subject.PrincipalCollection;
import org.apache.shiro.subject.support.DelegatingSubject;

import org.neo4j.kernel.api.security.AuthenticationResult;

public class ShiroSubject extends DelegatingSubject
{
    private AuthenticationResult authenticationResult;

    public ShiroSubject( SecurityManager securityManager, AuthenticationResult authenticationResult )
    {
        super( securityManager );
        this.authenticationResult = authenticationResult;
    }

    public ShiroSubject( PrincipalCollection principals, boolean authenticated, String host, Session session,
            boolean sessionCreationEnabled, SecurityManager securityManager, AuthenticationResult authenticationResult )
    {
        super( principals, authenticated, host, session, sessionCreationEnabled, securityManager );
        this.authenticationResult = authenticationResult;
    }

    public AuthenticationResult getAuthenticationResult()
    {
        return authenticationResult;
    }

    void setAuthenticationResult( AuthenticationResult authenticationResult )
    {
        this.authenticationResult = authenticationResult;
    }
}
