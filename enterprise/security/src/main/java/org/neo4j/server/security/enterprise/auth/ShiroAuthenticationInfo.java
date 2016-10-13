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

import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.SimpleAuthenticationInfo;
import org.apache.shiro.util.ByteSource;

import org.neo4j.kernel.api.security.AuthenticationResult;

import static org.neo4j.kernel.api.security.AuthenticationResult.*;

public class ShiroAuthenticationInfo extends SimpleAuthenticationInfo
{
    private AuthenticationResult authenticationResult;

    public ShiroAuthenticationInfo()
    {
        super();
        this.authenticationResult = AuthenticationResult.FAILURE;
    }

    public ShiroAuthenticationInfo( Object principal, String realmName, AuthenticationResult authenticationResult )
    {
        super( principal, null, realmName );
        this.authenticationResult = authenticationResult;
    }

    public ShiroAuthenticationInfo( Object principal, Object hashedCredentials, ByteSource credentialsSalt,
            String realmName, AuthenticationResult authenticationResult )
    {
        super( principal, hashedCredentials, credentialsSalt, realmName );
        this.authenticationResult = authenticationResult;
    }

    public AuthenticationResult getAuthenticationResult()
    {
        return authenticationResult;
    }

    @Override
    public void merge( AuthenticationInfo info )
    {
        if (info == null || info.getPrincipals() == null || info.getPrincipals().isEmpty()) {
            return;
        }

        super.merge( info );

        if ( info instanceof ShiroAuthenticationInfo )
        {
            authenticationResult = mergeAuthenticationResult( authenticationResult,
                    ((ShiroAuthenticationInfo) info).getAuthenticationResult() );
        }
        else
        {
            // If we get here (which means no AuthenticationException or UnknownAccountException was thrown)
            // it means the realm that provided the info was able to authenticate the subject,
            // so we claim the result to be an implicit success
            authenticationResult = mergeAuthenticationResult( authenticationResult, AuthenticationResult.SUCCESS );
        }
    }

    private static AuthenticationResult[][] mergeMatrix = {
        /* v result | new res >   SUCCESS,                  FAILURE,                  TOO_MANY_ATTEMPTS,        PASSWORD_CHANGE_REQUIRED */
        /* SUCCESS           */ { SUCCESS,                  SUCCESS,                  SUCCESS          ,        PASSWORD_CHANGE_REQUIRED },
        /* FAILURE           */ { SUCCESS,                  FAILURE,                  TOO_MANY_ATTEMPTS,        PASSWORD_CHANGE_REQUIRED },
        /* TOO_MANY_ATTEMPTS */ { SUCCESS,                  TOO_MANY_ATTEMPTS,        TOO_MANY_ATTEMPTS,        PASSWORD_CHANGE_REQUIRED },
        /* PASSWORD_CHANGE.. */ { PASSWORD_CHANGE_REQUIRED, PASSWORD_CHANGE_REQUIRED, PASSWORD_CHANGE_REQUIRED, PASSWORD_CHANGE_REQUIRED }
    };

    private static AuthenticationResult mergeAuthenticationResult(
            AuthenticationResult result, AuthenticationResult newResult )
    {
        AuthenticationResult mergedResult = mergeMatrix[result.ordinal()][newResult.ordinal()];
        return mergedResult;
    }
}
