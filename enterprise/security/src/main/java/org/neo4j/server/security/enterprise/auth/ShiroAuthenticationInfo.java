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

import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.SimpleAuthenticationInfo;
import org.apache.shiro.util.ByteSource;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.internal.kernel.api.security.AuthenticationResult;

import static org.neo4j.internal.kernel.api.security.AuthenticationResult.FAILURE;
import static org.neo4j.internal.kernel.api.security.AuthenticationResult.PASSWORD_CHANGE_REQUIRED;
import static org.neo4j.internal.kernel.api.security.AuthenticationResult.SUCCESS;
import static org.neo4j.internal.kernel.api.security.AuthenticationResult.TOO_MANY_ATTEMPTS;

public class ShiroAuthenticationInfo extends SimpleAuthenticationInfo
{
    private AuthenticationResult authenticationResult;
    private List<Throwable> throwables;

    public ShiroAuthenticationInfo()
    {
        super();
        this.authenticationResult = AuthenticationResult.FAILURE;
        this.throwables = new ArrayList<>( 1 );
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

    public void addThrowable( Throwable t )
    {
        throwables.add( t );
    }

    public List<Throwable> getThrowables()
    {
        return throwables;
    }

    @Override
    public void merge( AuthenticationInfo info )
    {
        if ( info == null || info.getPrincipals() == null || info.getPrincipals().isEmpty() )
        {
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
