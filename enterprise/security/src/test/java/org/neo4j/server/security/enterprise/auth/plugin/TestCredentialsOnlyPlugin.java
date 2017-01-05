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
package org.neo4j.server.security.enterprise.auth.plugin;

import java.util.Arrays;

import org.neo4j.server.security.enterprise.auth.plugin.api.AuthToken;
import org.neo4j.server.security.enterprise.auth.plugin.spi.AuthenticationPlugin;
import org.neo4j.server.security.enterprise.auth.plugin.spi.CustomCacheableAuthenticationInfo;

public class TestCredentialsOnlyPlugin extends AuthenticationPlugin.Adapter
{
    @Override
    public String name()
    {
        return getClass().getSimpleName();
    }

    @Override
    public AuthenticationInfo authenticate( AuthToken authToken )
    {
        String username = validateCredentials( authToken.credentials() );
        return new AuthenticationInfo( username, authToken.credentials() );
    }

    /**
     * Performs decryptions of the credentials and returns the decrypted username if successful
     */
    private String validateCredentials( char[] credentials )
    {
        return "trinity@MATRIX.NET";
    }

    class AuthenticationInfo implements CustomCacheableAuthenticationInfo, CustomCacheableAuthenticationInfo.CredentialsMatcher
    {
        private final String username;
        private final char[] credentials;

        AuthenticationInfo( String username, char[] credentials )
        {
            this.username = username;
            this.credentials = credentials;
        }

        @Override
        public Object principal()
        {
            return username;
        }

        @Override
        public CredentialsMatcher credentialsMatcher()
        {
            return this;
        }

        @Override
        public boolean doCredentialsMatch( AuthToken authToken )
        {
            return Arrays.equals( authToken.credentials(), credentials );
        }
    }
}
