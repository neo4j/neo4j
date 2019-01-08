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
