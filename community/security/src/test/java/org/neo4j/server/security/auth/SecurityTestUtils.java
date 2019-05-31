/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.security.auth;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.neo4j.configuration.Config;
import org.neo4j.server.security.systemgraph.BasicInMemorySystemGraphOperations;
import org.neo4j.server.security.systemgraph.BasicSystemGraphRealm;
import org.neo4j.server.security.systemgraph.SecurityGraphInitializer;
import org.neo4j.server.security.systemgraph.SystemGraphCredential;
import org.neo4j.time.Clocks;

import static org.neo4j.kernel.api.security.AuthToken.newBasicAuthToken;

public class SecurityTestUtils
{
    private SecurityTestUtils()
    {
    }

    public static BasicSystemGraphRealm simpleBasicSystemGraphRealm( Config config )
    {
        return new BasicSystemGraphRealm(
                new BasicInMemorySystemGraphOperations(),
                SecurityGraphInitializer.NO_OP,
                new SecureHasher(),
                new BasicPasswordPolicy(),
                new RateLimitedAuthenticationStrategy( Clocks.systemClock(), config ),
                true
        );
    }

    public static Map<String,Object> authToken( String username, String password )
    {
        return newBasicAuthToken( username, password );
    }

    public static byte[] password( String passwordString )
    {
        return passwordString != null ? passwordString.getBytes( StandardCharsets.UTF_8 ) : null;
    }

    public static SystemGraphCredential credentialFor( String passwordString )
    {
        return SystemGraphCredential.createCredentialForPassword( password( passwordString ), new SecureHasher() );
    }
}
