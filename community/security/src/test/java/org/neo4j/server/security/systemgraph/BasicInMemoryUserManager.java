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
package org.neo4j.server.security.systemgraph;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.configuration.Config;
import org.neo4j.cypher.internal.security.SecureHasher;
import org.neo4j.cypher.internal.security.SystemGraphCredential;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.security.PasswordPolicy;
import org.neo4j.kernel.impl.security.Credential;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.server.security.auth.AuthenticationStrategy;
import org.neo4j.server.security.auth.BasicPasswordPolicy;
import org.neo4j.server.security.auth.RateLimitedAuthenticationStrategy;
import org.neo4j.string.UTF8;
import org.neo4j.time.Clocks;

public class BasicInMemoryUserManager extends BasicSystemGraphRealm
{

    public Map<String,User> users = new HashMap<>();
    private SecureHasher secureHasher = new SecureHasher();
    private PasswordPolicy passwordPolicy = new BasicPasswordPolicy();

    public BasicInMemoryUserManager( Config config ) throws InvalidArgumentsException
    {
        this ( new RateLimitedAuthenticationStrategy( Clocks.systemClock(), config ) );
    }

    public BasicInMemoryUserManager( AuthenticationStrategy authStategy ) throws InvalidArgumentsException
    {
        super( SecurityGraphInitializer.NO_OP, null, new SecureHasher(), authStategy, true );

        // Setup initial user
        newUser( INITIAL_USER_NAME,  UTF8.encode( INITIAL_PASSWORD ), true );
    }

    public void newUser( String username, byte[] initialPassword, boolean requirePasswordChange ) throws InvalidArgumentsException
    {
        try
        {
            if ( users.containsKey( username ) )
            {
                throw new InvalidArgumentsException( "The specified user '" + username + "' already exists." );
            }
            assertValidUsername( username );
            passwordPolicy.validatePassword( initialPassword );

            Credential credential = SystemGraphCredential.createCredentialForPassword( initialPassword, secureHasher );
            User user = new User.Builder( username, credential )
                    .withRequiredPasswordChange( requirePasswordChange )
                    .withoutFlag( IS_SUSPENDED )
                    .build();

            users.put( username, user );
        }
        finally
        {
            // Clear password
            if ( initialPassword != null )
            {
                Arrays.fill( initialPassword, (byte) 0 );
            }
        }
    }

    @Override
    public User getUser( String username ) throws InvalidArgumentsException
    {
        User user = users.get( username );
        if ( user == null )
        {
            throw new InvalidArgumentsException( "User '" + username + "' does not exist." );
        }
        return user;
    }
}
