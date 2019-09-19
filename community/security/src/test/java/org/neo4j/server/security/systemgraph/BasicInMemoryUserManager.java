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
import java.util.Set;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.security.PasswordPolicy;
import org.neo4j.kernel.impl.security.Credential;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.server.security.auth.AuthenticationStrategy;
import org.neo4j.server.security.auth.BasicPasswordPolicy;
import org.neo4j.server.security.auth.RateLimitedAuthenticationStrategy;
import org.neo4j.server.security.auth.SecureHasher;
import org.neo4j.server.security.auth.exception.FormatException;
import org.neo4j.string.UTF8;
import org.neo4j.time.Clocks;

import static org.mockito.Mockito.mock;

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
        super( null,
                SecurityGraphInitializer.NO_OP,
                new SecureHasher(),
                new BasicPasswordPolicy(),
                authStategy,
                true);
        setupDefaultUser();
    }

    private void setupDefaultUser() throws InvalidArgumentsException
    {
        Credential credential = SystemGraphCredential.createCredentialForPassword( UTF8.encode( INITIAL_PASSWORD ), mock(SecureHasher.class) );
        User user = new User.Builder().withName( INITIAL_USER_NAME ).withCredentials( credential ).withRequiredPasswordChange( true ).withoutFlag(
                IS_SUSPENDED ).build();

        addUser( user );
    }

    @Override
    public User newUser( String username, byte[] initialPassword, boolean requirePasswordChange ) throws InvalidArgumentsException
    {
        try
        {
            assertValidUsername( username );
            passwordPolicy.validatePassword( initialPassword );

            Credential credential = SystemGraphCredential.createCredentialForPassword( initialPassword, secureHasher );
            User user = new User.Builder()
                    .withName( username )
                    .withCredentials( credential )
                    .withRequiredPasswordChange( requirePasswordChange )
                    .withoutFlag( IS_SUSPENDED )
                    .build();

            addUser( user );
            return user;
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

    private void addUser( User user ) throws InvalidArgumentsException
    {
        String username = user.name();
        assertValidUsername( username );
        if ( users.containsKey( username ) )
        {
            throw new InvalidArgumentsException( "The specified user '" + username + "' already exists." );
        }
        users.put( username, user );
    }

    @Override
    public Set<String> getAllUsernames()
    {
        return users.keySet();
    }

    @Override
    public boolean deleteUser( String username ) throws InvalidArgumentsException
    {
        User removed = users.remove( username );
        if ( removed == null )
        {
            throw new InvalidArgumentsException( "User '" + username + "' does not exist." );
        }
        return true;
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

    @Override
    public User silentlyGetUser( String username )
    {
        try
        {
            return getUser( username );
        }
        catch ( InvalidArgumentsException e )
        {
            return null;
        }
    }

    @Override
    public void setUserPassword( String username, byte[] password, boolean requirePasswordChange ) throws InvalidArgumentsException
    {
        try
        {
            User existingUser = getUser( username );
            passwordPolicy.validatePassword( password );

            if ( existingUser.credentials().matchesPassword( password ) )
            {
                throw new InvalidArgumentsException( "Old password and new password cannot be the same." );
            }

            String newCredentials = SystemGraphCredential.serialize( SystemGraphCredential.createCredentialForPassword( password, secureHasher ) );
            setUserCredentials( username, newCredentials, requirePasswordChange );
        }
        finally
        {
            // Clear password
            if ( password != null )
            {
                Arrays.fill( password, (byte) 0 );
            }
        }
    }

    private void setUserCredentials( String username, String newCredentials, boolean requirePasswordChange ) throws InvalidArgumentsException
    {
        User user = users.get( username );
        if ( user == null )
        {
            throw new InvalidArgumentsException( "User '" + username + "' does not exist." );
        }
        User augmented;
        try
        {
            augmented = user.augment()
                    .withCredentials( SystemGraphCredential.deserialize( newCredentials, new SecureHasher() ) )
                    .withRequiredPasswordChange( requirePasswordChange ).build();
        }
        catch ( FormatException e )
        {
            throw new RuntimeException( e );
        }

        users.put( username, augmented );
    }
}
