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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.server.security.auth.SecureHasher;
import org.neo4j.server.security.auth.exception.FormatException;

import static org.mockito.Mockito.mock;
import static org.neo4j.server.security.systemgraph.BasicSystemGraphRealm.assertValidUsername;

public class BasicInMemorySystemGraphOperations extends BasicSystemGraphOperations
{
    public Map<String,User> users = new HashMap<>();

    public BasicInMemorySystemGraphOperations()
    {
        super( mock( QueryExecutor.class ), mock(SecureHasher.class) );
    }

    @Override
    public void addUser( User user ) throws InvalidArgumentsException
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
    public User getUser( String username, boolean silent ) throws InvalidArgumentsException
    {
        User user = users.get( username );
        if ( !silent && user == null )
        {
            throw new InvalidArgumentsException( "User '" + username + "' does not exist." );
        }
        return user;
    }

    @Override
    public void setUserCredentials( String username, String newCredentials, boolean requirePasswordChange ) throws InvalidArgumentsException
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
