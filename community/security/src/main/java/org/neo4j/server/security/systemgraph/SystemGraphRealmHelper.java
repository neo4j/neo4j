/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import java.util.function.Supplier;

import org.neo4j.cypher.internal.security.FormatException;
import org.neo4j.cypher.internal.security.SecureHasher;
import org.neo4j.cypher.internal.security.SystemGraphCredential;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.security.AuthProviderFailedException;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.impl.security.Credential;
import org.neo4j.kernel.impl.security.User;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.kernel.database.DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID;

public class SystemGraphRealmHelper
{
    private final Supplier<GraphDatabaseService> systemSupplier;
    private final SecureHasher secureHasher;
    private GraphDatabaseService systemDb;

    /**
     * This flag is used in the same way as User.PASSWORD_CHANGE_REQUIRED, but it's
     * placed here because of user suspension not being a part of community edition
     */
    public static final String IS_SUSPENDED = "is_suspended";
    public static final String DEFAULT_DATABASE = "default_database";

    public SystemGraphRealmHelper( Supplier<GraphDatabaseService> systemSupplier, SecureHasher secureHasher )
    {
        this.systemSupplier = systemSupplier;
        this.secureHasher = secureHasher;
    }

    public User getUser( String username ) throws InvalidArgumentsException, FormatException
    {
        try ( Transaction tx = getSystemDb().beginTx() )
        {
            Node userNode = tx.findNode( Label.label( "User" ), "name", username );

            if ( userNode == null )
            {
                throw new InvalidArgumentsException( "User '" + username + "' does not exist." );
            }

            Credential credential = SystemGraphCredential.deserialize( (String) userNode.getProperty( "credentials" ), secureHasher );
            boolean requirePasswordChange = (boolean) userNode.getProperty( "passwordChangeRequired" );
            boolean suspended = (boolean) userNode.getProperty( "suspended" );
            String defaultDatabase = (String) userNode.getProperty( "defaultDatabase", null );
            tx.commit();

            User.Builder builder = new User.Builder( username, credential ).withRequiredPasswordChange( requirePasswordChange );
            builder = suspended ? builder.withFlag( IS_SUSPENDED ) : builder.withoutFlag( IS_SUSPENDED );
            builder = builder.withDefaultDatabase( defaultDatabase );
            return builder.build();
        }
        catch ( NotFoundException n )
        {
            // Can occur if the user was dropped by another thread after the null check.
            throw new InvalidArgumentsException( "User '" + username + "' does not exist." );
        }
    }

    public GraphDatabaseService getSystemDb()
    {
        if ( systemDb == null )
        {
            systemDb = systemSupplier.get();
        }
        return systemDb;
    }

    public static Supplier<GraphDatabaseService> makeSystemSupplier( DatabaseManager<?> databaseManager )
    {
        return () -> databaseManager.getDatabaseContext( NAMED_SYSTEM_DATABASE_ID ).orElseThrow(
                () -> new AuthProviderFailedException( "No database called `" + SYSTEM_DATABASE_NAME + "` was found." ) ).databaseFacade();
    }
}
