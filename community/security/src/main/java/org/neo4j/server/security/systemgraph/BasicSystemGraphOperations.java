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


import org.neo4j.cypher.internal.security.FormatException;
import org.neo4j.cypher.internal.security.SecureHasher;
import org.neo4j.cypher.internal.security.SystemGraphCredential;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.security.AuthProviderFailedException;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.impl.security.Credential;
import org.neo4j.kernel.impl.security.User;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.kernel.database.DatabaseIdRepository.SYSTEM_DATABASE_ID;

public class BasicSystemGraphOperations
{
    private final SecureHasher secureHasher;
    private final DatabaseManager<?> databaseManager;

    public BasicSystemGraphOperations( DatabaseManager databaseManager, SecureHasher secureHasher )
    {
        this.databaseManager = databaseManager;
        this.secureHasher = secureHasher;
    }

    User getUser( String username ) throws InvalidArgumentsException, FormatException
    {
        User user;
        try ( Transaction tx = getSystemDb().beginTx() )
        {
            Node userNode = tx.findNode( Label.label( "User" ), "name", username );

            if ( userNode == null )
            {
                throw new InvalidArgumentsException( "User '" + username + "' does not exist." );
            }
            else
            {
                Credential credential = SystemGraphCredential.deserialize((String) userNode.getProperty( "credentials" ) , secureHasher );
                boolean requirePasswordChange = (boolean) userNode.getProperty( "passwordChangeRequired" );
                boolean suspended = (boolean) userNode.getProperty( "suspended" );

                if ( suspended )
                {
                    user = new User.Builder( username, credential )
                            .withRequiredPasswordChange( requirePasswordChange )
                            .withFlag( BasicSystemGraphRealm.IS_SUSPENDED )
                            .build();
                }
                else
                {
                    user = new User.Builder( username, credential )
                            .withRequiredPasswordChange( requirePasswordChange )
                            .withoutFlag( BasicSystemGraphRealm.IS_SUSPENDED )
                            .build();
                }
            }
            tx.commit();
        }
        return user;
    }

    protected GraphDatabaseService getSystemDb()
    {
        return databaseManager.getDatabaseContext( SYSTEM_DATABASE_ID ).orElseThrow(
                () -> new AuthProviderFailedException( "No database called `" + SYSTEM_DATABASE_NAME + "` was found." ) ).databaseFacade();
    }
}
