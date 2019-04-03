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

import java.util.Collections;
import java.util.function.Supplier;

import org.neo4j.cypher.result.QueryResult;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.security.Credential;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.logging.Log;
import org.neo4j.server.security.auth.ListSnapshot;
import org.neo4j.server.security.auth.SecureHasher;
import org.neo4j.server.security.auth.UserRepository;
import org.neo4j.string.UTF8;

import static org.neo4j.kernel.api.security.UserManager.INITIAL_PASSWORD;
import static org.neo4j.kernel.api.security.UserManager.INITIAL_USER_NAME;

public class BasicSystemGraphInitializer
{
    protected final QueryExecutor queryExecutor;
    private final BasicSystemGraphOperations systemGraphOperations;
    private final Supplier<UserRepository> migrationUserRepositorySupplier;
    private final Supplier<UserRepository> initialUserRepositorySupplier;
    private final SecureHasher secureHasher;
    protected final Log log;

    public BasicSystemGraphInitializer(
            QueryExecutor queryExecutor,
            BasicSystemGraphOperations systemGraphOperations,
            Supplier<UserRepository> migrationUserRepositorySupplier,
            Supplier<UserRepository> initialUserRepositorySupplier,
            SecureHasher secureHasher, Log log )
    {
        this.queryExecutor = queryExecutor;
        this.systemGraphOperations = systemGraphOperations;
        this.migrationUserRepositorySupplier = migrationUserRepositorySupplier;
        this.initialUserRepositorySupplier = initialUserRepositorySupplier;
        this.secureHasher = secureHasher;
        this.log = log;
    }

    public void initializeSystemGraph() throws Exception
    {
        // If the system graph has not been initialized (typically the first time you start neo4j with the system graph auth provider)
        // we set it up by
        // 1) Try to migrate users from the auth file
        // 2) If no users were migrated, create one default user
        if ( noUsers() )
        {
            // Ensure that multiple users cannot have the same name and create an index
            final QueryResult.QueryResultVisitor<RuntimeException> resultVisitor = row -> true;
            queryExecutor.executeQuery( "CREATE CONSTRAINT ON (u:User) ASSERT u.name IS UNIQUE", Collections.emptyMap(), resultVisitor );

            if ( !migrateFromAuthFile() )
            {
                ensureDefaultUser();
            }
        }
    }

    protected boolean noUsers()
    {
        String query = "MATCH (u:User) RETURN count(u)";
        return queryExecutor.executeQueryLong( query ) == 0;
    }

    protected UserRepository startUserRepository( Supplier<UserRepository> supplier ) throws Exception
    {
        UserRepository userRepository = supplier.get();
        userRepository.init();
        userRepository.start();
        return userRepository;
    }

    protected void stopUserRepository( UserRepository userRepository ) throws Exception
    {
        userRepository.stop();
        userRepository.shutdown();
    }

    /* Adds neo4j user if no users exist */
    protected void ensureDefaultUser() throws Exception
    {
        boolean addedUser = false;

        if ( initialUserRepositorySupplier != null )
        {
            UserRepository initialUserRepository = startUserRepository( initialUserRepositorySupplier );
            if ( initialUserRepository.numberOfUsers() > 0 )
            {
                // In alignment with InternalFlatFileRealm we only allow the INITIAL_USER_NAME here for now
                // (This is what we get from the `set-initial-password` command)
                User initialUser = initialUserRepository.getUserByName( INITIAL_USER_NAME );
                if ( initialUser != null )
                {
                    systemGraphOperations.addUser( initialUser );
                    addedUser = true;
                }
            }
            stopUserRepository( initialUserRepository );
        }

        // If no initial user was set create the default neo4j user
        if ( !addedUser )
        {
            Credential credential = SystemGraphCredential.createCredentialForPassword( UTF8.encode( INITIAL_PASSWORD ), secureHasher );
            User user = new User.Builder().withName( INITIAL_USER_NAME ).withCredentials( credential ).withRequiredPasswordChange( true ).withoutFlag(
                    BasicSystemGraphRealm.IS_SUSPENDED ).build();

            systemGraphOperations.addUser( user );
        }
    }

    private boolean migrateFromAuthFile() throws Exception
    {
        UserRepository userRepository = startUserRepository( migrationUserRepositorySupplier );
        ListSnapshot<User> users = userRepository.getPersistedSnapshot();

        boolean usersToMigrate = !users.values().isEmpty();

        if ( usersToMigrate )
        {
            try ( Transaction transaction = queryExecutor.beginTx() )
            {
                // This is not an efficient implementation, since it executes many queries
                // If performance ever becomes an issue we could do this with a single query instead
                for ( User user : users.values() )
                {
                    systemGraphOperations.addUser( user );
                }
                transaction.success();
            }

            String userString = users.values().size() == 1 ? "user" : "users";
            log.info( "Completed migration of %s %s into system graph.", Integer.toString( users.values().size() ), userString );
        }

        stopUserRepository( userRepository );

        return usersToMigrate;
    }
}
