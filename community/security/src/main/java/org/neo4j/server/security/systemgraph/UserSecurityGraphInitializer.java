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

import org.neo4j.dbms.database.SystemGraphInitializer;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.impl.security.Credential;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.logging.Log;
import org.neo4j.server.security.auth.ListSnapshot;
import org.neo4j.server.security.auth.SecureHasher;
import org.neo4j.server.security.auth.UserRepository;
import org.neo4j.string.UTF8;

import static org.neo4j.kernel.api.security.UserManager.INITIAL_PASSWORD;
import static org.neo4j.kernel.api.security.UserManager.INITIAL_USER_NAME;

public class UserSecurityGraphInitializer implements SecurityGraphInitializer
{
    protected final SystemGraphInitializer systemGraphInitializer;
    protected QueryExecutor queryExecutor;
    protected Log log;
    private final BasicSystemGraphOperations systemGraphOperations;

    private final Supplier<UserRepository> migrationUserRepositorySupplier;
    private final Supplier<UserRepository> initialUserRepositorySupplier;
    private final SecureHasher secureHasher;

    public UserSecurityGraphInitializer(
            SystemGraphInitializer systemGraphInitializer,
            QueryExecutor queryExecutor,
            Log log,
            BasicSystemGraphOperations systemGraphOperations,
            Supplier<UserRepository> migrationUserRepositorySupplier,
            Supplier<UserRepository> initialUserRepositorySupplier,
            SecureHasher secureHasher )
    {
        this.systemGraphInitializer = systemGraphInitializer;
        this.queryExecutor = queryExecutor;
        this.log = log;
        this.systemGraphOperations = systemGraphOperations;
        this.migrationUserRepositorySupplier = migrationUserRepositorySupplier;
        this.initialUserRepositorySupplier = initialUserRepositorySupplier;
        this.secureHasher = secureHasher;
    }

    @Override
    public void initializeSecurityGraph( GraphDatabaseService database ) throws Exception
    {
        systemGraphInitializer.initializeSystemGraph( database );
        doInitializeSecurityGraph();
    }

    public void initializeSecurityGraph() throws Exception
    {
        systemGraphInitializer.initializeSystemGraph();
        doInitializeSecurityGraph();
    }

    private void doInitializeSecurityGraph() throws Exception
    {
        // If the system graph has not been initialized (typically the first time you start neo4j) we set it up by:
        // 1) Try to migrate users from the auth file
        // 2) If no users were migrated, create one default user
        if ( nbrOfUsers() == 0 )
        {
            setupConstraints();
            migrateFromAuthFile();
        }

        if ( nbrOfUsers() == 0 )
        {
            ensureDefaultUser();
        }
        else
        {
            ensureCorrectInitialPassword();
        }
    }

    protected long nbrOfUsers()
    {
        return systemGraphOperations.getAllUsernames().size();
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

    private void setupConstraints() throws InvalidArgumentsException

    {
        // Ensure that multiple users cannot have the same name and are indexed
        queryExecutor.executeQuery( "CREATE CONSTRAINT ON (u:User) ASSERT u.name IS UNIQUE", Collections.emptyMap(),
                new ErrorPreservingQuerySubscriber() );
    }

    private boolean onlyDefaultUserWithDefaultPassword() throws Exception
    {
        if ( nbrOfUsers() == 1 )
        {
            User user = systemGraphOperations.getUser( INITIAL_USER_NAME, true );
            return user != null && user.credentials().matchesPassword( UTF8.encode( INITIAL_PASSWORD ) );
        }
        return false;
    }

    protected void ensureCorrectInitialPassword() throws Exception
    {
        if ( onlyDefaultUserWithDefaultPassword() )
        {
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
                        systemGraphOperations.setUserCredentials( INITIAL_USER_NAME, initialUser.credentials().serialize(), false );
                    }
                }
                stopUserRepository( initialUserRepository );
            }
        }
    }

    private void migrateFromAuthFile() throws Exception
    {
        UserRepository userRepository = startUserRepository( migrationUserRepositorySupplier );
        doImportUsers( userRepository );
        stopUserRepository( userRepository );
    }

    protected void doImportUsers( UserRepository userRepository ) throws Exception
    {
        ListSnapshot<User> users = userRepository.getPersistedSnapshot();

        if ( !users.values().isEmpty() )
        {
            try ( Transaction transaction = queryExecutor.beginTx() )
            {

                // This is not an efficient implementation, since it executes many queries
                // If performance ever becomes an issue we could do this with a single query instead
                for ( User user : users.values() )
                {
                    systemGraphOperations.addUser( user );
                }
                transaction.commit();
            }

            // Log what happened to the security log
            String userString = users.values().size() == 1 ? "user" : "users";
            log.info( "Completed import of %s %s into system graph.", Integer.toString( users.values().size() ), userString );
        }
    }
}
