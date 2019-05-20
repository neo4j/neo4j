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
package org.neo4j.cypher.security;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.server.security.auth.InMemoryUserRepository;
import org.neo4j.server.security.auth.UserRepository;

import static org.neo4j.kernel.api.security.UserManager.INITIAL_USER_NAME;
import static org.neo4j.server.security.auth.SecurityTestUtils.credentialFor;

public class BasicImportOptionsBuilder
{
    protected List<User> migrateUsers = new ArrayList<>();
    protected List<User> initialUsers = new ArrayList<>();

    protected BasicImportOptionsBuilder()
    {
    }

    protected BasicImportOptionsBuilder migrateUser( String userName, String password, boolean pwdChangeRequired )
    {
        migrateUsers.add( createUser( userName, password, pwdChangeRequired ) );
        return this;
    }

    protected BasicImportOptionsBuilder migrateUsers( String... migrateUsers )
    {
        fillListWithUsers( this.migrateUsers, migrateUsers );
        return this;
    }

    protected BasicImportOptionsBuilder initialUser( String password, boolean pwdChangeRequired )
    {
        this.initialUsers.add( createUser( INITIAL_USER_NAME, password, pwdChangeRequired ) );
        return this;
    }

    Supplier<UserRepository> migrationSupplier() throws IOException, InvalidArgumentsException
    {
        UserRepository migrationUserRepository = new InMemoryUserRepository();
        populateUserRepository( migrationUserRepository, migrateUsers );
        return () -> migrationUserRepository;
    }

    Supplier<UserRepository> initialUserSupplier() throws IOException, InvalidArgumentsException
    {
        UserRepository initialUserRepository = new InMemoryUserRepository();
        populateUserRepository( initialUserRepository, initialUsers );
        return () -> initialUserRepository;
    }

    protected static void populateUserRepository( UserRepository repository, List<User> users ) throws IOException, InvalidArgumentsException
    {
        for ( User user : users )
        {
            repository.create( user );
        }
    }

    protected void fillListWithUsers( List<User> list, String... userNames )
    {
        for ( String userName :userNames )
        {
            // Use username as password to simplify test assertions
            list.add( createUser( userName, userName, false ) );
        }
    }

    private static User createUser( String userName, String password, boolean pwdChangeRequired )
    {
        return new User.Builder( userName, credentialFor( password ) ).withRequiredPasswordChange( pwdChangeRequired ).build();
    }
}
