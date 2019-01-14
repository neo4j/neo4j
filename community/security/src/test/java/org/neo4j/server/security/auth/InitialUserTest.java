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

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.security.Credential;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public abstract class InitialUserTest
{
    @Rule
    public EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();

    protected Config config;
    protected UserRepository users;

    protected abstract AuthManager authManager();

    @Test
    public void shouldCreateDefaultUserIfNoneExist() throws Throwable
    {
        // When
        authManager().start();

        // Then
        final User user = users.getUserByName( "neo4j" );
        assertNotNull( user );
        assertTrue( user.credentials().matchesPassword( "neo4j" ) );
        assertTrue( user.passwordChangeRequired() );
    }

    @Test
    public void shouldLoadInitialUserIfNoneExist() throws Throwable
    {
        // Given
        FileUserRepository initialUserRepository =
                CommunitySecurityModule.getInitialUserRepository( config, NullLogProvider.getInstance(), fsRule.get() );
        initialUserRepository.start();
        initialUserRepository.create(
                new User.Builder( "neo4j", Credential.forPassword( "123" ) )
                        .withRequiredPasswordChange( false )
                        .build()
        );
        initialUserRepository.shutdown();

        // When
        authManager().start();

        // Then
        final User user = users.getUserByName( "neo4j" );
        assertNotNull( user );
        assertTrue( user.credentials().matchesPassword( "123" ) );
        assertFalse( user.passwordChangeRequired() );
    }

    @Test
    public void shouldLoadInitialUserIfNoneExistEvenWithSamePassword() throws Throwable
    {
        // Given
        FileUserRepository initialUserRepository =
                CommunitySecurityModule.getInitialUserRepository( config, NullLogProvider.getInstance(), fsRule.get() );
        initialUserRepository.start();
        initialUserRepository.create(
                new User.Builder( "neo4j", Credential.forPassword( "neo4j" ) )
                        .withRequiredPasswordChange( false )
                        .build()
        );
        initialUserRepository.shutdown();

        // When
        authManager().start();

        // Then
        final User user = users.getUserByName( "neo4j" );
        assertNotNull( user );
        assertTrue( user.credentials().matchesPassword( "neo4j" ) );
        assertFalse( user.passwordChangeRequired() );
    }

    @Test
    public void shouldNotAddInitialUserIfUsersExist() throws Throwable
    {
        // Given
        FileUserRepository initialUserRepository =
                CommunitySecurityModule.getInitialUserRepository( config, NullLogProvider.getInstance(), fsRule.get() );
        initialUserRepository.start();
        initialUserRepository.create( newUser( "initUser", "123", false ) );
        initialUserRepository.shutdown();
        users.start();
        users.create( newUser( "oldUser", "321", false ) );
        users.shutdown();

        // When
        authManager().start();

        // Then
        final User initUser = users.getUserByName( "initUser" );
        assertNull( initUser );

        final User oldUser = users.getUserByName( "oldUser" );
        assertNotNull( oldUser );
        assertTrue( oldUser.credentials().matchesPassword( "321" ) );
        assertFalse( oldUser.passwordChangeRequired() );
    }

    @Test
    public void shouldNotUpdateUserIfInitialUserExist() throws Throwable
    {
        // Given
        FileUserRepository initialUserRepository =
                CommunitySecurityModule.getInitialUserRepository( config, NullLogProvider.getInstance(), fsRule.get() );
        initialUserRepository.start();
        initialUserRepository.create( newUser( "oldUser", "newPassword", false ) );
        initialUserRepository.shutdown();
        users.start();
        users.create( newUser( "oldUser", "oldPassword", true ) );
        users.shutdown();

        // When
        authManager().start();

        // Then
        final User oldUser = users.getUserByName( "oldUser" );
        assertNotNull( oldUser );
        assertTrue( oldUser.credentials().matchesPassword( "oldPassword" ) );
        assertTrue( oldUser.passwordChangeRequired() );
    }

    protected User newUser( String userName, String password, boolean pwdChange )
    {
        return new User.Builder( userName, Credential.forPassword( password ) )
                .withRequiredPasswordChange( pwdChange )
                .build();
    }
}
