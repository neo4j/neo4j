/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import java.io.File;

import org.neo4j.test.EphemeralFileSystemRule;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FileUserRepositoryTest
{
    public @Rule EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();

    @Test
    public void shouldStoreAndRetriveUsers() throws Exception
    {
        // Given
        FileUserRepository users = new FileUserRepository( fsRule.get(), new File( "dbms/auth.db" ) );
        User user = new User( "jake", "af123", Privileges.ADMIN, Credentials.INACCESSIBLE, true );
        users.save( user );

        // When
        User result = users.get( user.name() );

        // Then
        assertThat(result, equalTo(user));
    }

    @Test
    public void shouldPersistUsers() throws Throwable
    {
        // Given
        FileUserRepository users = new FileUserRepository( fsRule.get(), new File( "dbms/auth.db" ) );
        User user = new User( "jake", "af123", Privileges.ADMIN, Credentials.INACCESSIBLE, true );
        users.save( user );

        users = new FileUserRepository( fsRule.get(), new File( "dbms/auth.db" ) );
        users.start();

        // When
        User result = users.get( user.name() );

        // Then
        assertThat(result, equalTo(user));
    }

    @Test
    public void shouldNotAllowComplexNames() throws Exception
    {
        // Given
        FileUserRepository users = new FileUserRepository( fsRule.get(), new File( "dbms/auth.db" ) );

        // When
        assertTrue( users.isValidName( "neo4j" ) );
        assertTrue( users.isValidName( "johnosbourne" ) );
        assertTrue( users.isValidName( "john_osbourne" ) );

        assertFalse( users.isValidName( ":" ) );
        assertFalse( users.isValidName( "" ) );
        assertFalse( users.isValidName( "john osbourne" ) );
        assertFalse( users.isValidName( "john:osbourne" ) );
    }

    @Test
    public void shouldRecoverIfCrashedDuringWrite() throws Throwable
    {
        // Given
        File dbFile = new File( "dbms/auth.db" );
        FileUserRepository users = new FileUserRepository( fsRule.get(), dbFile );
        User user = new User( "jake", "af123", Privileges.ADMIN, Credentials.INACCESSIBLE, true );
        users.save( user );

        // And given we emulate having crashed when writing
        File tempFile = new File( dbFile.getAbsolutePath() + ".tmp" );
        fsRule.get().renameFile( dbFile, tempFile );

        // When
        users = new FileUserRepository( fsRule.get(), dbFile );
        users.start();

        // Then
        assertFalse(fsRule.get().fileExists( tempFile ));
        assertTrue(fsRule.get().fileExists( dbFile ));
        assertThat( users.get( user.name() ), equalTo(user));
    }
}
