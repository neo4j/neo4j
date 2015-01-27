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
import java.io.Writer;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.util.TestLogging;
import org.neo4j.server.security.auth.exception.ConcurrentModificationException;
import org.neo4j.test.EphemeralFileSystemRule;

import static java.lang.String.format;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.impl.util.TestLogger.LogCall.error;
import static org.neo4j.kernel.logging.DevNullLoggingService.DEV_NULL;

public class FileUserRepositoryTest
{
    public @Rule EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();

    @Test
    public void shouldStoreAndRetriveUsersByName() throws Exception
    {
        // Given
        FileUserRepository users = new FileUserRepository( fsRule.get(), new File( "dbms/auth.db" ), DEV_NULL );
        User user = new User( "jake", Credential.INACCESSIBLE, true );
        users.create( user );

        // When
        User result = users.findByName( user.name() );

        // Then
        assertThat(result, equalTo(user));
    }

    @Test
    public void shouldPersistUsers() throws Throwable
    {
        // Given
        FileUserRepository users = new FileUserRepository( fsRule.get(), new File( "dbms/auth.db" ), DEV_NULL );
        User user = new User( "jake", Credential.INACCESSIBLE, true );
        users.create( user );

        users = new FileUserRepository( fsRule.get(), new File( "dbms/auth.db" ), DEV_NULL );
        users.start();

        // When
        User resultByName = users.findByName( user.name() );

        // Then
        assertThat( resultByName, equalTo( user ) );
    }

    @Test
    public void shouldNotFindUserAfterDelete() throws Throwable
    {
        // Given
        FileUserRepository users = new FileUserRepository( fsRule.get(), new File( "dbms/auth.db" ), DEV_NULL );
        User user = new User( "jake", Credential.INACCESSIBLE, true );
        users.create( user );

        // When
        users.delete( user );

        // Then
        assertThat( users.findByName( user.name() ), nullValue() );
    }

    @Test
    public void shouldNotAllowComplexNames() throws Exception
    {
        // Given
        FileUserRepository users = new FileUserRepository( fsRule.get(), new File( "dbms/auth.db" ), DEV_NULL );

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
        FileUserRepository users = new FileUserRepository( fsRule.get(), dbFile, DEV_NULL );
        User user = new User( "jake", Credential.INACCESSIBLE, true );
        users.create( user );

        // And given we emulate having crashed when writing
        File tempFile = new File( dbFile.getAbsolutePath() + ".tmp" );
        fsRule.get().renameFile( dbFile, tempFile );

        // When
        users = new FileUserRepository( fsRule.get(), dbFile, DEV_NULL );
        users.start();

        // Then
        assertFalse(fsRule.get().fileExists( tempFile ));
        assertTrue(fsRule.get().fileExists( dbFile ));
        assertThat( users.findByName( user.name() ), equalTo(user));
    }

    @Test
    public void shouldThrowIfUpdateChangesName() throws Throwable
    {
        // Given
        FileUserRepository users = new FileUserRepository( fsRule.get(), new File( "dbms/auth.db" ), DEV_NULL );
        User user = new User( "jake", Credential.INACCESSIBLE, true );
        users.create( user );

        // When
        User updatedUser = new User( "john", Credential.INACCESSIBLE, true );
        try
        {
            users.update( user, updatedUser );
            fail( "expected exception not thrown" );
        } catch ( IllegalArgumentException e )
        {
            // Then continue
        }

        assertThat( users.findByName( user.name() ), equalTo( user ) );
    }

    @Test
    public void shouldThrowIfExistingUserDoesNotMatch() throws Throwable
    {
        // Given
        FileUserRepository users = new FileUserRepository( fsRule.get(), new File( "dbms/auth.db" ), DEV_NULL );
        User user = new User( "jake", Credential.INACCESSIBLE, true );
        users.create( user );
        User modifiedUser = new User( "jake", Credential.forPassword( "foo" ), false );

        // When
        User updatedUser = new User( "jake", Credential.forPassword( "bar" ), false );
        try
        {
            users.update( modifiedUser, updatedUser );
            fail( "expected exception not thrown" );
        } catch ( ConcurrentModificationException e )
        {
            // Then continue
        }
    }

    @Test
    public void shouldIgnoreInvalidEntries() throws Throwable
    {
        // Given
        FileSystemAbstraction fs = fsRule.get();
        File authFile = new File( "auth.db" );
        TestLogging testLogging = new TestLogging();

        Writer writer = fs.openAsWriter( authFile, "UTF-8", false );
        writer.write( "neo4j:fc4c600b43ffe4d5857b4439c35df88f:SHA-256,A42E541F276CF17036DB7818F8B09B1C229AAD52A17F69F4029617F3A554640F,FB7E8AE08A6A7C741F678AD22217808F:" );
        writer.close();

        // When
        FileUserRepository users = new FileUserRepository( fs, authFile, testLogging );
        users.start();

        // Then
        assertThat( users.numberOfUsers(), equalTo( 0 ) );
        testLogging.getMessagesLog( FileUserRepository.class ).assertExactly(
                error( format( "Ignoring authorization file \"%s\" (wrong number of line fields [line 1])", authFile.getAbsolutePath() ) ) );
    }
}
