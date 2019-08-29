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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.CopyOption;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.neo4j.io.fs.DelegatingFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.security.auth.exception.ConcurrentModificationException;
import org.neo4j.string.UTF8;
import org.neo4j.test.DoubleLatch;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.test.assertion.Assert.assertException;

@EphemeralTestDirectoryExtension
class FileUserRepositoryTest
{
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private TestDirectory testDirectory;

    private final LogProvider logProvider = NullLogProvider.getInstance();
    private File authFile;

    @BeforeEach
    void setUp()
    {
        authFile = new File( testDirectory.directory( "dbms" ), "auth" );
    }

    @Test
    void shouldStoreAndRetrieveUsersByName() throws Exception
    {
        // Given
        FileUserRepository users = new FileUserRepository( fs, authFile, logProvider );
        User user = new User.Builder( "jake", LegacyCredential.INACCESSIBLE ).withRequiredPasswordChange( true ).build();
        users.create( user );

        // When
        User result = users.getUserByName( user.name() );

        // Then
        assertThat( result, equalTo( user ) );
    }

    @Test
    void shouldPersistUsers() throws Throwable
    {
        // Given
        FileUserRepository users = new FileUserRepository( fs, authFile, logProvider );
        User user = new User.Builder( "jake", LegacyCredential.INACCESSIBLE ).withRequiredPasswordChange( true ).build();
        users.create( user );

        users = new FileUserRepository( fs, authFile, logProvider );
        users.start();

        // When
        User resultByName = users.getUserByName( user.name() );

        // Then
        assertThat( resultByName, equalTo( user ) );
    }

    @Test
    void shouldNotFindUserAfterDelete() throws Throwable
    {
        // Given
        FileUserRepository users = new FileUserRepository( fs, authFile, logProvider );
        User user = new User.Builder( "jake", LegacyCredential.INACCESSIBLE ).withRequiredPasswordChange( true ).build();
        users.create( user );

        // When
        users.delete( user );

        // Then
        assertThat( users.getUserByName( user.name() ), nullValue() );
    }

    @Test
    void shouldNotAllowComplexNames() throws Exception
    {
        // Given
        FileUserRepository users = new FileUserRepository( fs, authFile, logProvider );

        // When
        users.assertValidUsername( "neo4j" );
        users.assertValidUsername( "johnosbourne" );
        users.assertValidUsername( "john_osbourne" );

        assertException( () -> users.assertValidUsername( null ), InvalidArgumentsException.class,
                "The provided username is empty." );
        assertException( () -> users.assertValidUsername( "" ), InvalidArgumentsException.class,
                "The provided username is empty." );
        assertException( () -> users.assertValidUsername( "," ), InvalidArgumentsException.class,
                "Username ',' contains illegal characters. Use ascii characters that are not ',', ':' or whitespaces" +
                        "." );
        assertException( () -> users.assertValidUsername( "with space" ), InvalidArgumentsException.class,
                "Username 'with space' contains illegal characters. Use ascii characters that are not ',', ':' or " +
                        "whitespaces." );
        assertException( () -> users.assertValidUsername( "with:colon" ), InvalidArgumentsException.class,
                "Username 'with:colon' contains illegal characters. Use ascii characters that are not ',', ':' or " +
                        "whitespaces." );
        assertException( () -> users.assertValidUsername( "withå" ), InvalidArgumentsException.class,
                "Username 'withå' contains illegal characters. Use ascii characters that are not ',', ':' or " +
                        "whitespaces." );
    }

    @Test
    void shouldRecoverIfCrashedDuringMove() throws Throwable
    {
        // Given
        final IOException exception = new IOException( "simulated IO Exception on create" );
        FileSystemAbstraction crashingFileSystem =
            new DelegatingFileSystemAbstraction( fs )
            {
                @Override
                public void renameFile( File oldLocation, File newLocation, CopyOption... copyOptions ) throws IOException
                {
                    if ( authFile.getName().equals( newLocation.getName() ) )
                    {
                        throw exception;
                    }
                    super.renameFile( oldLocation, newLocation, copyOptions );
                }
            };

        FileUserRepository users = new FileUserRepository( crashingFileSystem, authFile, logProvider );
        users.start();
        User user = new User.Builder( "jake", LegacyCredential.INACCESSIBLE ).withRequiredPasswordChange( true ).build();

        // When
        var e = assertThrows( IOException.class, () -> users.create( user ) );
        assertSame( exception, e );

        // Then
        assertFalse( crashingFileSystem.fileExists( authFile ) );
        assertThat( crashingFileSystem.listFiles( authFile.getParentFile() ).length, equalTo( 0 ) );
    }

    @Test
    void shouldThrowIfUpdateChangesName() throws Throwable
    {
        // Given
        FileUserRepository users = new FileUserRepository( fs, authFile, logProvider );
        User user = new User.Builder( "jake", LegacyCredential.INACCESSIBLE ).withRequiredPasswordChange( true ).build();
        users.create( user );

        // When
        User updatedUser = new User.Builder( "john", LegacyCredential.INACCESSIBLE ).withRequiredPasswordChange( true )
                .build();
        var e = assertThrows( IllegalArgumentException.class, () -> users.update( user, updatedUser ) );
        assertThat( users.getUserByName( user.name() ), equalTo( user ) );
    }

    @Test
    void shouldThrowIfExistingUserDoesNotMatch() throws Throwable
    {
        // Given
        FileUserRepository users = new FileUserRepository( fs, authFile, logProvider );
        User user = new User.Builder( "jake", LegacyCredential.INACCESSIBLE ).withRequiredPasswordChange( true ).build();
        users.create( user );
        User modifiedUser = user.augment().withCredentials( LegacyCredential.forPassword( "foo" ) ).build();

        // When
        User updatedUser = user.augment().withCredentials( LegacyCredential.forPassword( "bar" ) ).build();
        var e = assertThrows( ConcurrentModificationException.class, () -> users.update( modifiedUser, updatedUser ) );
    }

    @Test
    void shouldFailOnReadingInvalidEntries() throws Throwable
    {
        // Given
        AssertableLogProvider logProvider = new AssertableLogProvider();
        fs.mkdir( authFile.getParentFile() );
        // First line is correctly formatted, second line has an extra field
        FileRepositorySerializer.writeToFile( fs, authFile, UTF8.encode(
                "admin:SHA-256,A42E541F276CF17036DB7818F8B09B1C229AAD52A17F69F4029617F3A554640F,FB7E8AE08A6A7C741F678AD22217808F:\n" +
                "neo4j:fc4c600b43ffe4d5857b4439c35df88f:SHA-256," +
                        "A42E541F276CF17036DB7818F8B09B1C229AAD52A17F69F4029617F3A554640F,FB7E8AE08A6A7C741F678AD22217808F:\n" ) );

        // When
        FileUserRepository users = new FileUserRepository( fs, authFile, logProvider );

        var e = assertThrows( IllegalStateException.class, () -> users.start() );
        assertThat( e.getMessage(), startsWith( "Failed to read authentication file: " ) );

        assertThat( users.numberOfUsers(), equalTo( 0 ) );
        logProvider.assertExactly(
                AssertableLogProvider.inLog( FileUserRepository.class ).error(
                        "Failed to read authentication file \"%s\" (%s)", authFile.getAbsolutePath(),
                        "wrong number of line fields, expected 3, got 4 [line 2]"
                )
        );
    }

    @Test
    void shouldProvideUserByUsernameEvenIfMidSetUsers() throws Throwable
    {
        // Given
        FileUserRepository users = new FileUserRepository( fs, authFile, logProvider );
        users.create( new User.Builder( "oskar", LegacyCredential.forPassword( "hidden" ) ).build() );
        DoubleLatch latch = new DoubleLatch( 2 );

        // When
        var executor = Executors.newSingleThreadExecutor();
        try
        {
            Future<?> setUsers = executor.submit( () ->
            {
                try
                {
                    users.setUsers( new HangingListSnapshot( latch, 10L, Collections.emptyList() ) );
                }
                catch ( InvalidArgumentsException e )
                {
                    throw new RuntimeException( e );
                }
            } );

            latch.startAndWaitForAllToStart();

            // Then
            assertNotNull( users.getUserByName( "oskar" ) );

            latch.finish();
            setUsers.get();
        }
        finally
        {
            executor.shutdown();
        }
    }

    static class HangingListSnapshot extends ListSnapshot<User>
    {
        private final DoubleLatch latch;

        HangingListSnapshot( DoubleLatch latch, long timestamp, List<User> values )
        {
            super( timestamp, values, true );
            this.latch = latch;
        }

        @Override
        public long timestamp()
        {
            latch.start();
            latch.finishAndWaitForAllToFinish();
            return super.timestamp();
        }
    }
}
