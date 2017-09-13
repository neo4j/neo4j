/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.nio.file.CopyOption;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;

import org.neo4j.graphdb.mockfs.DelegatingFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.impl.security.Credential;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.security.auth.exception.ConcurrentModificationException;
import org.neo4j.string.UTF8;
import org.neo4j.test.DoubleLatch;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.concurrent.ThreadingRule;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.neo4j.test.assertion.Assert.assertException;

public class FileUserRepositoryTest
{
    private File authFile;
    private final LogProvider logProvider = NullLogProvider.getInstance();
    private FileSystemAbstraction fs;

    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();
    @Rule
    public final ExpectedException thrown = ExpectedException.none();
    @Rule
    public final ThreadingRule threading = new ThreadingRule();
    @Rule
    public final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

    @Before
    public void setUp()
    {
        fs = fileSystemRule.get();
        authFile = new File( testDirectory.directory( "dbms" ), "auth" );
    }

    @Test
    public void shouldStoreAndRetriveUsersByName() throws Exception
    {
        // Given
        FileUserRepository users = new FileUserRepository( fs, authFile, logProvider );
        User user = new User.Builder( "jake", Credential.INACCESSIBLE ).withRequiredPasswordChange( true ).build();
        users.create( user );

        // When
        User result = users.getUserByName( user.name() );

        // Then
        assertThat( result, equalTo( user ) );
    }

    @Test
    public void shouldPersistUsers() throws Throwable
    {
        // Given
        FileUserRepository users = new FileUserRepository( fs, authFile, logProvider );
        User user = new User.Builder( "jake", Credential.INACCESSIBLE ).withRequiredPasswordChange( true ).build();
        users.create( user );

        users = new FileUserRepository( fs, authFile, logProvider );
        users.start();

        // When
        User resultByName = users.getUserByName( user.name() );

        // Then
        assertThat( resultByName, equalTo( user ) );
    }

    @Test
    public void shouldNotFindUserAfterDelete() throws Throwable
    {
        // Given
        FileUserRepository users = new FileUserRepository( fs, authFile, logProvider );
        User user = new User.Builder( "jake", Credential.INACCESSIBLE ).withRequiredPasswordChange( true ).build();
        users.create( user );

        // When
        users.delete( user );

        // Then
        assertThat( users.getUserByName( user.name() ), nullValue() );
    }

    @Test
    public void shouldNotAllowComplexNames() throws Exception
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
    public void shouldRecoverIfCrashedDuringMove() throws Throwable
    {
        // Given
        final IOException exception = new IOException( "simulated IO Exception on create" );
        FileSystemAbstraction craschingFileSystem =
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

        FileUserRepository users = new FileUserRepository( craschingFileSystem, authFile, logProvider );
        users.start();
        User user = new User.Builder( "jake", Credential.INACCESSIBLE ).withRequiredPasswordChange( true ).build();

        // When
        try
        {
            users.create( user );
            fail( "Expected an IOException" );
        }
        catch ( IOException e )
        {
            assertSame( exception, e );
        }

        // Then
        assertFalse( craschingFileSystem.fileExists( authFile ) );
        assertThat( craschingFileSystem.listFiles( authFile.getParentFile() ).length, equalTo( 0 ) );
    }

    @Test
    public void shouldThrowIfUpdateChangesName() throws Throwable
    {
        // Given
        FileUserRepository users = new FileUserRepository( fs, authFile, logProvider );
        User user = new User.Builder( "jake", Credential.INACCESSIBLE ).withRequiredPasswordChange( true ).build();
        users.create( user );

        // When
        User updatedUser = new User.Builder( "john", Credential.INACCESSIBLE ).withRequiredPasswordChange( true )
                .build();
        try
        {
            users.update( user, updatedUser );
            fail( "expected exception not thrown" );
        }
        catch ( IllegalArgumentException e )
        {
            // Then continue
        }

        assertThat( users.getUserByName( user.name() ), equalTo( user ) );
    }

    @Test
    public void shouldThrowIfExistingUserDoesNotMatch() throws Throwable
    {
        // Given
        FileUserRepository users = new FileUserRepository( fs, authFile, logProvider );
        User user = new User.Builder( "jake", Credential.INACCESSIBLE ).withRequiredPasswordChange( true ).build();
        users.create( user );
        User modifiedUser = user.augment().withCredentials( Credential.forPassword( "foo" ) ).build();

        // When
        User updatedUser = user.augment().withCredentials( Credential.forPassword( "bar" ) ).build();
        try
        {
            users.update( modifiedUser, updatedUser );
            fail( "expected exception not thrown" );
        }
        catch ( ConcurrentModificationException e )
        {
            // Then continue
        }
    }

    @Test
    public void shouldFailOnReadingInvalidEntries() throws Throwable
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

        thrown.expect( IllegalStateException.class );
        thrown.expectMessage( startsWith( "Failed to read authentication file: " ) );

        try
        {
            users.start();
        }
        // Then
        catch ( IllegalStateException e )
        {
            assertThat( users.numberOfUsers(), equalTo( 0 ) );
            logProvider.assertExactly(
                    AssertableLogProvider.inLog( FileUserRepository.class ).error(
                            "Failed to read authentication file \"%s\" (%s)", authFile.getAbsolutePath(),
                            "wrong number of line fields, expected 3, got 4 [line 2]"
                    )
            );
            throw e;
        }
    }

    @Test
    public void shouldProvideUserByUsernameEvenIfMidSetUsers() throws Throwable
    {
        // Given
        FileUserRepository users = new FileUserRepository( fs, authFile, logProvider );
        users.create( new User.Builder( "oskar", Credential.forPassword( "hidden" ) ).build() );
        DoubleLatch latch = new DoubleLatch( 2 );

        // When
        Future<Object> setUsers = threading.execute( o ->
            {
                users.setUsers( new HangingListSnapshot( latch, 10L, Collections.emptyList() ) );
                return null;
            }, null );

        latch.startAndWaitForAllToStart();

        // Then
        assertNotNull( users.getUserByName( "oskar" ) );

        latch.finish();
        setUsers.get();
    }

    class HangingListSnapshot extends ListSnapshot<User>
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
