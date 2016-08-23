/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.nio.file.CopyOption;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.spi.FileSystemProvider;
import java.util.Arrays;
import java.util.Collection;

import org.neo4j.io.fs.DelegatingFileSystem;
import org.neo4j.io.fs.DelegatingFileSystemProvider;
import org.neo4j.kernel.api.security.exception.InvalidArgumentsException;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.security.auth.exception.ConcurrentModificationException;
import org.neo4j.string.UTF8;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class FileUserRepositoryTest
{
    private final FileSystem fs;
    private Path authFile;

    @Parameters(name = "{1} filesystem")
    public static Collection<Object[]> data()
    {
        return Arrays.asList( new Object[][]{
                        {Configuration.unix(), "unix"},
                        {Configuration.osX(), "osX"},
                        {Configuration.windows(), "windows"}}
        );
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    public FileUserRepositoryTest( Configuration fsConfig, String fsType )
    {
        fs = Jimfs.newFileSystem( fsConfig );
        authFile = fs.getPath( "dbms", "auth.db" );
    }

    @Test
    public void shouldStoreAndRetriveUsersByName() throws Exception
    {
        // Given
        FileUserRepository users = new FileUserRepository( authFile, NullLogProvider.getInstance() );
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
        FileUserRepository users = new FileUserRepository( authFile, NullLogProvider.getInstance() );
        User user = new User.Builder( "jake", Credential.INACCESSIBLE ).withRequiredPasswordChange( true ).build();
        users.create( user );

        users = new FileUserRepository( authFile, NullLogProvider.getInstance() );
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
        FileUserRepository users = new FileUserRepository( authFile, NullLogProvider.getInstance() );
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
        FileUserRepository users = new FileUserRepository( authFile, NullLogProvider.getInstance() );

        // When
        assertTrue( users.isValidUsername( "neo4j" ) );
        assertTrue( users.isValidUsername( "johnosbourne" ) );
        assertTrue( users.isValidUsername( "john_osbourne" ) );

        assertFalse( users.isValidUsername( ":" ) );
        assertFalse( users.isValidUsername( "" ) );
        assertFalse( users.isValidUsername( "john osbourne" ) );
        assertFalse( users.isValidUsername( "john:osbourne" ) );
    }

    @Test
    public void shouldRecoverIfCrashedDuringMove() throws Throwable
    {
        // Given
        final IOException exception = new IOException( "simulated IO Exception on create" );
        FileSystem moveFailingFileSystem = new DelegatingFileSystem( fs )
        {
            @Override
            protected DelegatingFileSystemProvider createDelegate( FileSystemProvider provider )
            {
                return new WrappedProvider( provider, this )
                {
                    @Override
                    public void move( Path source, Path target, CopyOption... options ) throws IOException
                    {
                        if ( authFile.getFileName().toString().equals( target.getFileName().toString() ) )
                        {
                            throw exception;
                        }
                        super.move( source, target, options );
                    }
                };
            }
        };

        Path authFile = moveFailingFileSystem.getPath( "dbms", "auth.db" );

        FileUserRepository users = new FileUserRepository( authFile, NullLogProvider.getInstance() );
        users.start();
        User user = new User.Builder( "jake", Credential.INACCESSIBLE ).withRequiredPasswordChange( true ).build();

        // When
        try
        {
            users.create( user );
            fail( "Expected an IOException" );
        } catch ( IOException e )
        {
            assertSame( exception, e );
        }

        // Then
        assertFalse( Files.exists( authFile ) );
        assertFalse( Files.newDirectoryStream( authFile.getParent() ).iterator().hasNext() );
    }

    @Test
    public void shouldThrowIfUpdateChangesName() throws Throwable
    {
        // Given
        FileUserRepository users = new FileUserRepository( authFile, NullLogProvider.getInstance() );
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
        FileUserRepository users = new FileUserRepository( authFile, NullLogProvider.getInstance() );
        User user = new User.Builder( "jake", Credential.INACCESSIBLE ).withRequiredPasswordChange( true ).build();
        users.create( user );
        User modifiedUser = user.augment().withCredentials( Credential.forPassword( "foo" ) ).build();

        // When
        User updatedUser = user.augment().withCredentials( Credential.forPassword( "bar" ) ).build();
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
    public void shouldFailOnReadingInvalidEntries() throws Throwable
    {
        // Given
        AssertableLogProvider logProvider = new AssertableLogProvider();
        Files.createDirectories( authFile.getParent() );
        // First line is correctly formatted, second line has an extra field
        Files.write( authFile, UTF8.encode(
                "admin:SHA-256,A42E541F276CF17036DB7818F8B09B1C229AAD52A17F69F4029617F3A554640F,FB7E8AE08A6A7C741F678AD22217808F:\n" +
                "neo4j:fc4c600b43ffe4d5857b4439c35df88f:SHA-256," +
                        "A42E541F276CF17036DB7818F8B09B1C229AAD52A17F69F4029617F3A554640F,FB7E8AE08A6A7C741F678AD22217808F:\n" ) );

        // When
        FileUserRepository users = new FileUserRepository( authFile, logProvider );

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
                            "Failed to read authentication file \"%s\" (%s)", authFile.toAbsolutePath(),
                            "wrong number of line fields, expected 3, got 4 [line 2]"
                    )
            );
            throw e;
        }
    }
}
