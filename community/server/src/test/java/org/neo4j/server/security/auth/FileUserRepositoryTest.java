/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import org.junit.Test;

import java.io.IOException;
import java.nio.file.CopyOption;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.spi.FileSystemProvider;
import java.util.Arrays;
import java.util.Collection;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.neo4j.io.fs.DelegatingFileSystem;
import org.neo4j.io.fs.DelegatingFileSystemProvider;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.kernel.impl.util.Charsets;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.security.auth.exception.ConcurrentModificationException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.logging.AssertableLogProvider.inLog;

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
        User user = new User( "jake", Credential.INACCESSIBLE, true );
        users.create( user );

        // When
        User result = users.findByName( user.name() );

        // Then
        assertThat( result, equalTo( user ) );
    }

    @Test
    public void shouldPersistUsers() throws Throwable
    {
        // Given
        FileUserRepository users = new FileUserRepository( authFile, NullLogProvider.getInstance() );
        User user = new User( "jake", Credential.INACCESSIBLE, true );
        users.create( user );

        users = new FileUserRepository( authFile, NullLogProvider.getInstance() );
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
        FileUserRepository users = new FileUserRepository( authFile, NullLogProvider.getInstance() );
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
        FileUserRepository users = new FileUserRepository( authFile, NullLogProvider.getInstance() );

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
        User user = new User( "jake", Credential.INACCESSIBLE, true );

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
        FileUserRepository users = new FileUserRepository( authFile, NullLogProvider.getInstance() );
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
        AssertableLogProvider logProvider = new AssertableLogProvider();
        Files.createDirectories( authFile.getParent() );
        Files.write( authFile,
                "neo4j:fc4c600b43ffe4d5857b4439c35df88f:SHA-256,A42E541F276CF17036DB7818F8B09B1C229AAD52A17F69F4029617F3A554640F,FB7E8AE08A6A7C741F678AD22217808F:\n".getBytes( Charsets.UTF_8 ) );

        // When
        FileUserRepository users = new FileUserRepository( authFile, logProvider );
        users.start();

        // Then
        assertThat( users.numberOfUsers(), equalTo( 0 ) );
        logProvider.assertExactly(
                inLog( FileUserRepository.class ).error(
                        "Ignoring authorization file \"%s\" (%s)", authFile.toAbsolutePath(), "wrong number of line fields [line 1]"
                )
        );
    }
}
