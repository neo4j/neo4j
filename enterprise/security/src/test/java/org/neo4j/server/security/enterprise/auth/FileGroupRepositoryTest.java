/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.security.enterprise.auth;

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
public class FileGroupRepositoryTest
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

    public FileGroupRepositoryTest( Configuration fsConfig, String fsType )
    {
        fs = Jimfs.newFileSystem( fsConfig );
        authFile = fs.getPath( "dbms", "auth.db" );
    }

    @Test
    public void shouldStoreAndRetriveGroupsByName() throws Exception
    {
        // Given
        FileGroupRepository groups = new FileGroupRepository( authFile, NullLogProvider.getInstance() );
        GroupRecord group = new GroupRecord( "admin", "petra", "olivia" );
        groups.create( group );

        // When
        GroupRecord result = groups.findByName( group.name() );

        // Then
        assertThat( result, equalTo( group ) );
    }

    @Test
    public void shouldPersistGroups() throws Throwable
    {
        // Given
        FileGroupRepository groups = new FileGroupRepository( authFile, NullLogProvider.getInstance() );
        GroupRecord group = new GroupRecord( "admin", "craig", "karl" );
        groups.create( group );

        groups = new FileGroupRepository( authFile, NullLogProvider.getInstance() );
        groups.start();

        // When
        GroupRecord resultByName = groups.findByName( group.name() );

        // Then
        assertThat( resultByName, equalTo( group ) );
    }

    @Test
    public void shouldNotFindGroupAfterDelete() throws Throwable
    {
        // Given
        FileGroupRepository groups = new FileGroupRepository( authFile, NullLogProvider.getInstance() );
        GroupRecord group = new GroupRecord( "jake", "admin" );
        groups.create( group );

        // When
        groups.delete( group );

        // Then
        assertThat( groups.findByName( group.name() ), nullValue() );
    }

    @Test
    public void shouldNotAllowComplexNames() throws Exception
    {
        // Given
        FileGroupRepository groups = new FileGroupRepository( authFile, NullLogProvider.getInstance() );

        // When
        assertTrue( groups.isValidName( "neo4j" ) );
        assertTrue( groups.isValidName( "johnosbourne" ) );
        assertTrue( groups.isValidName( "john_osbourne" ) );

        assertFalse( groups.isValidName( ":" ) );
        assertFalse( groups.isValidName( "" ) );
        assertFalse( groups.isValidName( "john osbourne" ) );
        assertFalse( groups.isValidName( "john:osbourne" ) );
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

        FileGroupRepository groups = new FileGroupRepository( authFile, NullLogProvider.getInstance() );
        groups.start();
        GroupRecord group = new GroupRecord( "admin", "jake" );

        // When
        try
        {
            groups.create( group );
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
        FileGroupRepository groups = new FileGroupRepository( authFile, NullLogProvider.getInstance() );
        GroupRecord group = new GroupRecord( "admin", "steve", "bob" );
        groups.create( group );

        // When
        GroupRecord updatedGroup = new GroupRecord( "admins", "steve", "bob" );
        try
        {
            groups.update( group, updatedGroup );
            fail( "expected exception not thrown" );
        } catch ( IllegalArgumentException e )
        {
            // Then continue
        }

        assertThat( groups.findByName( group.name() ), equalTo( group ) );
    }

    @Test
    public void shouldThrowIfExistingGroupDoesNotMatch() throws Throwable
    {
        // Given
        FileGroupRepository groups = new FileGroupRepository( authFile, NullLogProvider.getInstance() );
        GroupRecord group = new GroupRecord( "admin", "jake" );
        groups.create( group );
        GroupRecord modifiedGroup = new GroupRecord( "admin", "jake", "john" );

        // When
        GroupRecord updatedGroup = new GroupRecord( "admin", "john" );
        try
        {
            groups.update( modifiedGroup, updatedGroup );
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
        Files.write( authFile, UTF8.encode(
                "neo4j:admin:\n" +
                "admin:admin:\n" ) );

        // When
        FileGroupRepository groups = new FileGroupRepository( authFile, logProvider );
        thrown.expect( IllegalStateException.class );
        thrown.expectMessage( startsWith( "Failed to read group file: " ) );
        groups.start();

        // Then
        assertThat( groups.numberOfGroups(), equalTo( 1 ) );
        logProvider.assertExactly(
                AssertableLogProvider.inLog( FileGroupRepository.class ).error(
                        "Ignoring group file \"%s\" (%s)", authFile.toAbsolutePath(), "wrong number of line fields [line 1]"
                )
        );
    }
}
