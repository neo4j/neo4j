/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.server.security.enterprise.auth;

import org.junit.After;
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
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.security.auth.FileRepositorySerializer;
import org.neo4j.server.security.auth.ListSnapshot;
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
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.test.assertion.Assert.assertException;

public class FileRoleRepositoryTest
{
    private File roleFile;
    private final LogProvider logProvider = NullLogProvider.getInstance();
    private FileSystemAbstraction fs;
    private RoleRepository roleRepository;

    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();
    @Rule
    public final ExpectedException thrown = ExpectedException.none();
    @Rule
    public final ThreadingRule threading = new ThreadingRule();
    @Rule
    public final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

    @Before
    public void setup()
    {
        fs = fileSystemRule.get();
        roleFile = new File( testDirectory.directory( "dbms" ), "roles" );
        roleRepository = new FileRoleRepository( fs, roleFile, logProvider );
    }

    @After
    public void tearDown() throws IOException
    {
        fs.close();
    }

    @Test
    public void shouldStoreAndRetrieveRolesByName() throws Exception
    {
        // Given
        RoleRecord role = new RoleRecord( "admin", "petra", "olivia" );
        roleRepository.create( role );

        // When
        RoleRecord result = roleRepository.getRoleByName( role.name() );

        // Then
        assertThat( result, equalTo( role ) );
    }

    @Test
    public void shouldPersistRoles() throws Throwable
    {
        // Given
        RoleRecord role = new RoleRecord( "admin", "craig", "karl" );
        roleRepository.create( role );

        roleRepository = new FileRoleRepository( fs, roleFile, logProvider );
        roleRepository.start();

        // When
        RoleRecord resultByName = roleRepository.getRoleByName( role.name() );

        // Then
        assertThat( resultByName, equalTo( role ) );
    }

    @Test
    public void shouldNotFindRoleAfterDelete() throws Throwable
    {
        // Given
        RoleRecord role = new RoleRecord( "jake", "admin" );
        roleRepository.create( role );

        // When
        roleRepository.delete( role );

        // Then
        assertThat( roleRepository.getRoleByName( role.name() ), nullValue() );
    }

    @Test
    public void shouldNotAllowComplexNames() throws Exception
    {
        // Given

        // When
        roleRepository.assertValidRoleName( "neo4j" );
        roleRepository.assertValidRoleName( "johnosbourne" );
        roleRepository.assertValidRoleName( "john_osbourne" );

        assertException( () -> roleRepository.assertValidRoleName( null ), InvalidArgumentsException.class,
                "The provided role name is empty." );
        assertException( () -> roleRepository.assertValidRoleName( "" ), InvalidArgumentsException.class,
                "The provided role name is empty." );
        assertException( () -> roleRepository.assertValidRoleName( ":" ), InvalidArgumentsException.class,
                "Role name ':' contains illegal characters. Use simple ascii characters and numbers." );
        assertException( () -> roleRepository.assertValidRoleName( "john osbourne" ), InvalidArgumentsException.class,
                "Role name 'john osbourne' contains illegal characters. Use simple ascii characters and numbers." );
        assertException( () -> roleRepository.assertValidRoleName( "john:osbourne" ), InvalidArgumentsException.class,
                "Role name 'john:osbourne' contains illegal characters. Use simple ascii characters and numbers." );
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
                    public void renameFile( File oldLocation, File newLocation, CopyOption... copyOptions ) throws
                            IOException
                    {
                        if ( roleFile.getName().equals( newLocation.getName() ) )
                        {
                            throw exception;
                        }
                        super.renameFile( oldLocation, newLocation, copyOptions );
                    }
                };

        roleRepository = new FileRoleRepository( craschingFileSystem, roleFile, logProvider );
        roleRepository.start();
        RoleRecord role = new RoleRecord( "admin", "jake" );

        // When
        try
        {
            roleRepository.create( role );
            fail( "Expected an IOException" );
        }
        catch ( IOException e )
        {
            assertSame( exception, e );
        }

        // Then
        assertFalse( craschingFileSystem.fileExists( roleFile ) );
        assertThat( craschingFileSystem.listFiles( roleFile.getParentFile() ).length, equalTo( 0 ) );
    }

    @Test
    public void shouldThrowIfUpdateChangesName() throws Throwable
    {
        // Given
        RoleRecord role = new RoleRecord( "admin", "steve", "bob" );
        roleRepository.create( role );

        // When
        RoleRecord updatedRole = new RoleRecord( "admins", "steve", "bob" );
        try
        {
            roleRepository.update( role, updatedRole );
            fail( "expected exception not thrown" );
        }
        catch ( IllegalArgumentException e )
        {
            // Then continue
        }

        assertThat( roleRepository.getRoleByName( role.name() ), equalTo( role ) );
    }

    @Test
    public void shouldThrowIfExistingRoleDoesNotMatch() throws Throwable
    {
        // Given
        RoleRecord role = new RoleRecord( "admin", "jake" );
        roleRepository.create( role );
        RoleRecord modifiedRole = new RoleRecord( "admin", "jake", "john" );

        // When
        RoleRecord updatedRole = new RoleRecord( "admin", "john" );
        try
        {
            roleRepository.update( modifiedRole, updatedRole );
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

        fs.mkdirs( roleFile.getParentFile() );
        // First line is correctly formatted, second line has an extra field
        FileRepositorySerializer.writeToFile( fs, roleFile, UTF8.encode(
                "neo4j:admin\n" +
                "admin:admin:\n" ) );

        // When
        roleRepository = new FileRoleRepository( fs, roleFile, logProvider );

        thrown.expect( IllegalStateException.class );
        thrown.expectMessage( startsWith( "Failed to read role file '" ) );

        try
        {
            roleRepository.start();
        }
        // Then
        catch ( IllegalStateException e )
        {
            assertThat( roleRepository.numberOfRoles(), equalTo( 0 ) );
            logProvider.assertExactly(
                    AssertableLogProvider.inLog( FileRoleRepository.class ).error(
                            "Failed to read role file \"%s\" (%s)", roleFile.getAbsolutePath(),
                            "wrong number of line fields [line 2]"
                    )
            );
            throw e;
        }
    }

    @Test
    public void shouldNotAddEmptyUserToRole() throws Throwable
    {
        // Given
        fs.mkdirs( roleFile.getParentFile() );
        FileRepositorySerializer.writeToFile( fs, roleFile, UTF8.encode( "admin:neo4j\nreader:\n" ) );

        // When
        roleRepository = new FileRoleRepository( fs, roleFile, logProvider );
        roleRepository.start();

        RoleRecord role = roleRepository.getRoleByName( "admin" );
        assertTrue( "neo4j should be assigned to 'admin'", role.users().contains( "neo4j" ) );
        assertTrue( "only one admin should exist", role.users().size() == 1 );

        role = roleRepository.getRoleByName( "reader" );
        assertTrue( "no users should be assigned to 'reader'", role.users().isEmpty() );
    }

    @Test
    public void shouldProvideRolesByUsernameEvenIfMidSetRoles() throws Throwable
    {
        // Given
        roleRepository = new FileRoleRepository( fs, roleFile, logProvider );
        roleRepository.create( new RoleRecord( "admin", "oskar" ) );
        DoubleLatch latch = new DoubleLatch( 2 );

        // When
        Future<Object> setUsers = threading.execute( o ->
        {
            roleRepository.setRoles( new HangingListSnapshot( latch, 10L, Collections.emptyList() ) );
            return null;
        }, null );

        latch.startAndWaitForAllToStart();

        // Then
        assertThat( roleRepository.getRoleNamesByUsername( "oskar" ), containsInAnyOrder( "admin" ) );

        latch.finish();
        setUsers.get();
    }

    class HangingListSnapshot extends ListSnapshot<RoleRecord>
    {
        private final DoubleLatch latch;

        HangingListSnapshot( DoubleLatch latch, long timestamp, List<RoleRecord> values )
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
