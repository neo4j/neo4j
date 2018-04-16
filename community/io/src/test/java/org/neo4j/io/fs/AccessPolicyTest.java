/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.io.fs;

import org.apache.commons.lang3.SystemUtils;
import org.hamcrest.collection.IsEmptyCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.AclEntry;
import java.nio.file.attribute.AclFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.UserPrincipal;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;

public class AccessPolicyTest extends FileSystemAbstractionTest
{
    @Override
    protected FileSystemAbstraction buildFileSystemAbstraction()
    {
        return new DefaultFileSystemAbstraction();
    }

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldAllowSettingFilePermissionsIfRunningOnPOSIX() throws Exception
    {
        // Note that we re-use the file intentionally, to test that the method overwrites
        // any existing permissions
        File path = new File( testDirectory.directory(), String.valueOf( UUID.randomUUID() ) );
        fsa.mkdirs( testDirectory.directory() );
        fsa.create( path ).close();

        // When
        fsa.setAccessPolicy( path, AccessPolicy.CRITICAL );

        // Then
        if ( SystemUtils.IS_OS_UNIX )
        {
            Set<PosixFilePermission> perms = Files.getPosixFilePermissions( path.toPath() );
            assertThat( perms, containsInAnyOrder( PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_READ ) );
        }
        else if ( SystemUtils.IS_OS_WINDOWS )
        {
            // On windows, there should be no permissions for anyone but the owner
            AclFileAttributeView attrs = Files.getFileAttributeView( path.toPath(), AclFileAttributeView.class );
            UserPrincipal owner = attrs.getOwner();
            List<AclEntry> nonOwnerAcl = attrs.getAcl().stream().filter( a -> !a.principal().equals( owner ) ).collect(
                    Collectors.toList() );
            assertThat( nonOwnerAcl, IsEmptyCollection.empty() );

            // But the owner - we - should be able to read it
            assertEquals( path.canRead(), true );
            assertEquals( path.canWrite(), true );
        }
    }

    public static void main( String... argv ) throws IOException
    {
        File f = new File( "/tmp/" + String.valueOf( UUID.randomUUID() ) );
        if ( !f.createNewFile() )
        {
            System.exit( 1 );
        }

        // Make unreadable for everyone
        if ( !f.setReadable( false ) )
        {
            System.exit( 2 );
        }

        // Ensure they are as expected
        System.out.println( Files.getPosixFilePermissions( f.toPath() ) );
    }
}
