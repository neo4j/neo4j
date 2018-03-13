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
package org.neo4j.io.fs;

import org.apache.commons.lang3.SystemUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.UUID;

import static java.lang.String.format;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;
import static org.neo4j.io.fs.DefaultFileSystemAbstraction.UNABLE_TO_CREATE_DIRECTORY_FORMAT;

public class DefaultFileSystemAbstractionTest extends FileSystemAbstractionTest
{
    @Override
    protected FileSystemAbstraction buildFileSystemAbstraction()
    {
        return new DefaultFileSystemAbstraction();
    }

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldFailGracefullyWhenPathCannotBeCreated()
    {
        path = new File( testDirectory.directory(), String.valueOf( UUID.randomUUID() ) )
        {
            @Override
            public boolean mkdirs()
            {
                return false;
            }
        };

        try
        {
            fsa.mkdirs( path );

            fail();
        }
        catch ( IOException e )
        {
            assertFalse( fsa.fileExists( path ) );
            String expectedMessage = format( UNABLE_TO_CREATE_DIRECTORY_FORMAT, path );
            assertThat( e.getMessage(), is( expectedMessage ) );
        }
    }

    @Test
    public void shouldAllowSettingFilePermissionsIfRunningOnPOSIX() throws Exception
    {
        // Given
        assumeTrue( SystemUtils.IS_OS_UNIX );

        // Note that we re-use the file intentionally, to test that the method overwrites
        // any existing permissions
        File path = new File( testDirectory.directory(), String.valueOf( UUID.randomUUID() ) );
        fsa.mkdirs( testDirectory.directory() );
        fsa.create( path ).close();

        for ( FilePermission permission : FilePermission.values() )
        {
            // When
            fsa.setPermissions( path, permission );

            // Then
            assertEquals( fsa.getPermissions( path ), new HashSet<>( Collections.singletonList( permission ) ) );
        }
    }

    @Test
    public void shouldShoutAndScreamIfSettingPermissionsOnSystemWithoutSupportForIt() throws Exception
    {
        // Given
        assumeFalse( SystemUtils.IS_OS_UNIX );
        File path = new File( testDirectory.directory(), String.valueOf( UUID.randomUUID() ) );
        fsa.mkdirs( testDirectory.directory() );
        fsa.create( path ).close();

        // Expect
        exception.expect( IOException.class );

        // When
        fsa.setPermissions( path, FilePermission.GROUP_EXECUTE );
    }
}
