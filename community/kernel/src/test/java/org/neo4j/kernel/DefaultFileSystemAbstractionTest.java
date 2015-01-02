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
package org.neo4j.kernel;

import static java.lang.String.format;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.DefaultFileSystemAbstraction.UNABLE_TO_CREATE_DIRECTORY_FORMAT;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

public class DefaultFileSystemAbstractionTest
{
    private final DefaultFileSystemAbstraction defaultFileSystemAbstraction = new DefaultFileSystemAbstraction();

    private File path;

    @Before
    public void before() throws Exception
    {
        path = new File( "target/" + UUID.randomUUID() );
    }

    @Test
    public void shouldCreatePath() throws Exception
    {
        defaultFileSystemAbstraction.mkdirs( path );

        assertThat( path.exists(), is( true ) );
    }

    @Test
    public void shouldCreateDeepPath() throws Exception
    {
        path = new File( path, UUID.randomUUID() + "/" + UUID.randomUUID() );

        defaultFileSystemAbstraction.mkdirs( path );

        assertThat( path.exists(), is( true ) );
    }

    @Test
    public void shouldCreatePathThatAlreadyExists() throws Exception
    {
        assertTrue( path.mkdir() );

        defaultFileSystemAbstraction.mkdirs( path );

        assertThat( path.exists(), is( true ) );
    }

    @Test
    public void shouldCreatePathThatPointsToFile() throws Exception
    {
        assertTrue( path.mkdir() );
        path = new File( path, "some_file" );
        assertTrue( path.createNewFile() );

        defaultFileSystemAbstraction.mkdirs( path );

        assertThat( path.exists(), is( true ) );
    }

    @Test
    public void shouldFailGracefullyWhenPathCannotBeCreated() throws Exception
    {
        path = new File( "target/" + UUID.randomUUID() )
        {
            @Override
            public boolean mkdirs()
            {
                return false;
            }
        };

        try
        {
            defaultFileSystemAbstraction.mkdirs( path );

            fail();
        }
        catch ( IOException e )
        {
            assertThat( path.exists(), is( false ) );
            assertThat( e.getMessage(), is( format( UNABLE_TO_CREATE_DIRECTORY_FORMAT, path ) ) );
        }
    }
}