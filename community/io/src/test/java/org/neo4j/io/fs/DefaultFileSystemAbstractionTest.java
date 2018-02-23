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

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import static java.lang.String.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.io.fs.DefaultFileSystemAbstraction.UNABLE_TO_CREATE_DIRECTORY_FORMAT;

public class DefaultFileSystemAbstractionTest extends FileSystemAbstractionTest
{
    @Override
    protected FileSystemAbstraction buildFileSystemAbstraction()
    {
        return new DefaultFileSystemAbstraction();
    }

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

            fail("Failure was expected");
        }
        catch ( IOException e )
        {
            assertFalse( fsa.fileExists( path ) );
            String expectedMessage = format( UNABLE_TO_CREATE_DIRECTORY_FORMAT, path );
            assertThat( e.getMessage(), is( expectedMessage ) );
        }
    }
}
