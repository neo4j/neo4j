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
package org.neo4j.kernel.impl.util;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TargetDirectory.TestDirectory;

import static org.junit.Assert.assertArrayEquals;

import static org.neo4j.kernel.impl.util.Converters.regexFiles;

public class ConvertersTest
{
    public final @Rule TestDirectory directory = TargetDirectory.testDirForTest( getClass() );

    @Test
    public void shouldSortFilesByNumberCleverly() throws Exception
    {
        // GIVEN
        File file1 = existenceOfFile( "file1" );
        File file123 = existenceOfFile( "file123" );
        File file12 = existenceOfFile( "file12" );
        File file2 = existenceOfFile( "file2" );
        File file32 = existenceOfFile( "file32" );

        // WHEN
        File[] files = regexFiles( true ).apply( directory.file( "file.*" ).getAbsolutePath() );

        // THEN
        assertArrayEquals( new File[] {file1, file2, file12, file32, file123}, files );
    }

    private File existenceOfFile( String name ) throws IOException
    {
        File file = directory.file( name );
        file.createNewFile();
        return file;
    }
}
