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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.FileUtils;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestFileUtils
{
    @Rule
    public TargetDirectory.TestDirectory testDirectory = TargetDirectory.testDirForTest( getClass() );

    private File path;

    @Before
    public void doBefore() throws Exception
    {
        path = testDirectory.directory( "path" );
    }

    @Test
    public void moveFileToDirectory() throws Exception
    {
        File file = touchFile( "source" );
        File targetDir = directory( "dir" );

        File newLocationOfFile = FileUtils.moveFileToDirectory( file, targetDir );
        assertTrue( newLocationOfFile.exists() );
        assertFalse( file.exists() );
        assertEquals( newLocationOfFile, targetDir.listFiles()[0] );
    }

    @Test
    public void moveFile() throws Exception
    {
        File file = touchFile( "source" );
        File targetDir = directory( "dir" );

        File newLocationOfFile = new File( targetDir, "new-name" );
        FileUtils.moveFile( file, newLocationOfFile );
        assertTrue( newLocationOfFile.exists() );
        assertFalse( file.exists() );
        assertEquals( newLocationOfFile, targetDir.listFiles()[0] );
    }

    private File directory( String name ) throws IOException
    {
        File dir = new File( path, name );
        dir.mkdirs();
        return dir;
    }

    private File touchFile( String name ) throws IOException
    {
        File file = new File( path, name );
        file.createNewFile();
        return file;
    }
}
