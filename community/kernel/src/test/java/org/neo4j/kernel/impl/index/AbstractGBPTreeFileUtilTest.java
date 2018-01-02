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
package org.neo4j.kernel.impl.index;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * This test is inherited on blockdevice to test that behaviour is unified between product and blockdevice.
 */
public abstract class AbstractGBPTreeFileUtilTest
{
    private GBPTreeFileUtil fileUtil;
    private File existingFile;
    private File nonExistingFile;
    private File nonExistingDirectory;

    @Before
    public void setup() throws IOException
    {
        this.fileUtil = getGBPTreeFileUtil();
        this.existingFile = existingFile( "existing_file" );
        this.nonExistingFile = nonExistingFile( "non_existing_file" );
        this.nonExistingDirectory = nonExistingDirectory( "non_existing_directory" );
    }

    @After
    public void cleanUp()
    {
    }

    protected abstract GBPTreeFileUtil getGBPTreeFileUtil();

    protected abstract File existingFile( String fileName ) throws IOException;

    protected abstract File nonExistingFile( String fileName );

    protected abstract File nonExistingDirectory( String directoryName );

    protected abstract void assertFileDoesNotExist( File file );

    protected abstract void assertDirectoryExist( File directory );

    /* deleteFile( File storeFile ) */

    @Test
    public void fileMustNotExistAfterDeleteFile() throws Exception
    {
        // given
        // when
        fileUtil.deleteFile( existingFile );

        // then
        assertFileDoesNotExist( existingFile );
    }

    @Test
    public void deleteFileMustThrowIfFileIsMissing() throws Exception
    {
        // given
        // when
        try
        {
            fileUtil.deleteFile( nonExistingFile );
            fail( "Should have failed" );
        }
        catch ( NoSuchFileException e )
        {
            // then
        }
    }

    /* deleteIfPresent( File storeFile ) */

    @Test
    public void deleteFileIfPresentMustDeleteFileIfPresent() throws Exception
    {
        // given
        // when
        fileUtil.deleteFileIfPresent( existingFile );

        // then
        assertFileDoesNotExist( existingFile );
    }

    @Test
    public void deleteFileIfPresentMustNotThrowIfFileIsMissing() throws Exception
    {
        // given
        // when
        fileUtil.deleteFileIfPresent( nonExistingFile );

        // then
        // this should be fine
    }

    /* boolean storeFileExists( File storeFile ) */

    @Test
    public void storeFileExistsMustReturnTrueForExistingFile() throws Exception
    {
        assertTrue( fileUtil.storeFileExists( existingFile ) );
    }

    @Test
    public void storeFileExistsMustReturnFalseForNonExistingFile() throws Exception
    {
        assertFalse( fileUtil.storeFileExists( nonExistingFile ) );
    }

    /* mkdirs( File dir ) */

    @Test
    public void directoryMustExistAfterMkdirs() throws Exception
    {
        // given
        // when
        fileUtil.mkdirs( nonExistingDirectory );

        // then
        assertDirectoryExist( nonExistingDirectory );
    }
}
