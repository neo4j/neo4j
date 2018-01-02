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
package org.neo4j.kernel.impl.transaction;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.neo4j.kernel.impl.transaction.log.PhysicalLogFile.DEFAULT_VERSION_SUFFIX;

public class PhysicalLogFilesTest
{
    private final FileSystemAbstraction fs = mock( FileSystemAbstraction.class );
    private final File tmpDirectory = new File( "." );
    private final String filename = "filename";

    @Test
    public void shouldGetTheFileNameForAGivenVersion()
    {
        // given
        final PhysicalLogFiles files = new PhysicalLogFiles( tmpDirectory, filename, fs );
        final int version = 12;

        // when
        final File versionFileName = files.getLogFileForVersion( version );

        // then
        final File expected = new File( tmpDirectory, filename + DEFAULT_VERSION_SUFFIX + version );
        assertEquals( expected, versionFileName );
    }

    @Test
    public void shouldVisitEachLofFile()
    {
        // given
        PhysicalLogFiles files = new PhysicalLogFiles( tmpDirectory, filename, fs );

        final File[] filesOnDisk = new File[]{
                new File( tmpDirectory, filename + DEFAULT_VERSION_SUFFIX + "1" ),
                new File( tmpDirectory, "crap" + DEFAULT_VERSION_SUFFIX + "2" ),
                new File( tmpDirectory, filename + DEFAULT_VERSION_SUFFIX + "3" ),
                new File( tmpDirectory, filename )
        };

        when( fs.listFiles( tmpDirectory ) ).thenReturn( filesOnDisk );

        // when
        final List<File> seenFiles = new ArrayList<>();
        final List<Long> seenVersions = new ArrayList<>();

        files.accept( new PhysicalLogFiles.LogVersionVisitor()
        {
            @Override
            public void visit( File file, long logVersion )
            {
                seenFiles.add( file );
                seenVersions.add( logVersion );
            }
        } );

        // then
        assertEquals( Arrays.asList(
                new File( tmpDirectory, filename + DEFAULT_VERSION_SUFFIX + "1" ),
                new File( tmpDirectory, filename + DEFAULT_VERSION_SUFFIX + "3" )
        ), seenFiles );
        assertEquals( Arrays.asList(
                1l,
                3l
        ), seenVersions );
    }

    @Test
    public void shouldBeAbleToRetrieveTheHighestLogVersion()
    {
        // given
        PhysicalLogFiles files = new PhysicalLogFiles( tmpDirectory, filename, fs );

        final File[] filesOnDisk = new File[]{
                new File( tmpDirectory, filename + DEFAULT_VERSION_SUFFIX + "1" ),
                new File( tmpDirectory, "crap" + DEFAULT_VERSION_SUFFIX + "4" ),
                new File( tmpDirectory, filename + DEFAULT_VERSION_SUFFIX + "3" ),
                new File( tmpDirectory, filename )
        };

        when( fs.listFiles( tmpDirectory ) ).thenReturn( filesOnDisk );

        // when
        final long highestLogVersion = files.getHighestLogVersion();

        // then
        assertEquals( 3, highestLogVersion );
    }

    @Test
    public void shouldReturnANegativeValueIfThereAreNoLogFiles()
    {
        // given
        PhysicalLogFiles files = new PhysicalLogFiles( tmpDirectory, filename, fs );

        final File[] filesOnDisk = new File[]{
                new File( tmpDirectory, "crap" + DEFAULT_VERSION_SUFFIX + "4" ),
                new File( tmpDirectory, filename )
        };

        when( fs.listFiles( tmpDirectory ) ).thenReturn( filesOnDisk );

        // when
        final long highestLogVersion = files.getHighestLogVersion();

        // then
        assertEquals( -1, highestLogVersion );
    }

    @Test
    public void shouldFindTheVersionBasedOnTheFilename()
    {
        // given
        final File file =
                new File( "v" + DEFAULT_VERSION_SUFFIX + DEFAULT_VERSION_SUFFIX + DEFAULT_VERSION_SUFFIX + "2" );

        // when
        long logVersion = PhysicalLogFiles.getLogVersion( file );

        // then
        assertEquals( 2, logVersion );
    }

    @Test
    public void shouldThrowIfThereIsNoVersionInTheFileName()
    {
        // given
        final File file = new File( "wrong" );

        // when
        try
        {
            PhysicalLogFiles.getLogVersion( file );
            fail( "should have thrown" );
        }
        catch ( RuntimeException ex )
        {
            assertEquals( "Invalid log file '" + file.getName() + "'", ex.getMessage() );
        }
    }

    @Test(expected = NumberFormatException.class)
    public void shouldThrowIfVersionIsNotANumber()
    {
        // given
        final File file = new File( "aa" + DEFAULT_VERSION_SUFFIX + "A" );

        // when
        PhysicalLogFiles.getLogVersion( file );
    }
}
