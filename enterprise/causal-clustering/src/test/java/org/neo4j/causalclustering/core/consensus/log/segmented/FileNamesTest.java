/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.core.consensus.log.segmented;

import org.junit.Test;

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.Log;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class FileNamesTest
{
    @Test
    public void shouldProperlyFormatFilenameForVersion()
    {
        // Given
        File base = new File( "base" );
        FileNames fileNames = new FileNames( base );

        // When - Then
        // when asking for a given version...
        for ( int i = 0; i < 100; i++ )
        {
            File forVersion = fileNames.getForVersion( i );
            // ...then the expected thing is returned
            assertEquals( forVersion, new File( base, FileNames.BASE_FILE_NAME + i ) );
        }
    }

    @Test
    public void shouldWorkCorrectlyOnReasonableDirectoryContents()
    {
        // Given
        // a raft log directory with just the expected files, without gaps
        File base = new File( "base" );
        FileNames fileNames = new FileNames( base );
        FileSystemAbstraction fsa = mock( FileSystemAbstraction.class );
        Log log = mock( Log.class );
        List<File> filesPresent = new LinkedList<>();
        int lower = 0;
        int upper = 24;
        // the files are added in reverse order, so we can verify that FileNames orders based on version
        for ( int i = upper; i >= lower; i-- )
        {
            filesPresent.add( fileNames.getForVersion( i ) );
        }
        when( fsa.listFiles( base ) ).thenReturn( filesPresent.toArray( new File[]{} ) );

        // When
        // asked for the contents of the directory
        SortedMap<Long, File> allFiles = fileNames.getAllFiles( fsa, log );

        // Then
        // all the things we added above should be returned
        assertEquals( upper - lower + 1, allFiles.size() );
        long currentVersion = lower;
        for ( Map.Entry<Long, File> longFileEntry : allFiles.entrySet() )
        {
            assertEquals( currentVersion, longFileEntry.getKey().longValue() );
            assertEquals( fileNames.getForVersion( currentVersion ), longFileEntry.getValue() );
            currentVersion++;
        }
    }

    @Test
    public void shouldIgnoreUnexpectedLogDirectoryContents()
    {
        // Given
        // a raft log directory with just the expected files, without gaps
        File base = new File( "base" );
        FileNames fileNames = new FileNames( base );
        FileSystemAbstraction fsa = mock( FileSystemAbstraction.class );
        Log log = mock( Log.class );
        List<File> filesPresent = new LinkedList<>();

        filesPresent.add( fileNames.getForVersion( 0 ) ); // should be included
        filesPresent.add( fileNames.getForVersion( 1 ) ); // should be included
        filesPresent.add( fileNames.getForVersion( 10 ) ); // should be included
        filesPresent.add( fileNames.getForVersion( 11 ) ); // should be included
        filesPresent.add( new File( base, FileNames.BASE_FILE_NAME + "01" ) ); // should be ignored
        filesPresent.add( new File( base, FileNames.BASE_FILE_NAME + "001" ) ); // should be ignored
        filesPresent.add( new File( base, FileNames.BASE_FILE_NAME ) ); // should be ignored
        filesPresent.add( new File( base, FileNames.BASE_FILE_NAME + "-1" ) ); // should be ignored
        filesPresent.add( new File( base, FileNames.BASE_FILE_NAME + "1a" ) ); // should be ignored
        filesPresent.add( new File( base, FileNames.BASE_FILE_NAME + "a1" ) ); // should be ignored
        filesPresent.add( new File( base, FileNames.BASE_FILE_NAME + "ab" ) ); // should be ignored

        when( fsa.listFiles( base ) ).thenReturn( filesPresent.toArray( new File[]{} ) );

        // When
        // asked for the contents of the directory
        SortedMap<Long,File> allFiles = fileNames.getAllFiles( fsa, log );

        // Then
        // only valid things should be returned
        assertEquals( 4, allFiles.size() );
        assertEquals( allFiles.get( 0L ), fileNames.getForVersion( 0 ) );
        assertEquals( allFiles.get( 1L ), fileNames.getForVersion( 1 ) );
        assertEquals( allFiles.get( 10L ), fileNames.getForVersion( 10 ) );
        assertEquals( allFiles.get( 11L ), fileNames.getForVersion( 11 ) );

        // and the invalid ones should be logged
        verify( log, times( 7 ) ).warn( anyString() );
    }
}
