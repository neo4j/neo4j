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
package org.neo4j.kernel.impl.transaction.log.pruning;

import org.junit.Test;
import org.mockito.Matchers;

import java.io.File;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.LogFileInformation;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeader.LOG_HEADER_SIZE;

public class ThresholdBasedPruneStrategyTest
{
    private final FileSystemAbstraction fileSystem = mock( FileSystemAbstraction.class );
    private final LogFileInformation logFileInfo = mock( LogFileInformation.class );
    private final PhysicalLogFiles files = mock( PhysicalLogFiles.class );
    private final Threshold threshold = mock( Threshold.class );

    @Test
    public void shouldNotDeleteAnythingIfThresholdDoesNotAllow() throws Exception
    {
        // Given
        File fileName1 = new File( "logical.log.v1" );
        File fileName2 = new File( "logical.log.v2" );
        File fileName3 = new File( "logical.log.v3" );
        File fileName4 = new File( "logical.log.v4" );
        File fileName5 = new File( "logical.log.v5" );
        File fileName6 = new File( "logical.log.v6" );

        when( files.getLogFileForVersion( 6 ) ).thenReturn( fileName6 );
        when( files.getLogFileForVersion( 5 ) ).thenReturn( fileName5 );
        when( files.getLogFileForVersion( 4 ) ).thenReturn( fileName4 );
        when( files.getLogFileForVersion( 3 ) ).thenReturn( fileName3 );
        when( files.getLogFileForVersion( 2 ) ).thenReturn( fileName2 );
        when( files.getLogFileForVersion( 1 ) ).thenReturn( fileName1 );

        when( fileSystem.fileExists( fileName6 ) ).thenReturn( true );
        when( fileSystem.fileExists( fileName5 ) ).thenReturn( true );
        when( fileSystem.fileExists( fileName4 ) ).thenReturn( true );
        when( fileSystem.fileExists( fileName3 ) ).thenReturn( true );
        when( fileSystem.fileExists( fileName2 ) ).thenReturn( true );
        when( fileSystem.fileExists( fileName1 ) ).thenReturn( true );

        when( fileSystem.getFileSize( Matchers.<File>any() ) ).thenReturn( LOG_HEADER_SIZE + 1l );

        when( threshold.reached( Matchers.<File>any(), anyLong(), Matchers.<LogFileInformation>any() ) ).thenReturn( false );

        final ThresholdBasedPruneStrategy strategy = new ThresholdBasedPruneStrategy( fileSystem, logFileInfo, files, threshold );

        // When
        strategy.prune( 7l );

        // Then
        verify( threshold, times( 1 ) ).init();
        verify( fileSystem, times( 0 ) ).deleteFile( Matchers.<File>any() );
    }

    @Test
    public void shouldDeleteJustWhatTheThresholdSays() throws Exception
    {
        // Given
        when( threshold.reached( Matchers.<File>any(), Matchers.eq( 6l ), Matchers.<LogFileInformation>any() ) )
                .thenReturn( false );
        when( threshold.reached( Matchers.<File>any(), Matchers.eq( 5l ), Matchers.<LogFileInformation>any() ) )
                .thenReturn( false );
        when( threshold.reached( Matchers.<File>any(), Matchers.eq( 4l ), Matchers.<LogFileInformation>any() ) )
                .thenReturn( false );
        when( threshold.reached( Matchers.<File>any(), Matchers.eq( 3l ), Matchers.<LogFileInformation>any() ) )
                .thenReturn( true );

        File fileName1 = new File( "logical.log.v1" );
        File fileName2 = new File( "logical.log.v2" );
        File fileName3 = new File( "logical.log.v3" );
        File fileName4 = new File( "logical.log.v4" );
        File fileName5 = new File( "logical.log.v5" );
        File fileName6 = new File( "logical.log.v6" );

        when( files.getLogFileForVersion( 6 ) ).thenReturn( fileName6 );
        when( files.getLogFileForVersion( 5 ) ).thenReturn( fileName5 );
        when( files.getLogFileForVersion( 4 ) ).thenReturn( fileName4 );
        when( files.getLogFileForVersion( 3 ) ).thenReturn( fileName3 );
        when( files.getLogFileForVersion( 2 ) ).thenReturn( fileName2 );
        when( files.getLogFileForVersion( 1 ) ).thenReturn( fileName1 );

        when( fileSystem.fileExists( fileName6 ) ).thenReturn( true );
        when( fileSystem.fileExists( fileName5 ) ).thenReturn( true );
        when( fileSystem.fileExists( fileName4 ) ).thenReturn( true );
        when( fileSystem.fileExists( fileName3 ) ).thenReturn( true );
        when( fileSystem.fileExists( fileName2 ) ).thenReturn( true );
        when( fileSystem.fileExists( fileName1 ) ).thenReturn( true );

        when( fileSystem.getFileSize( Matchers.<File>any() ) ).thenReturn( LOG_HEADER_SIZE + 1l );

        final ThresholdBasedPruneStrategy strategy = new ThresholdBasedPruneStrategy(
                fileSystem, logFileInfo, files, threshold
        );

        // When
        strategy.prune( 7l );

        // Then
        verify( threshold, times( 1 ) ).init();
        verify( fileSystem, times( 1 ) ).deleteFile( fileName1 );
        verify( fileSystem, times( 1 ) ).deleteFile( fileName2 );
        verify( fileSystem, times( 1 ) ).deleteFile( fileName3 );
    }
}
