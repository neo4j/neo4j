/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.impl.transaction.log.pruning;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.LogFileInformation;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFile;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_FORMAT_LOG_HEADER_SIZE;

class ThresholdBasedPruneStrategyTest
{
    private final FileSystemAbstraction fileSystem = mock( FileSystemAbstraction.class );
    private final LogFile logFile = mock( TransactionLogFile.class );
    private final Threshold threshold = mock( Threshold.class );

    @Test
    void shouldNotDeleteAnythingIfThresholdDoesNotAllow()
    {
        // Given
        Path fileName0 = Path.of( "logical.log.v0" );
        Path fileName1 = Path.of( "logical.log.v1" );
        Path fileName2 = Path.of( "logical.log.v2" );
        Path fileName3 = Path.of( "logical.log.v3" );
        Path fileName4 = Path.of( "logical.log.v4" );
        Path fileName5 = Path.of( "logical.log.v5" );
        Path fileName6 = Path.of( "logical.log.v6" );

        when( logFile.getLogFileForVersion( 6 ) ).thenReturn( fileName6 );
        when( logFile.getLogFileForVersion( 5 ) ).thenReturn( fileName5 );
        when( logFile.getLogFileForVersion( 4 ) ).thenReturn( fileName4 );
        when( logFile.getLogFileForVersion( 3 ) ).thenReturn( fileName3 );
        when( logFile.getLogFileForVersion( 2 ) ).thenReturn( fileName2 );
        when( logFile.getLogFileForVersion( 1 ) ).thenReturn( fileName1 );
        when( logFile.getLogFileForVersion( 0 ) ).thenReturn( fileName0 );
        when( logFile.getLowestLogVersion() ).thenReturn( 0L );

        when( fileSystem.fileExists( fileName6.toFile() ) ).thenReturn( true );
        when( fileSystem.fileExists( fileName5.toFile() ) ).thenReturn( true );
        when( fileSystem.fileExists( fileName4.toFile() ) ).thenReturn( true );
        when( fileSystem.fileExists( fileName3.toFile() ) ).thenReturn( true );
        when( fileSystem.fileExists( fileName2.toFile() ) ).thenReturn( true );
        when( fileSystem.fileExists( fileName1.toFile() ) ).thenReturn( true );
        when( fileSystem.fileExists( fileName0.toFile() ) ).thenReturn( true );

        when( fileSystem.getFileSize( any() ) ).thenReturn( CURRENT_FORMAT_LOG_HEADER_SIZE + 1L );

        when( threshold.reached( any(), anyLong(), any() ) ).thenReturn( false );

        ThresholdBasedPruneStrategy strategy = new ThresholdBasedPruneStrategy( logFile, threshold );

        // When
        strategy.findLogVersionsToDelete( 7L ).forEachOrdered(
                v -> fileSystem.deleteFile( logFile.getLogFileForVersion( v ).toFile() ) );

        // Then
        verify( threshold ).init();
        verify( fileSystem, never() ).deleteFile( any() );
    }

    @Test
    void shouldDeleteJustWhatTheThresholdSays()
    {
        // Given
        when( threshold.reached( any(), eq( 6L ), any() ) )
                .thenReturn( false );
        when( threshold.reached( any(), eq( 5L ), any() ) )
                .thenReturn( false );
        when( threshold.reached( any(), eq( 4L ), any() ) )
                .thenReturn( false );
        when( threshold.reached( any(), eq( 3L ), any() ) )
                .thenReturn( true );

        Path fileName1 = Path.of( "logical.log.v1" );
        Path fileName2 = Path.of( "logical.log.v2" );
        Path fileName3 = Path.of( "logical.log.v3" );
        Path fileName4 = Path.of( "logical.log.v4" );
        Path fileName5 = Path.of( "logical.log.v5" );
        Path fileName6 = Path.of( "logical.log.v6" );

        when( logFile.getLogFileForVersion( 6 ) ).thenReturn( fileName6 );
        when( logFile.getLogFileForVersion( 5 ) ).thenReturn( fileName5 );
        when( logFile.getLogFileForVersion( 4 ) ).thenReturn( fileName4 );
        when( logFile.getLogFileForVersion( 3 ) ).thenReturn( fileName3 );
        when( logFile.getLogFileForVersion( 2 ) ).thenReturn( fileName2 );
        when( logFile.getLogFileForVersion( 1 ) ).thenReturn( fileName1 );
        when( logFile.getLowestLogVersion() ).thenReturn( 1L );

        when( fileSystem.getFileSize( any() ) ).thenReturn( CURRENT_FORMAT_LOG_HEADER_SIZE + 1L );

        ThresholdBasedPruneStrategy strategy = new ThresholdBasedPruneStrategy( logFile, threshold );

        // When
        strategy.findLogVersionsToDelete( 7L ).forEachOrdered(
                v -> fileSystem.deleteFile( logFile.getLogFileForVersion( v ).toFile() ) );

        // Then
        verify( threshold ).init();
        verify( fileSystem ).deleteFile( fileName1.toFile() );
        verify( fileSystem ).deleteFile( fileName2.toFile() );
        verify( fileSystem ).deleteFile( fileName3.toFile() );
        verify( fileSystem, never() ).deleteFile( fileName4.toFile() );
        verify( fileSystem, never() ).deleteFile( fileName5.toFile() );
        verify( fileSystem, never() ).deleteFile( fileName6.toFile() );
    }

    @Test
    void minimalAvailableVersionHigherThanRequested()
    {
        when( logFile.getLowestLogVersion() ).thenReturn( 10L );
        when( threshold.reached( any(), anyLong(), any() ) ).thenReturn( true );

        ThresholdBasedPruneStrategy strategy = new ThresholdBasedPruneStrategy( logFile, threshold );

        assertFalse( strategy.findLogVersionsToDelete( 5 ).findAny().isPresent() );
    }

    @Test
    void rangeWithMissingFilesCanBeProduced()
    {
        when( logFile.getLowestLogVersion() ).thenReturn( 10L );
        when( threshold.reached( any(), anyLong(), any() ) ).thenReturn( true );
        when( fileSystem.fileExists( any() ) ).thenReturn( false );

        ThresholdBasedPruneStrategy strategy = new ThresholdBasedPruneStrategy( logFile, threshold );

        assertArrayEquals( new long[]{10, 11, 12, 13}, strategy.findLogVersionsToDelete( 15 ).toArray() );
    }

    @Test
    void mustHaveToStringOfThreshold()
    {
        Threshold threshold = new Threshold()
        {
            @Override
            public void init()
            {
            }

            @Override
            public boolean reached( Path file, long version, LogFileInformation source )
            {
                return false;
            }

            @Override
            public String toString()
            {
                return "Super-duper threshold";
            }
        };
        ThresholdBasedPruneStrategy strategy = new ThresholdBasedPruneStrategy( logFile, threshold );
        assertEquals( "Super-duper threshold", strategy.toString() );
    }
}
