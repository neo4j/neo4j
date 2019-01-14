/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.junit.Test;
import org.mockito.InOrder;

import java.io.File;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFiles;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeader.LOG_HEADER_SIZE;

public class ThresholdBasedPruneStrategyTest
{
    private final FileSystemAbstraction fileSystem = mock( FileSystemAbstraction.class );
    private final LogFiles files = mock( TransactionLogFiles.class );
    private final Threshold threshold = mock( Threshold.class );

    @Test
    public void shouldNotDeleteAnythingIfThresholdDoesNotAllow()
    {
        // Given
        File fileName0 = new File( "logical.log.v0" );
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
        when( files.getLogFileForVersion( 0 ) ).thenReturn( fileName0 );

        when( fileSystem.fileExists( fileName6 ) ).thenReturn( true );
        when( fileSystem.fileExists( fileName5 ) ).thenReturn( true );
        when( fileSystem.fileExists( fileName4 ) ).thenReturn( true );
        when( fileSystem.fileExists( fileName3 ) ).thenReturn( true );
        when( fileSystem.fileExists( fileName2 ) ).thenReturn( true );
        when( fileSystem.fileExists( fileName1 ) ).thenReturn( true );
        when( fileSystem.fileExists( fileName0 ) ).thenReturn( true );

        when( fileSystem.getFileSize( any() ) ).thenReturn( LOG_HEADER_SIZE + 1L );

        when( threshold.reached( any(), anyLong(), any() ) ).thenReturn( false );

        final ThresholdBasedPruneStrategy strategy = new ThresholdBasedPruneStrategy( fileSystem, files, threshold );

        // When
        strategy.findLogVersionsToDelete( 7L ).forEachOrdered(
                v -> fileSystem.deleteFile( files.getLogFileForVersion( v ) ) );

        // Then
        verify( threshold, times( 1 ) ).init();
        verify( fileSystem, never() ).deleteFile( any() );
    }

    @Test
    public void shouldDeleteJustWhatTheThresholdSays()
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

        when( fileSystem.getFileSize( any() ) ).thenReturn( LOG_HEADER_SIZE + 1L );

        final ThresholdBasedPruneStrategy strategy = new ThresholdBasedPruneStrategy( fileSystem, files, threshold );

        // When
        strategy.findLogVersionsToDelete( 7L ).forEachOrdered(
                v -> fileSystem.deleteFile( files.getLogFileForVersion( v ) ) );

        // Then
        InOrder order = inOrder( threshold, fileSystem );
        order.verify( threshold, times( 1 ) ).init();
        order.verify( fileSystem, times( 1 ) ).deleteFile( fileName1 );
        order.verify( fileSystem, times( 1 ) ).deleteFile( fileName2 );
        order.verify( fileSystem, times( 1 ) ).deleteFile( fileName3 );
        verify( fileSystem, never() ).deleteFile( fileName4 );
        verify( fileSystem, never() ).deleteFile( fileName5 );
        verify( fileSystem, never() ).deleteFile( fileName6 );
    }
}
