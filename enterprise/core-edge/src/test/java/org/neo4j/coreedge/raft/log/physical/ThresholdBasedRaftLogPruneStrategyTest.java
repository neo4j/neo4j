/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.coreedge.raft.log.physical;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.mockito.Matchers;
import org.neo4j.kernel.impl.transaction.log.LogFileInformation;
import org.neo4j.kernel.impl.transaction.log.pruning.Threshold;

public class ThresholdBasedRaftLogPruneStrategyTest
{
    private final LogFileInformation logFileInfo = mock( PhysicalRaftLogFileInformation.class );
    private final PhysicalRaftLogFiles files = mock( PhysicalRaftLogFiles.class );
    private final Threshold threshold = mock( Threshold.class );

    @Test
    public void shouldNotDeleteAnythingIfThresholdDoesNotAllow() throws Exception
    {
        // Given
        when( threshold.reached( any(), anyLong(), any() ) ).thenReturn( false );

        when( files.containsEntries( anyLong() ) ).thenReturn( true );
        when( files.versionExists( anyLong() ) ).thenReturn( true );

        final RaftLogPruneStrategy strategy = new RaftLogPruneStrategy( logFileInfo, files, threshold );

        // When
        strategy.prune( 7L );

        // Then
        verify( threshold, times( 1 ) ).init();
        verify( files, times( 0 ) ).pruneUpTo( anyLong() );
    }

    @Test
    public void shouldDeleteJustWhatTheThresholdSays() throws Exception
    {
        // Given
        when( threshold.reached( any(), Matchers.eq( 6L ), any() ) ).thenReturn( false );
        when( threshold.reached( any(), Matchers.eq( 5L ), any() ) ).thenReturn( false );
        when( threshold.reached( any(), Matchers.eq( 4L ), any() ) ).thenReturn( false );
        when( threshold.reached( any(), Matchers.eq( 3L ), any() ) ).thenReturn( true );

        when( files.containsEntries( anyLong() ) ).thenReturn( true );
        when( files.versionExists( anyLong() ) ).thenReturn( true );

        final RaftLogPruneStrategy strategy = new RaftLogPruneStrategy( logFileInfo, files, threshold );

        // When
        strategy.prune( 7L );

        // Then
        verify( threshold, times( 1 ) ).init();
        verify( files, times( 1 ) ).pruneUpTo( 3 );
    }

    @Test
    public void shouldAlwaysKeepOneFileAround() throws Exception
    {
        // Given
        when( threshold.reached( any(), Matchers.eq( 3L ), any() ) ).thenReturn( true );
        when( threshold.reached( any(), Matchers.eq( 2L ), any() ) ).thenReturn( true );
        when( threshold.reached( any(), Matchers.eq( 1L ), any() ) ).thenReturn( true );
        when( threshold.reached( any(), Matchers.eq( 0L ), any() ) ).thenReturn( true );

        when( files.containsEntries( anyLong() ) ).thenReturn( true );
        when( files.versionExists( anyLong() ) ).thenReturn( true );

        final RaftLogPruneStrategy strategy = new RaftLogPruneStrategy( logFileInfo, files, threshold );

        // When
        strategy.prune( 3L );

        // Then
        verify( threshold, times( 1 ) ).init();
        verify( files, times( 1 ) ).pruneUpTo( 2 );
    }
}
