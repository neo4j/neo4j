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
package org.neo4j.kernel.recovery;

import org.junit.Test;

import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.entry.CheckPoint;
import org.neo4j.kernel.recovery.LatestCheckPointFinder.LatestCheckPoint;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.transaction.log.LogVersionRepository.INITIAL_LOG_VERSION;

public class PositionToRecoverFromTest
{
    private final long logVersion = 2l;
    private final LatestCheckPointFinder finder = mock( LatestCheckPointFinder.class );

    @Test
    public void shouldReturnUnspecifiedIfThereIsNoNeedForRecovery() throws Throwable
    {
        // given
        when( finder.find( logVersion ) ).thenReturn( new LatestCheckPoint( null, false, logVersion ) );

        // when
        LogPosition logPosition = new PositionToRecoverFrom( finder ).apply( logVersion );

        // then
        assertEquals( LogPosition.UNSPECIFIED, logPosition );
    }

    @Test
    public void shouldReturnLogPositionToRecoverFromIfNeeded() throws Throwable
    {
        // given
        LogPosition checkPointLogPosition = new LogPosition( 1l, 4242 );
        when( finder.find( logVersion ) )
                .thenReturn( new LatestCheckPoint( new CheckPoint( checkPointLogPosition ), true, logVersion ) );

        // when
        LogPosition logPosition = new PositionToRecoverFrom( finder ).apply( logVersion );

        // then
        assertEquals( checkPointLogPosition, logPosition );
    }

    @Test
    public void shouldRecoverFromStartOfLogZeroIfThereAreNoCheckPointAndOldestLogIsVersionZero() throws Throwable
    {
        // given
        when( finder.find( logVersion ) ).thenReturn( new LatestCheckPoint( null, true, INITIAL_LOG_VERSION ) );

        // when
        LogPosition logPosition = new PositionToRecoverFrom( finder ).apply( logVersion );

        // then
        assertEquals( LogPosition.start( INITIAL_LOG_VERSION ), logPosition );
    }

    @Test
    public void shouldFailIfThereAreNoCheckPointsAndOldestLogVersionInNotZero() throws Throwable
    {
        // given
        long oldestLogVersionFound = 1l;
        when( finder.find( logVersion ) ).thenReturn( new LatestCheckPoint( null, true, oldestLogVersionFound ) );

        // when
        try
        {
            new PositionToRecoverFrom( finder ).apply( logVersion );
        }
        catch( UnderlyingStorageException ex )
        {
            // then
            final String expectedMessage =
                    "No check point found in any log file from version " + oldestLogVersionFound + " to " + logVersion;
            assertEquals( expectedMessage, ex.getMessage() );
        }
    }
}
