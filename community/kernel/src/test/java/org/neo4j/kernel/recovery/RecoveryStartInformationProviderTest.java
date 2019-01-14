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
package org.neo4j.kernel.recovery;

import org.junit.Test;

import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.entry.CheckPoint;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryVersion;
import org.neo4j.kernel.recovery.LogTailScanner.LogTailInformation;
import org.neo4j.kernel.recovery.RecoveryStartInformationProvider.Monitor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.transaction.log.LogVersionRepository.INITIAL_LOG_VERSION;
import static org.neo4j.kernel.recovery.LogTailScanner.NO_TRANSACTION_ID;

public class RecoveryStartInformationProviderTest
{
    private final long currentLogVersion = 2L;
    private final long logVersion = 2L;
    private final LogTailScanner tailScanner = mock( LogTailScanner.class );
    private final Monitor monitor = mock( Monitor.class );

    @Test
    public void shouldReturnUnspecifiedIfThereIsNoNeedForRecovery()
    {
        // given
        when( tailScanner.getTailInformation() ).thenReturn( new LogTailScanner.LogTailInformation( false,
                NO_TRANSACTION_ID, logVersion, currentLogVersion, LogEntryVersion.CURRENT ) );

        // when
        RecoveryStartInformation recoveryStartInformation = new RecoveryStartInformationProvider( tailScanner, monitor ).get();

        // then
        verify( monitor ).noCommitsAfterLastCheckPoint( null );
        assertEquals( LogPosition.UNSPECIFIED, recoveryStartInformation.getRecoveryPosition() );
        assertEquals( NO_TRANSACTION_ID, recoveryStartInformation.getFirstTxIdAfterLastCheckPoint() );
        assertFalse( recoveryStartInformation.isRecoveryRequired() );
    }

    @Test
    public void shouldReturnLogPositionToRecoverFromIfNeeded()
    {
        // given
        LogPosition checkPointLogPosition = new LogPosition( 1L, 4242 );
        when( tailScanner.getTailInformation() )
                .thenReturn( new LogTailInformation( new CheckPoint( checkPointLogPosition ), true, 10L, logVersion,
                        currentLogVersion, LogEntryVersion.CURRENT ) );

        // when
        RecoveryStartInformation recoveryStartInformation = new RecoveryStartInformationProvider( tailScanner, monitor ).get();

        // then
        verify( monitor ).commitsAfterLastCheckPoint( checkPointLogPosition, 10L );
        assertEquals( checkPointLogPosition, recoveryStartInformation.getRecoveryPosition() );
        assertEquals( 10L, recoveryStartInformation.getFirstTxIdAfterLastCheckPoint() );
        assertTrue( recoveryStartInformation.isRecoveryRequired() );
    }

    @Test
    public void shouldRecoverFromStartOfLogZeroIfThereAreNoCheckPointAndOldestLogIsVersionZero()
    {
        // given
        when( tailScanner.getTailInformation() ).thenReturn( new LogTailInformation( true, 10L, INITIAL_LOG_VERSION,
                currentLogVersion, LogEntryVersion.CURRENT ) );

        // when
        RecoveryStartInformation recoveryStartInformation = new RecoveryStartInformationProvider( tailScanner, monitor ).get();

        // then
        verify( monitor ).noCheckPointFound();
        assertEquals( LogPosition.start( INITIAL_LOG_VERSION ), recoveryStartInformation.getRecoveryPosition() );
        assertEquals( 10L, recoveryStartInformation.getFirstTxIdAfterLastCheckPoint() );
        assertTrue( recoveryStartInformation.isRecoveryRequired() );
    }

    @Test
    public void shouldFailIfThereAreNoCheckPointsAndOldestLogVersionInNotZero()
    {
        // given
        long oldestLogVersionFound = 1L;
        when( tailScanner.getTailInformation() ).thenReturn( new LogTailScanner.LogTailInformation( true, 10L,
            oldestLogVersionFound, currentLogVersion, LogEntryVersion.CURRENT ) );

        // when
        try
        {
            new RecoveryStartInformationProvider( tailScanner, monitor ).get();
        }
        catch ( UnderlyingStorageException ex )
        {
            // then
            final String expectedMessage =
                    "No check point found in any log file from version " + oldestLogVersionFound + " to " + logVersion;
            assertEquals( expectedMessage, ex.getMessage() );
        }
    }
}
