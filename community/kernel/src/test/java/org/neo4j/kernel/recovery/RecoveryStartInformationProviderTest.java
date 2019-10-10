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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import org.neo4j.exceptions.UnderlyingStorageException;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.entry.CheckPoint;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryVersion;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.recovery.LogTailScanner.LogTailInformation;
import org.neo4j.kernel.recovery.RecoveryStartInformationProvider.Monitor;
import org.neo4j.storageengine.api.StoreId;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_FORMAT_LOG_HEADER_SIZE;
import static org.neo4j.kernel.recovery.LogTailScanner.NO_TRANSACTION_ID;
import static org.neo4j.kernel.recovery.RecoveryStartInformation.MISSING_LOGS;
import static org.neo4j.storageengine.api.LogVersionRepository.INITIAL_LOG_VERSION;

class RecoveryStartInformationProviderTest
{
    private final long currentLogVersion = 2L;
    private final long logVersion = 2L;
    private final LogTailScanner tailScanner = mock( LogTailScanner.class );
    private final LogFiles logFiles = mock( LogFiles.class );
    private final Monitor monitor = mock( Monitor.class );

    @BeforeEach
    void setUp() throws IOException
    {
        var logHeader = new LogHeader( 0, 1, StoreId.UNKNOWN );
        when( logFiles.extractHeader( 0 ) ).thenReturn( logHeader );
    }

    @Test
    void shouldReturnUnspecifiedIfThereIsNoNeedForRecovery()
    {
        // given
        when( tailScanner.getTailInformation() ).thenReturn( new LogTailScanner.LogTailInformation( false,
                NO_TRANSACTION_ID, logVersion, currentLogVersion, LogEntryVersion.LATEST_VERSION ) );

        // when
        RecoveryStartInformation recoveryStartInformation = new RecoveryStartInformationProvider( tailScanner, logFiles, monitor ).get();

        // then
        verify( monitor ).noCommitsAfterLastCheckPoint( null );
        assertEquals( LogPosition.UNSPECIFIED, recoveryStartInformation.getRecoveryPosition() );
        assertEquals( NO_TRANSACTION_ID, recoveryStartInformation.getFirstTxIdAfterLastCheckPoint() );
        assertFalse( recoveryStartInformation.isRecoveryRequired() );
    }

    @Test
    void shouldReturnLogPositionToRecoverFromIfNeeded()
    {
        // given
        LogPosition checkPointLogPosition = new LogPosition( 1L, 4242 );
        when( tailScanner.getTailInformation() )
                .thenReturn( new LogTailInformation( new CheckPoint( checkPointLogPosition ), true, 10L, logVersion,
                        currentLogVersion, LogEntryVersion.LATEST_VERSION, StoreId.UNKNOWN ) );

        // when
        RecoveryStartInformation recoveryStartInformation = new RecoveryStartInformationProvider( tailScanner, logFiles, monitor ).get();

        // then
        verify( monitor ).commitsAfterLastCheckPoint( checkPointLogPosition, 10L );
        assertEquals( checkPointLogPosition, recoveryStartInformation.getRecoveryPosition() );
        assertEquals( 10L, recoveryStartInformation.getFirstTxIdAfterLastCheckPoint() );
        assertTrue( recoveryStartInformation.isRecoveryRequired() );
    }

    @Test
    void shouldRecoverFromStartOfLogZeroIfThereAreNoCheckPointAndOldestLogIsVersionZero()
    {
        // given
        when( tailScanner.getTailInformation() ).thenReturn( new LogTailInformation( true, 10L, INITIAL_LOG_VERSION,
                currentLogVersion, LogEntryVersion.LATEST_VERSION ) );

        // when
        RecoveryStartInformation recoveryStartInformation = new RecoveryStartInformationProvider( tailScanner, logFiles, monitor ).get();

        // then
        verify( monitor ).noCheckPointFound();
        assertEquals( new LogPosition( 0, CURRENT_FORMAT_LOG_HEADER_SIZE ), recoveryStartInformation.getRecoveryPosition() );
        assertEquals( 10L, recoveryStartInformation.getFirstTxIdAfterLastCheckPoint() );
        assertTrue( recoveryStartInformation.isRecoveryRequired() );
    }

    @Test
    void  detectMissingTransactionLogsInformation()
    {
        when( tailScanner.getTailInformation() ).thenReturn( new LogTailInformation( false, -1, -1,
                -1, LogEntryVersion.LATEST_VERSION ) );

        RecoveryStartInformation recoveryStartInformation = new RecoveryStartInformationProvider( tailScanner, logFiles, monitor ).get();

        assertSame( MISSING_LOGS, recoveryStartInformation );
    }

    @Test
    void shouldFailIfThereAreNoCheckPointsAndOldestLogVersionInNotZero()
    {
        // given
        long oldestLogVersionFound = 1L;
        when( tailScanner.getTailInformation() ).thenReturn(
                new LogTailScanner.LogTailInformation( true, 10L, oldestLogVersionFound, currentLogVersion, LogEntryVersion.LATEST_VERSION ) );

        // when
        UnderlyingStorageException storageException =
                assertThrows( UnderlyingStorageException.class, () -> new RecoveryStartInformationProvider( tailScanner, logFiles, monitor ).get() );
        final String expectedMessage = "No check point found in any log file from version " + oldestLogVersionFound + " to " + logVersion;
        assertEquals( expectedMessage, storageException.getMessage() );
    }
}
