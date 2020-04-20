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
package org.neo4j.kernel.impl.transaction.log;

import org.junit.jupiter.api.Test;

import org.neo4j.kernel.recovery.LogTailScanner;
import org.neo4j.storageengine.migration.UpgradeNotAllowedException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.transaction.log.LogVersionUpgradeChecker.check;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryParserSetV2_3.V2_3;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryVersion.LATEST;

class LogVersionUpgradeCheckerTest
{
    private final LogTailScanner tailScanner = mock( LogTailScanner.class );

    @Test
    void noThrowWhenLatestVersionAndUpgradeIsNotAllowed()
    {
        when( tailScanner.getTailInformation() ).thenReturn( new OnlyVersionTailInformation( LATEST.version() ) );

        check( tailScanner, false );
    }

    @Test
    void throwWhenVersionIsOlderAndUpgradeIsNotAllowed()
    {
        when( tailScanner.getTailInformation() ).thenReturn( new OnlyVersionTailInformation( V2_3.version() ) );

        assertThrows( UpgradeNotAllowedException.class, () -> check( tailScanner, false ) );
    }

    @Test
    void stillAcceptLatestVersionWhenUpgradeIsAllowed()
    {
        when( tailScanner.getTailInformation() ).thenReturn( new OnlyVersionTailInformation( LATEST.version() ) );

        check( tailScanner, true );
    }

    @Test
    void acceptOlderLogsWhenUpgradeIsAllowed()
    {
        when( tailScanner.getTailInformation() ).thenReturn( new OnlyVersionTailInformation( V2_3.version() ) );

        check( tailScanner, true );
    }

    private static class OnlyVersionTailInformation extends LogTailScanner.LogTailInformation
    {
        OnlyVersionTailInformation( byte logEntryVersion )
        {
            super( false, 0, 0, 0, logEntryVersion );
        }
    }
}
