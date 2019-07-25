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
package org.neo4j.kernel.impl.transaction.log;

import org.junit.jupiter.api.Test;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryVersion;
import org.neo4j.kernel.recovery.LogTailScanner;
import org.neo4j.storageengine.migration.UpgradeNotAllowedException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.transaction.log.LogVersionUpgradeChecker.check;

class LogVersionUpgradeCheckerTest
{
    private LogTailScanner tailScanner = mock( LogTailScanner.class );

    @Test
    void noThrowWhenLatestVersionAndUpgradeIsNotAllowed()
    {
        when( tailScanner.getTailInformation() ).thenReturn( new OnlyVersionTailInformation( LogEntryVersion.LATEST_VERSION ) );

        check( tailScanner, Config.defaults( GraphDatabaseSettings.allow_upgrade, false ) );
    }

    @Test
    void throwWhenVersionIsOlderAndUpgradeIsNotAllowed()
    {
        when( tailScanner.getTailInformation() ).thenReturn( new OnlyVersionTailInformation( LogEntryVersion.V3_0_10 ) );

        assertThrows( UpgradeNotAllowedException.class, () -> check( tailScanner, Config.defaults( GraphDatabaseSettings.allow_upgrade, false ) ) );
    }

    @Test
    void stillAcceptLatestVersionWhenUpgradeIsAllowed()
    {
        when( tailScanner.getTailInformation() ).thenReturn( new OnlyVersionTailInformation( LogEntryVersion.LATEST_VERSION ) );

        check( tailScanner, Config.defaults( GraphDatabaseSettings.allow_upgrade, true ) );
    }

    @Test
    void acceptOlderLogsWhenUpgradeIsAllowed()
    {
        when( tailScanner.getTailInformation() ).thenReturn( new OnlyVersionTailInformation( LogEntryVersion.V3_0_10 ) );

        check( tailScanner, Config.defaults( GraphDatabaseSettings.allow_upgrade, true ) );
    }

    private static class OnlyVersionTailInformation extends LogTailScanner.LogTailInformation
    {
        OnlyVersionTailInformation( LogEntryVersion logEntryVersion )
        {
            super( false, 0, 0, 0, logEntryVersion );
        }
    }
}
