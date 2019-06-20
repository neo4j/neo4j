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
package org.neo4j.kernel.impl.transaction.log.entry;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LogEntryVersionTest
{
    @Test
    void shouldBeAbleToSelectAnyVersion()
    {
        for ( LogEntryVersion version : LogEntryVersion.values() )
        {
            // GIVEN
            byte code = version.version();

            // WHEN
            LogEntryVersion selectedVersion = LogEntryVersion.byVersion( code );

            // THEN
            assertEquals( version, selectedVersion );
        }
    }

    @Test
    void shouldWarnAboutOldLogVersion()
    {
        assertThrows( UnsupportedLogVersionException.class, () -> LogEntryVersion.byVersion( (byte) -4 ) );
    }

    @Test
    void shouldWarnAboutNewerLogVersion()
    {
        assertThrows( UnsupportedLogVersionException.class, () -> LogEntryVersion.byVersion( (byte) -42 ) ); // unused for now
    }

    @Test
    void moreRecent()
    {
        assertTrue( LogEntryVersion.moreRecentVersionExists( LogEntryVersion.V3_0_10 ) );
        assertFalse( LogEntryVersion.moreRecentVersionExists( LogEntryVersion.V4_0 ) );
    }
}
