/*
 * Copyright (c) "Neo4j"
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
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryParserSetV2_3.V2_3;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryParserSetV4_0.V4_0;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryParserSetV4_2.V4_2;
import static org.neo4j.kernel.impl.transaction.log.entry.TransactionLogVersionSelector.INSTANCE;

class TransactionLogVersionSelectorTest
{
    @Test
    void shouldBeAbleToSelectAnyVersion()
    {
        assertEquals( V2_3, INSTANCE.select( V2_3.versionByte() ) );
        assertEquals( V4_0, INSTANCE.select( V4_0.versionByte() ) );
        assertEquals( V4_2, INSTANCE.select( V4_2.versionByte() ) );
    }

    @Test
    void shouldWarnAboutOldLogVersion()
    {
        assertThrows( UnsupportedLogVersionException.class, () -> INSTANCE.select( (byte) -4 ) );
    }

    @Test
    void shouldWarnAboutNotUsedNegativeLogVersion()
    {
        assertThrows( UnsupportedLogVersionException.class, () -> INSTANCE.select( (byte) -42 ) ); // unused for now
    }

    @Test
    void shouldWarnAboutNotUsedPositiveLogVersion()
    {
        assertThrows( UnsupportedLogVersionException.class, () -> INSTANCE.select( (byte) 100 ) ); // unused for now
    }

    @Test
    void moreRecent()
    {
        assertTrue( INSTANCE.moreRecentVersionExists( V2_3.versionByte() ) );
        assertTrue( INSTANCE.moreRecentVersionExists( V4_0.versionByte() ) );
        assertFalse( INSTANCE.moreRecentVersionExists( V4_2.versionByte() ) );
    }
}
