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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LogEntryVersionTest
{
    @Rule
    public ExpectedException expect = ExpectedException.none();

    @Test
    public void shouldBeAbleToSelectAnyVersion()
    {
        for ( LogEntryVersion version : LogEntryVersion.values() )
        {
            // GIVEN
            byte code = version.byteCode();

            // WHEN
            LogEntryVersion selectedVersion = LogEntryVersion.byVersion( code );

            // THEN
            assertEquals( version, selectedVersion );
        }
    }

    @Test
    public void shouldWarnAboutOldLogVersion()
    {
        expect.expect( UnsupportedLogVersionException.class );
        LogEntryVersion.byVersion( (byte)-4 );
    }

    @Test
    public void shouldWarnAboutNewerLogVersion()
    {
        expect.expect( UnsupportedLogVersionException.class );
        LogEntryVersion.byVersion( (byte)-42 ); // unused for now
    }

    @Test
    public void moreRecent()
    {
        assertTrue( LogEntryVersion.moreRecentVersionExists( LogEntryVersion.V2_3 ) );
        assertTrue( LogEntryVersion.moreRecentVersionExists( LogEntryVersion.V3_0 ) );
        assertTrue( LogEntryVersion.moreRecentVersionExists( LogEntryVersion.V2_3_5 ) );
        assertTrue( LogEntryVersion.moreRecentVersionExists( LogEntryVersion.V3_0_2 ) );
        assertFalse( LogEntryVersion.moreRecentVersionExists( LogEntryVersion.V3_0_10 ) );
    }
}
