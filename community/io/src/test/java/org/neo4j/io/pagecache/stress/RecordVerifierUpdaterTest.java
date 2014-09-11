/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.io.pagecache.stress;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

public class RecordVerifierUpdaterTest
{
    @Test
    public void shouldVerifyAndUpdateRecord() throws Exception
    {
        StubPageCursor pageCursor = StubPageCursor.create( 0L );
        RecordVerifierUpdater recordVerifierUpdater = new RecordVerifierUpdater( 1 );

        recordVerifierUpdater.verifyChecksumAndUpdateCount( pageCursor, 0, 0 );

        assertThat( pageCursor.getCount( 0, 0 ), is( 1L ) );
        assertThat( pageCursor.getChecksum( 0 ), is( 1L ) );
    }

    @Test
    public void shouldVerifyAndUpdateRecordMultipleTimes() throws Exception
    {
        StubPageCursor pageCursor = StubPageCursor.create( 0L );
        RecordVerifierUpdater recordVerifierUpdater = new RecordVerifierUpdater( 1 );

        recordVerifierUpdater.verifyChecksumAndUpdateCount( pageCursor.resetOffset(), 0, 0 );
        recordVerifierUpdater.verifyChecksumAndUpdateCount( pageCursor.resetOffset(), 0, 0 );
        recordVerifierUpdater.verifyChecksumAndUpdateCount( pageCursor.resetOffset(), 0, 0 );

        assertThat( pageCursor.getCount( 0, 0 ), is( 3L ) );
        assertThat( pageCursor.getChecksum( 0 ), is( 3L ) );
    }

    @Test
    public void shouldVerifyAndUpdateRecords() throws Exception
    {
        StubPageCursor pageCursor = StubPageCursor.create( 0L, 0L );
        RecordVerifierUpdater recordVerifierUpdater = new RecordVerifierUpdater( 2 );

        recordVerifierUpdater.verifyChecksumAndUpdateCount( pageCursor.resetOffset(), 0, 0 );
        recordVerifierUpdater.verifyChecksumAndUpdateCount( pageCursor.resetOffset(), 0, 1 );
        recordVerifierUpdater.verifyChecksumAndUpdateCount( pageCursor.resetOffset(), 0, 0 );
        recordVerifierUpdater.verifyChecksumAndUpdateCount( pageCursor.resetOffset(), 0, 1 );
        recordVerifierUpdater.verifyChecksumAndUpdateCount( pageCursor.resetOffset(), 0, 1 );

        assertThat( pageCursor.getCount( 0, 0 ), is( 2L ) );
        assertThat( pageCursor.getCount( 0, 1 ), is( 3L ) );
        assertThat( pageCursor.getChecksum( 0 ), is( 5L ) );
    }

    @Test
    public void shouldVerifyAndUpdateRecordEvenWhenItFlowsOver() throws Exception
    {
        StubPageCursor pageCursor = StubPageCursor.create( Long.MAX_VALUE );
        RecordVerifierUpdater recordVerifierUpdater = new RecordVerifierUpdater( 1 );

        recordVerifierUpdater.verifyChecksumAndUpdateCount( pageCursor, 0, 0 );

        assertThat( pageCursor.getCount( 0, 0 ), is( Long.MIN_VALUE ) );
        assertThat( pageCursor.getChecksum( 0 ), is( Long.MIN_VALUE ) );
    }
}
