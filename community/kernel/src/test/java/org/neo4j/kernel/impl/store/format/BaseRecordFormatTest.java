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
package org.neo4j.kernel.impl.store.format;

import org.junit.Test;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.StubPageCursor;
import org.neo4j.kernel.impl.store.format.standard.DynamicRecordFormat;
import org.neo4j.kernel.impl.store.record.DynamicRecord;

import static org.junit.Assert.assertEquals;

public class BaseRecordFormatTest
{
    @Test
    public void shouldRecognizeDesignatedInUseBit()
    {
        // GIVEN
        RecordFormat<DynamicRecord> format = new DynamicRecordFormat();
        PageCursor cursor = new StubPageCursor( 0, 1_000 );

        byte inUseByte = 0;
        for ( int i = 0; i < 8; i++ )
        {
            // WHEN
            cursor.setOffset( 68 );
            cursor.putByte( cursor.getOffset(), inUseByte );

            // THEN
            assertEquals( shouldBeInUse( inUseByte ), format.isInUse( cursor ) );
            inUseByte <<= 1;
            inUseByte |= 1;
        }
    }

    private boolean shouldBeInUse( byte inUseByte )
    {
        return (inUseByte & 0x10) != 0;
    }
}
