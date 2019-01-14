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
package org.neo4j.kernel.impl.store.format.standard;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Collection;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.StubPageCursor;
import org.neo4j.kernel.impl.store.format.RecordFormat;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.neo4j.kernel.impl.store.NoStoreHeader.NO_STORE_HEADER;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;

@RunWith( Parameterized.class )
public class RelationshipGroupRecordFormatTest
{
    @Parameters
    public static Collection<RecordFormats> formats()
    {
        return asList( StandardV2_3.RECORD_FORMATS, StandardV3_0.RECORD_FORMATS );
    }

    private final RecordFormat<RelationshipGroupRecord> format;
    private final int recordSize;

    public RelationshipGroupRecordFormatTest( RecordFormats formats )
    {
        this.format = formats.relationshipGroup();
        this.recordSize = format.getRecordSize( NO_STORE_HEADER );
    }

    @Test
    public void shouldReadUnsignedRelationshipTypeId() throws Exception
    {
        // GIVEN
        try ( PageCursor cursor = new StubPageCursor( 1, recordSize * 10 ) )
        {
            int offset = 10;
            cursor.next();
            RelationshipGroupRecord group = new RelationshipGroupRecord( 2 )
                    .initialize( true, Short.MAX_VALUE + offset, 1, 2, 3, 4, 5 );
            cursor.setOffset( offset );
            format.write( group, cursor, recordSize );

            // WHEN
            RelationshipGroupRecord read = new RelationshipGroupRecord( group.getId() );
            cursor.setOffset( offset );
            format.read( read, cursor, NORMAL, recordSize );

            // THEN
            assertEquals( group, read );
        }
    }
}
