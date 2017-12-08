/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.store.format.highlimit;

import org.junit.Test;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.StubPageCursor;
import org.neo4j.kernel.impl.store.format.RecordFormat;
import org.neo4j.kernel.impl.store.id.IdSequence;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.kernel.impl.store.NoStoreHeader.NO_STORE_HEADER;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;

public class RelationshipTypeTokenRecordFormatTest
{
    @Test
    public void shouldHandleRelationshipTypesBeyond2Bytes() throws Exception
    {
        // given
        RecordFormat<RelationshipTypeTokenRecord> format = HighLimit.RECORD_FORMATS.relationshipTypeToken();
        int typeId = 1 << (Short.SIZE + Byte.SIZE) - 1;
        RelationshipTypeTokenRecord record = new RelationshipTypeTokenRecord( typeId );
        int recordSize = format.getRecordSize( NO_STORE_HEADER );
        record.initialize( true, 10 );
        IdSequence doubleUnits = mock( IdSequence.class );
        PageCursor cursor = new StubPageCursor( 0, (int) kibiBytes( 8 ) );

        // when
        format.prepare( record, recordSize, doubleUnits );
        format.write( record, cursor, recordSize );
        verifyNoMoreInteractions( doubleUnits );

        // then
        RelationshipTypeTokenRecord read = new RelationshipTypeTokenRecord( typeId );
        format.read( record, cursor, NORMAL, recordSize );
        assertEquals( record, read );
    }

    @Test
    public void shouldReport3BytesMaxIdForRelationshipTypes() throws Exception
    {
        // given
        RecordFormat<RelationshipTypeTokenRecord> format = HighLimit.RECORD_FORMATS.relationshipTypeToken();

        // when
        long maxId = format.getMaxId();

        // then
        assertEquals( (1 << HighLimitFormatSettings.RELATIONSHIP_TYPE_TOKEN_MAXIMUM_ID_BITS) - 1, maxId );
    }
}
