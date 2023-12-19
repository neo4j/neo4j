/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
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
    public void shouldReport3BytesMaxIdForRelationshipTypes()
    {
        // given
        RecordFormat<RelationshipTypeTokenRecord> format = HighLimit.RECORD_FORMATS.relationshipTypeToken();

        // when
        long maxId = format.getMaxId();

        // then
        assertEquals( (1 << HighLimitFormatSettings.RELATIONSHIP_TYPE_TOKEN_MAXIMUM_ID_BITS) - 1, maxId );
    }
}
