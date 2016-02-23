/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import java.io.IOException;

import org.neo4j.io.ByteUnit;
import org.neo4j.io.pagecache.StubPageCursor;
import org.neo4j.kernel.impl.store.IntStoreHeader;
import org.neo4j.kernel.impl.store.format.highlimit.RelationshipRecordFormat;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class RelationshipRecordFormatTest
{
    private static final int DATA_SIZE = 100;

    @Test
    public void writeAndReadRecordWithRelativeReferences() throws IOException
    {
        RelationshipRecordFormat format = new RelationshipRecordFormat();
        int recordSize = format.getRecordSize( new IntStoreHeader( DATA_SIZE ) );
        StubPageCursor cursor = new StubPageCursor( 0, (int) ByteUnit.kibiBytes( 4 ) );
        long recordId = 0xF1F1F1F1F1F1L;
        int recordOffset = cursor.getOffset();

        RelationshipRecord record = createRecord( format, recordId );
        format.write( record, cursor, recordSize, null);

        RelationshipRecord recordFromStore = format.newRecord();
        recordFromStore.setId( recordId  );
        resetCursor( cursor, recordOffset );
        format.read( recordFromStore, cursor, RecordLoad.NORMAL, recordSize, null );

        // records should be the same
        assertEquals( record.getFirstNextRel(), recordFromStore.getFirstNextRel() );
        assertEquals( record.getFirstNode(), recordFromStore.getFirstNode() );
        assertEquals( record.getFirstPrevRel(), recordFromStore.getFirstPrevRel() );
        assertEquals( record.getSecondNextRel(), recordFromStore.getSecondNextRel() );
        assertEquals( record.getSecondNode(), recordFromStore.getSecondNode() );
        assertEquals( record.getSecondPrevRel(), recordFromStore.getSecondPrevRel() );

        // now lets try to read same data into a record with different id - we should get different absolute references
        resetCursor( cursor, recordOffset );
        RelationshipRecord recordWithOtherId = format.newRecord();
        recordWithOtherId.setId( 1L  );
        format.read( recordWithOtherId, cursor, RecordLoad.NORMAL, recordSize, null );

        assertNotEquals( record.getFirstNextRel(), recordWithOtherId.getFirstNextRel() );
        assertNotEquals( record.getFirstPrevRel(), recordWithOtherId.getFirstPrevRel() );
        assertNotEquals( record.getSecondNextRel(), recordWithOtherId.getSecondNextRel() );
        assertNotEquals( record.getSecondPrevRel(), recordWithOtherId.getSecondPrevRel() );
    }

    private void resetCursor( StubPageCursor cursor, int recordOffset )
    {
        cursor.setOffset( recordOffset );
    }

    private RelationshipRecord createRecord( RelationshipRecordFormat format, long recordId )
    {
        RelationshipRecord record = format.newRecord();
        record.setInUse( true );
        record.setId( recordId );
        record.setFirstNextRel( 1L );
        record.setFirstNode( 2L );
        record.setFirstPrevRel( 3L );
        record.setSecondNextRel( 4L );
        record.setSecondNode( 5L );
        record.setSecondPrevRel( 6L );
        return record;
    }
}
