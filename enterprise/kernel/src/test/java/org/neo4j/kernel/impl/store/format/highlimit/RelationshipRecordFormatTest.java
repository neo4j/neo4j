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
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.StubPageCursor;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.neo4j.kernel.impl.store.NoStoreHeader.NO_STORE_HEADER;
import static org.neo4j.kernel.impl.store.RecordPageLocationCalculator.offsetForId;
import static org.neo4j.kernel.impl.store.format.BaseRecordFormat.IN_USE_BIT;

public class RelationshipRecordFormatTest
{
    private final RelationshipRecordFormat format = new RelationshipRecordFormat();
    private final int recordSize = format.getRecordSize( NO_STORE_HEADER );
    private final StubPageCursor cursor = new StubPageCursor( 0, (int) ByteUnit.kibiBytes( 4 ) )
    {
        @Override
        public boolean next( long pageId ) throws IOException
        {
            // We're going to use this cursor in an environment where in all genericness this cursor
            // is one that can be moved around to other pages. That's not possible with this stub cursor,
            // however we know that in this test we'll stay on page 0 even if there are calls to next(pageId)
            // which are part of the format code.
            assertEquals( 0, pageId );
            return true;
        }
    };

    @Test
    public void writeAndReadRecordWithRelativeReferences() throws IOException
    {
        long recordId = 0xF1F1F1F1F1F1L;
        int recordOffset = cursor.getOffset();

        RelationshipRecord record = createRecord( format, recordId, false, false );
        RelationshipRecord firstInSecondChain = createRecord( format, recordId, false, true );
        RelationshipRecord firstInFirstChain = createRecord( format, recordId, true, false );
        RelationshipRecord firstInBothChains = createRecord( format, recordId, true, true );

        checkRecord( format, recordSize, cursor, recordId, recordOffset, record );
        checkRecord( format, recordSize, cursor, recordId, recordOffset, firstInSecondChain );
        checkRecord( format, recordSize, cursor, recordId, recordOffset, firstInFirstChain );
        checkRecord( format, recordSize, cursor, recordId, recordOffset, firstInBothChains );
    }

    /*
     * This test acts as a test group for whoever uses BaseHighLimitRecordFormat base class,
     * the logic for marking both units as unused when deleting exists there.
     */
    @Test
    public void shouldMarkBothUnitsAsUnusedhenDeletingRecordWhichHasSecondaryUnit() throws Exception
    {
        // GIVEN a record which requires two units
        PagedFile storeFile = mock( PagedFile.class );
        when( storeFile.pageSize() ).thenReturn( cursor.getCurrentPageSize() );
        long hugeValue = 1L << 48;
        RelationshipRecord record = new RelationshipRecord( 5 ).initialize( true,
                hugeValue + 1, hugeValue + 2, hugeValue + 3, 4,
                hugeValue + 5, hugeValue + 6, hugeValue + 7, hugeValue + 8, true, true );
        record.setSecondaryUnitId( 17 );
        record.setRequiresSecondaryUnit( true );
        cursor.setOffset( offsetForId( record.getId(), cursor.getCurrentPageSize(), recordSize ) );
        format.write( record, cursor, recordSize, storeFile );

        // WHEN deleting that record
        record.setInUse( false );
        cursor.setOffset( offsetForId( record.getId(), cursor.getCurrentPageSize(), recordSize ) );
        format.write( record, cursor, recordSize, storeFile );

        // THEN both units should have been marked as unused
        cursor.setOffset( offsetForId( record.getId(), cursor.getCurrentPageSize(), recordSize ) );
        assertFalse( recordInUse( cursor ) );
        cursor.setOffset( offsetForId( record.getSecondaryUnitId(), cursor.getCurrentPageSize(), recordSize ) );
        assertFalse( recordInUse( cursor ) );
    }

    private boolean recordInUse( StubPageCursor cursor )
    {
        byte header = cursor.getByte();
        return (header & IN_USE_BIT) != 0;
    }

    private void checkRecord( RelationshipRecordFormat format, int recordSize, StubPageCursor cursor,
            long recordId, int recordOffset, RelationshipRecord record ) throws IOException
    {
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

    private RelationshipRecord createRecord( RelationshipRecordFormat format, long recordId,
            boolean firstInFirstChain, boolean firstInSecondChain )
    {
        RelationshipRecord record = format.newRecord();
        record.setInUse( true );
        record.setFirstInFirstChain( firstInFirstChain );
        record.setFirstInSecondChain( firstInSecondChain );
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
