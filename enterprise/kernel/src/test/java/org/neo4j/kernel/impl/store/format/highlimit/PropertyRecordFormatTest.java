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
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class PropertyRecordFormatTest
{
    private static final int DATA_SIZE = 100;

    @Test
    public void writeAndReadRecordWithRelativeReferences() throws IOException
    {
        PropertyRecordFormat format = new PropertyRecordFormat();
        int recordSize = format.getRecordSize( new IntStoreHeader( DATA_SIZE ) );
        StubPageCursor cursor = new StubPageCursor( 0, (int) ByteUnit.kibiBytes( 4 ) );
        long recordId = 0xF1F1F1F1F1F1L;
        int recordOffset = cursor.getOffset();

        PropertyRecord record = createRecord( format, recordId );
        format.write( record, cursor, recordSize, null);

        PropertyRecord recordFromStore = format.newRecord();
        recordFromStore.setId( recordId  );
        resetCursor( cursor, recordOffset );
        format.read( recordFromStore, cursor, RecordLoad.NORMAL, recordSize, null );

        // records should be the same
        assertEquals( record.getNextProp(), recordFromStore.getNextProp() );
        assertEquals( record.getPrevProp(), recordFromStore.getPrevProp() );

        // now lets try to read same data into a record with different id - we should get different absolute references
        resetCursor( cursor, recordOffset );
        PropertyRecord recordWithOtherId = format.newRecord();
        recordWithOtherId.setId( 1L  );
        format.read( recordWithOtherId, cursor, RecordLoad.NORMAL, recordSize, null );

        assertNotEquals( record.getNextProp(), recordWithOtherId.getNextProp() );
        assertNotEquals( record.getPrevProp(), recordWithOtherId.getPrevProp() );
    }

    private void resetCursor( StubPageCursor cursor, int recordOffset )
    {
        cursor.setOffset( recordOffset );
    }

    private PropertyRecord createRecord( PropertyRecordFormat format, long recordId )
    {
        PropertyRecord record = format.newRecord();
        record.setInUse( true );
        record.setId( recordId );
        record.setNextProp( 1L );
        record.setPrevProp( 3L );
        return record;
    }
}
