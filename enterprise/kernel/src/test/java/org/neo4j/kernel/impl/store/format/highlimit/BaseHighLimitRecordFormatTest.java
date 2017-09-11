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

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.StubPageCursor;
import org.neo4j.io.pagecache.StubPagedFile;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.impl.store.format.highlimit.BaseHighLimitRecordFormat.HEADER_BIT_FIRST_RECORD_UNIT;
import static org.neo4j.kernel.impl.store.format.highlimit.BaseHighLimitRecordFormat.HEADER_BIT_RECORD_UNIT;

public class BaseHighLimitRecordFormatTest
{
    @Test
    public void mustNotCheckForOutOfBoundsWhenReadingSingleRecord() throws Exception
    {
        MyRecordFormat format = new MyRecordFormat();
        StubPageCursor cursor = new StubPageCursor( 0, 3 );
        format.read( new MyRecord( 0 ), cursor, RecordLoad.NORMAL, 4 );
        assertFalse( cursor.checkAndClearBoundsFlag() );
    }

    @Test
    public void mustCheckForOutOfBoundsWhenReadingDoubleRecord() throws Exception
    {
        MyRecordFormat format = new MyRecordFormat();
        StubPageCursor cursor = new StubPageCursor( 0, 4 );
        cursor.putByte( 0, (byte) (HEADER_BIT_RECORD_UNIT + HEADER_BIT_FIRST_RECORD_UNIT) );
        StubPagedFile pagedFile = new StubPagedFile( 3 )
        {
            @Override
            protected void prepareCursor( StubPageCursor cursor )
            {
                cursor.putByte( 0, (byte) HEADER_BIT_RECORD_UNIT );
            }
        };
        format.shortsPerRecord.add( 2 );
        format.read( new MyRecord( 0 ), cursor, RecordLoad.NORMAL, 4 );
        assertTrue( cursor.checkAndClearBoundsFlag() );
    }

    @Test
    public void mustNotCheckForOutOfBoundsWhenWritingSingleRecord() throws Exception
    {
        MyRecordFormat format = new MyRecordFormat();
        StubPageCursor cursor = new StubPageCursor( 0, 3 );
        MyRecord record = new MyRecord( 0 );
        record.setInUse( true );
        format.write( record, cursor, 4 );
        assertFalse( cursor.checkAndClearBoundsFlag() );
    }

    @Test
    public void mustCheckForOutOfBoundsWhenWritingDoubleRecord() throws Exception
    {
        MyRecordFormat format = new MyRecordFormat();
        StubPageCursor cursor = new StubPageCursor( 0, 5 );
        MyRecord record = new MyRecord( 0 );
        record.setRequiresSecondaryUnit( true );
        record.setSecondaryUnitId( 42 );
        record.setInUse( true );
        format.shortsPerRecord.add( 3 ); // make the write go out of bounds
        format.write( record, cursor, 4 );
        assertTrue( cursor.checkAndClearBoundsFlag() );
    }

    private class MyRecordFormat extends BaseHighLimitRecordFormat<MyRecord>
    {
        private Queue<Integer> shortsPerRecord = new ConcurrentLinkedQueue<>();

        protected MyRecordFormat()
        {
            super( header -> 4, 4 );
        }

        @Override
        protected void doReadInternal( MyRecord record, PageCursor cursor, int recordSize,
                                       long inUseByte, boolean inUse )
        {
            int shortsPerRecord = getShortsPerRecord();
            for ( int i = 0; i < shortsPerRecord; i++ )
            {
                short v = (short) ((cursor.getByte() & 0xFF) << 8);
                v += cursor.getByte() & 0xFF;
                record.value = v;
            }
        }

        private int getShortsPerRecord()
        {
            Integer value = shortsPerRecord.poll();
            return value == null ? 1 : value;
        }

        @Override
        protected void doWriteInternal( MyRecord record, PageCursor cursor ) throws IOException
        {
            int intsPerRecord = getShortsPerRecord();
            for ( int i = 0; i < intsPerRecord; i++ )
            {
                short v = record.value;
                byte a = (byte) ((v & 0x0000FF00) >> 8);
                byte b = (byte)  (v & 0x000000FF);
                cursor.putByte( a );
                cursor.putByte( b );
            }
        }

        @Override
        protected byte headerBits( MyRecord record )
        {
            return 0;
        }

        @Override
        protected boolean canUseFixedReferences( MyRecord record, int recordSize )
        {
            return false;
        }

        @Override
        protected int requiredDataLength( MyRecord record )
        {
            return 4;
        }

        @Override
        public MyRecord newRecord()
        {
            return new MyRecord( 0 );
        }
    }

    private class MyRecord extends AbstractBaseRecord
    {
        public short value;

        protected MyRecord( long id )
        {
            super( id );
        }
    }
}
