/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.store.format.v2_2;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.impl.store.CommonAbstractStore;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.standard.BaseRecordCursor;
import org.neo4j.kernel.impl.store.standard.FixedSizeRecordStoreFormat;
import org.neo4j.kernel.impl.store.standard.StoreFormat;
import org.neo4j.kernel.impl.store.standard.StoreToolkit;

import static org.neo4j.kernel.impl.store.format.NeoStoreFormatUtils.longFromIntAndMod;

public class RelationshipGroupStoreFormat_v2_2 extends FixedSizeRecordStoreFormat<RelationshipGroupRecord, RelationshipGroupStoreFormat_v2_2.RelationshipGroupRecordCursor>
{
    private final RelationshipGroupRecordFormat recordFormat;

    public RelationshipGroupStoreFormat_v2_2()
    {
        super( RelationshipGroupRecordFormat.RECORD_SIZE, "RelationshipStore", CommonAbstractStore.ALL_STORES_VERSION );
        this.recordFormat = new RelationshipGroupRecordFormat();
    }

    @Override
    public RelationshipGroupRecordCursor createCursor( PagedFile file, StoreToolkit toolkit, int flags )
    {
        return new RelationshipGroupRecordCursor( file, toolkit, recordFormat, flags );
    }

    @Override
    public StoreFormat.RecordFormat<RelationshipGroupRecord> recordFormat()
    {
        return recordFormat;
    }

    /** Full definition of the record format */
    public static class RelationshipGroupRecordFormat implements StoreFormat.RecordFormat<RelationshipGroupRecord>
    {
        private final static int IN_USE                = 0;
        private final static int HIGH_BYTE             = 1 + IN_USE;
        private final static int TYPE                  = 1 + HIGH_BYTE;
        private final static int NEXT_LOW_BITS         = 2 + TYPE;
        private final static int NEXT_OUT_LOW_BITS     = 4 + NEXT_LOW_BITS;
        private final static int NEXT_IN_LOW_BITS      = 4 + NEXT_OUT_LOW_BITS;
        private final static int NEXT_LOOP_LOW_BITS    = 4 + NEXT_IN_LOW_BITS;
        private final static int OWNER_LOW_BITS        = 4 + NEXT_LOOP_LOW_BITS;
        private final static int OWNER_HIGH_BITS       = 4 + OWNER_LOW_BITS;

        private final static int RECORD_SIZE = OWNER_HIGH_BITS + 1;

        @Override
        public String recordName()
        {
            return "RelationshipGroupRecord";
        }

        @Override
        public long id( RelationshipGroupRecord record )
        {
            return record.getId();
        }

        @Override
        public RelationshipGroupRecord newRecord( long id )
        {
            return new RelationshipGroupRecord( id, -1 );
        }

        @Override
        public void serialize( PageCursor cursor, int offset, RelationshipGroupRecord record )
        {
            if(record.inUse())
            {

                long nextMod = record.getNext() == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (record.getNext() & 0x700000000L) >> 31;
                long nextOutMod = record.getFirstOut() == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (record.getFirstOut() & 0x700000000L) >> 28;
                long nextInMod = record.getFirstIn() == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (record.getFirstIn() & 0x700000000L) >> 31;
                long nextLoopMod = record.getFirstLoop() == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (record.getFirstLoop() & 0x700000000L) >> 28;

                // [    ,   x] in use
                // [    ,xxx ] high next id bits
                // [ xxx,    ] high firstOut bits
                cursor.putByte(  offset + IN_USE,             (byte) (nextOutMod | nextMod | 1) );
                // [    ,xxx ] high firstIn bits
                // [ xxx,    ] high firstLoop bits
                cursor.putByte(  offset + HIGH_BYTE,          (byte) (nextLoopMod | nextInMod) );
                cursor.putShort( offset + TYPE,               (short) record.getType() );
                cursor.putInt(   offset + NEXT_LOW_BITS,      (int) record.getNext() );
                cursor.putInt(   offset + NEXT_OUT_LOW_BITS,  (int) record.getFirstOut() );
                cursor.putInt(   offset + NEXT_IN_LOW_BITS,   (int) record.getFirstIn() );
                cursor.putInt(   offset + NEXT_LOOP_LOW_BITS, (int) record.getFirstLoop() );
                cursor.putInt(   offset + OWNER_LOW_BITS,     (int) record.getOwningNode() );
                cursor.putByte(  offset + OWNER_HIGH_BITS,    (byte) (record.getOwningNode() >> 32) );
            }
            else
            {
                cursor.putByte( offset + IN_USE, (byte)0);
            }
        }

        @Override
        public void deserialize( PageCursor cursor, int offset, long id, RelationshipGroupRecord record )
        {
            // [    ,   x] in use
            // [    ,xxx ] high next id bits
            // [ xxx,    ] high firstOut bits
            long inUseByte       = cursor.getByte(offset + IN_USE);
            // [    ,xxx ] high firstIn bits
            // [ xxx,    ] high firstLoop bits
            long highByte        = cursor.getByte(offset + HIGH_BYTE);
            int type             = cursor.getShort(offset + TYPE);
            long nextLowBits     = cursor.getUnsignedInt(offset + NEXT_LOW_BITS);
            long nextOutLowBits  = cursor.getUnsignedInt(offset + NEXT_OUT_LOW_BITS);
            long nextInLowBits   = cursor.getUnsignedInt(offset + NEXT_IN_LOW_BITS);
            long nextLoopLowBits = cursor.getUnsignedInt(offset + NEXT_LOOP_LOW_BITS);
            long ownerLowBits    = cursor.getUnsignedInt(offset + OWNER_LOW_BITS );
            byte ownerHighBits   = cursor.getByte(offset + OWNER_HIGH_BITS );

            record.setId( id );
            record.setType( type );

            record.setInUse( (inUseByte&0x1) > 0 );
            record.setNext(      longFromIntAndMod( nextLowBits,     (inUseByte & 0xE) << 31 ) );
            record.setFirstOut(  longFromIntAndMod( nextOutLowBits,  (inUseByte & 0x70) << 28 ) );
            record.setFirstIn(   longFromIntAndMod( nextInLowBits,   (highByte & 0xE) << 31 ) );
            record.setFirstLoop( longFromIntAndMod( nextLoopLowBits, (highByte & 0x70) << 28 ) );
            record.setOwningNode( ownerLowBits | (((long) ownerHighBits) << 32) );
        }

        @Override
        public boolean inUse( PageCursor cursor, int offset )
        {
            return (cursor.getByte( offset + IN_USE ) & 0x1) == 1;
        }
    }

    /**
     * This is our custom record cursor, extending {@link org.neo4j.kernel.impl.store.standard.BaseRecordCursor} to
     * get required common functionality, but adding some custom field-reading of our own.
     */
    public static class RelationshipGroupRecordCursor extends BaseRecordCursor<RelationshipGroupRecord, RelationshipGroupRecordFormat>
    {
        public RelationshipGroupRecordCursor( PagedFile file, StoreToolkit toolkit, RelationshipGroupRecordFormat format,
                                              int flags )
        {
            super( file, toolkit, format, flags );
        }

        // TODO: Add field-reading methods here to allow traversing rels without creating relationship record objects
    }
}
