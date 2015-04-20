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
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.standard.BaseRecordCursor;
import org.neo4j.kernel.impl.store.standard.FixedSizeRecordStoreFormat;
import org.neo4j.kernel.impl.store.standard.StoreFormat;
import org.neo4j.kernel.impl.store.standard.StoreToolkit;

import static org.neo4j.kernel.impl.store.format.NeoStoreFormatUtils.longFromIntAndMod;

public class RelationshipStoreFormat_v2_2 extends FixedSizeRecordStoreFormat<RelationshipRecord, RelationshipStoreFormat_v2_2.RelationshipRecordCursor>
{
    private final RelationshipRecordFormat recordFormat;

    public RelationshipStoreFormat_v2_2()
    {
        super( RelationshipRecordFormat.RECORD_SIZE, "RelationshipStore", CommonAbstractStore.ALL_STORES_VERSION );
        this.recordFormat = new RelationshipRecordFormat();
    }

    @Override
    public RelationshipRecordCursor createCursor( PagedFile file, StoreToolkit toolkit, int flags )
    {
        return new RelationshipRecordCursor( file, toolkit, recordFormat, flags );
    }

    @Override
    public StoreFormat.RecordFormat<RelationshipRecord> recordFormat()
    {
        return recordFormat;
    }

    /** Full definition of the record format */
    public static class RelationshipRecordFormat implements StoreFormat.RecordFormat<RelationshipRecord>
    {
        private final static int IN_USE          = 0;
        private final static int FIRST_NODE      = 1 + IN_USE ;
        private final static int SECOND_NODE     = 4 + FIRST_NODE;
        private final static int TYPE            = 4 + SECOND_NODE;
        private final static int FIRST_PREV_REL  = 4 + TYPE;
        private final static int FIRST_NEXT_REL  = 4 + FIRST_PREV_REL;
        private final static int SECOND_PREV_REL = 4 + FIRST_NEXT_REL;
        private final static int SECOND_NEXT_REL = 4 + SECOND_PREV_REL;
        private final static int NEXT_PROP       = 4 + SECOND_NEXT_REL;
        private final static int EXTRA           = 4 + NEXT_PROP;

        private final static int RECORD_SIZE = EXTRA + 1;

        @Override
        public String recordName()
        {
            return "RelationshipRecord";
        }

        @Override
        public long id( RelationshipRecord record )
        {
            return record.getId();
        }

        @Override
        public RelationshipRecord newRecord( long id )
        {
            return new RelationshipRecord( id );
        }

        @Override
        public void serialize( PageCursor cursor, int offset, RelationshipRecord record )
        {
            if(record.inUse())
            {
                long firstNode = record.getFirstNode();
                short firstNodeMod = (short)((firstNode & 0x700000000L) >> 31);

                long secondNode = record.getSecondNode();
                long secondNodeMod = (secondNode & 0x700000000L) >> 4;

                long firstPrevRel = record.getFirstPrevRel();
                long firstPrevRelMod = firstPrevRel == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (firstPrevRel & 0x700000000L) >> 7;

                long firstNextRel = record.getFirstNextRel();
                long firstNextRelMod = firstNextRel == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (firstNextRel & 0x700000000L) >> 10;

                long secondPrevRel = record.getSecondPrevRel();
                long secondPrevRelMod = secondPrevRel == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (secondPrevRel & 0x700000000L) >> 13;

                long secondNextRel = record.getSecondNextRel();
                long secondNextRelMod = secondNextRel == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (secondNextRel & 0x700000000L) >> 16;

                long nextProp = record.getNextProp();
                long nextPropMod = nextProp == Record.NO_NEXT_PROPERTY.intValue() ? 0 : (nextProp & 0xF00000000L) >> 28;

                // [    ,   x] in use flag
                // [    ,xxx ] first node high order bits
                // [xxxx,    ] next prop high order bits
                short inUseUnsignedByte = (short)((record.inUse() ? Record.IN_USE : Record.NOT_IN_USE).byteValue() | firstNodeMod | nextPropMod);

                // [ xxx,    ][    ,    ][    ,    ][    ,    ] second node high order bits,     0x70000000
                // [    ,xxx ][    ,    ][    ,    ][    ,    ] first prev rel high order bits,  0xE000000
                // [    ,   x][xx  ,    ][    ,    ][    ,    ] first next rel high order bits,  0x1C00000
                // [    ,    ][  xx,x   ][    ,    ][    ,    ] second prev rel high order bits, 0x380000
                // [    ,    ][    , xxx][    ,    ][    ,    ] second next rel high order bits, 0x70000
                // [    ,    ][    ,    ][xxxx,xxxx][xxxx,xxxx] type
                int typeInt = (int)(record.getType() | secondNodeMod | firstPrevRelMod | firstNextRelMod | secondPrevRelMod | secondNextRelMod);

                // [    ,   x] 1:st in start node chain, 0x1
                // [    ,  x ] 1:st in end node chain,   0x2
                long firstInStartNodeChain = record.isFirstInFirstChain() ? 0x1 : 0;
                long firstInEndNodeChain = record.isFirstInSecondChain() ? 0x2 : 0;
                byte extraByte = (byte) (firstInEndNodeChain | firstInStartNodeChain);

                cursor.putByte( offset + IN_USE         , (byte)inUseUnsignedByte );
                cursor.putInt(  offset + FIRST_NODE     , (int) firstNode );
                cursor.putInt(  offset + SECOND_NODE    , (int) secondNode );
                cursor.putInt(  offset + TYPE           , typeInt );
                cursor.putInt(  offset + FIRST_PREV_REL , (int) firstPrevRel );
                cursor.putInt(  offset + FIRST_NEXT_REL , (int) firstNextRel );
                cursor.putInt(  offset + SECOND_PREV_REL, (int) secondPrevRel );
                cursor.putInt(  offset + SECOND_NEXT_REL, (int) secondNextRel );
                cursor.putInt(  offset + NEXT_PROP      , (int) nextProp );
                cursor.putByte( offset + EXTRA          , extraByte );
            }
            else
            {
                cursor.putByte( offset + IN_USE, (byte)0);
            }
        }

        @Override
        public void deserialize( PageCursor cursor, int offset, long id, RelationshipRecord record )
        {
            record.setId( id );

            // [    ,   x] in use flag
            // [    ,xxx ] first node high order bits
            // [xxxx,    ] next prop high order bits
            long inUseByte  = cursor.getByte(       offset + IN_USE );
            long firstNode  = cursor.getUnsignedInt(offset + FIRST_NODE );
            long secondNode = cursor.getUnsignedInt(offset + SECOND_NODE );
            // [ xxx,    ][    ,    ][    ,    ][    ,    ] second node high order bits,     0x70000000
            // [    ,xxx ][    ,    ][    ,    ][    ,    ] first prev rel high order bits,  0xE000000
            // [    ,   x][xx  ,    ][    ,    ][    ,    ] first next rel high order bits,  0x1C00000
            // [    ,    ][  xx,x   ][    ,    ][    ,    ] second prev rel high order bits, 0x380000
            // [    ,    ][    , xxx][    ,    ][    ,    ] second next rel high order bits, 0x70000
            // [    ,    ][    ,    ][xxxx,xxxx][xxxx,xxxx] type
            long typeInt       = cursor.getInt(         offset + TYPE );
            long firstPrevRel  = cursor.getUnsignedInt( offset + FIRST_PREV_REL );
            long firstNextRel  = cursor.getUnsignedInt( offset + FIRST_NEXT_REL );
            long secondPrevRel = cursor.getUnsignedInt( offset + SECOND_PREV_REL );
            long secondNextRel = cursor.getUnsignedInt( offset + SECOND_NEXT_REL );
            long nextProp      = cursor.getUnsignedInt( offset + NEXT_PROP );
            byte extraByte     = cursor.getByte( offset + EXTRA );

            record.setFirstNode( longFromIntAndMod( firstNode, (inUseByte & 0xEL) << 31 ) );
            record.setSecondNode( longFromIntAndMod( secondNode, (typeInt & 0x70000000L) << 4 ) );
            record.setType( (int)(typeInt & 0xFFFF) );
            record.setInUse( (inUseByte & 0x1) == Record.IN_USE.intValue() );
            record.setFirstPrevRel( longFromIntAndMod( firstPrevRel, (typeInt & 0xE000000L) << 7 ) );
            record.setFirstNextRel( longFromIntAndMod( firstNextRel, (typeInt & 0x1C00000L) << 10 ) );
            record.setSecondPrevRel( longFromIntAndMod( secondPrevRel, (typeInt & 0x380000L) << 13 ) );
            record.setSecondNextRel( longFromIntAndMod( secondNextRel, (typeInt & 0x70000L) << 16 ) );
            record.setFirstInFirstChain( (extraByte & 0x1) != 0 );
            record.setFirstInSecondChain( (extraByte & 0x2) != 0 );
            record.setNextProp( longFromIntAndMod( nextProp, (inUseByte & 0xF0L) << 28 ) );
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
    public static class RelationshipRecordCursor extends BaseRecordCursor<RelationshipRecord, RelationshipRecordFormat>
    {
        public RelationshipRecordCursor( PagedFile file, StoreToolkit toolkit, RelationshipRecordFormat format, int flags )
        {
            super( file, toolkit, format, flags );
        }

        // TODO: Add field-reading methods here to allow traversing rels without creating relationship record objects
    }
}