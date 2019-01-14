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

import java.io.IOException;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.format.BaseOneByteHeaderRecordFormat;
import org.neo4j.kernel.impl.store.format.BaseRecordFormat;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

public class RelationshipRecordFormat extends BaseOneByteHeaderRecordFormat<RelationshipRecord>
{
    // record header size
    // directed|in_use(byte)+first_node(int)+second_node(int)+rel_type(int)+
    // first_prev_rel_id(int)+first_next_rel_id+second_prev_rel_id(int)+
    // second_next_rel_id+next_prop_id(int)+first-in-chain-markers(1)
    public static final int RECORD_SIZE = 34;

    public RelationshipRecordFormat()
    {
        super( fixedRecordSize( RECORD_SIZE ), 0, IN_USE_BIT, StandardFormatSettings.RELATIONSHIP_MAXIMUM_ID_BITS );
    }

    @Override
    public RelationshipRecord newRecord()
    {
        return new RelationshipRecord( -1 );
    }

    @Override
    public void read( RelationshipRecord record, PageCursor cursor, RecordLoad mode, int recordSize )
    {
        byte headerByte = cursor.getByte();
        boolean inUse = isInUse( headerByte );
        record.setInUse( inUse );
        if ( mode.shouldLoad( inUse ) )
        {
            // [    ,   x] in use flag
            // [    ,xxx ] first node high order bits
            // [xxxx,    ] next prop high order bits
            long firstNode = cursor.getInt() & 0xFFFFFFFFL;
            long firstNodeMod = (headerByte & 0xEL) << 31;

            long secondNode = cursor.getInt() & 0xFFFFFFFFL;

            // [ xxx,    ][    ,    ][    ,    ][    ,    ] second node high order bits,     0x70000000
            // [    ,xxx ][    ,    ][    ,    ][    ,    ] first prev rel high order bits,  0xE000000
            // [    ,   x][xx  ,    ][    ,    ][    ,    ] first next rel high order bits,  0x1C00000
            // [    ,    ][  xx,x   ][    ,    ][    ,    ] second prev rel high order bits, 0x380000
            // [    ,    ][    , xxx][    ,    ][    ,    ] second next rel high order bits, 0x70000
            // [    ,    ][    ,    ][xxxx,xxxx][xxxx,xxxx] type
            long typeInt = cursor.getInt();
            long secondNodeMod = (typeInt & 0x70000000L) << 4;
            int type = (int)(typeInt & 0xFFFF);

            long firstPrevRel = cursor.getInt() & 0xFFFFFFFFL;
            long firstPrevRelMod = (typeInt & 0xE000000L) << 7;

            long firstNextRel = cursor.getInt() & 0xFFFFFFFFL;
            long firstNextRelMod = (typeInt & 0x1C00000L) << 10;

            long secondPrevRel = cursor.getInt() & 0xFFFFFFFFL;
            long secondPrevRelMod = (typeInt & 0x380000L) << 13;

            long secondNextRel = cursor.getInt() & 0xFFFFFFFFL;
            long secondNextRelMod = (typeInt & 0x70000L) << 16;

            long nextProp = cursor.getInt() & 0xFFFFFFFFL;
            long nextPropMod = (headerByte & 0xF0L) << 28;

            byte extraByte = cursor.getByte();

            record.initialize( inUse,
                    BaseRecordFormat.longFromIntAndMod( nextProp, nextPropMod ),
                    BaseRecordFormat.longFromIntAndMod( firstNode, firstNodeMod ),
                    BaseRecordFormat.longFromIntAndMod( secondNode, secondNodeMod ),
                    type,
                    BaseRecordFormat.longFromIntAndMod( firstPrevRel, firstPrevRelMod ),
                    BaseRecordFormat.longFromIntAndMod( firstNextRel, firstNextRelMod ),
                    BaseRecordFormat.longFromIntAndMod( secondPrevRel, secondPrevRelMod ),
                    BaseRecordFormat.longFromIntAndMod( secondNextRel, secondNextRelMod ),
                    (extraByte & 0x1) != 0,
                    (extraByte & 0x2) != 0 );
        }
    }

    @Override
    public void write( RelationshipRecord record, PageCursor cursor, int recordSize )
    {
        if ( record.inUse() )
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
            short inUseUnsignedByte = (short) ((record.inUse() ? Record.IN_USE :
                                                Record.NOT_IN_USE).byteValue() | firstNodeMod | nextPropMod);

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

            cursor.putByte( (byte)inUseUnsignedByte );
            cursor.putInt( (int) firstNode );
            cursor.putInt( (int) secondNode );
            cursor.putInt( typeInt );
            cursor.putInt( (int) firstPrevRel );
            cursor.putInt( (int) firstNextRel );
            cursor.putInt( (int) secondPrevRel );
            cursor.putInt( (int) secondNextRel );
            cursor.putInt( (int) nextProp );
            cursor.putByte( extraByte );
        }
        else
        {
            markAsUnused( cursor );
        }
    }
}
