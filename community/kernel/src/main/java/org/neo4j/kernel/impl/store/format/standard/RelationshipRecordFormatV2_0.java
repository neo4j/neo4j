/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.store.format.standard;

import java.io.IOException;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.format.BaseOneByteHeaderRecordFormat;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

class RelationshipRecordFormatV2_0 extends BaseOneByteHeaderRecordFormat<RelationshipRecord>
{
    RelationshipRecordFormatV2_0()
    {
        super( fixedRecordSize( 33 ), 0, IN_USE_BIT, StandardFormatSettings.RELATIONSHIP_MAXIMUM_ID_BITS );
    }

    @Override
    public RelationshipRecord newRecord()
    {
        return new RelationshipRecord( -1 );
    }

    @Override
    public void read( RelationshipRecord record, PageCursor cursor, RecordLoad mode, int recordSize ) throws IOException
    {
        long headerByte = cursor.getByte();
        boolean inUse = isInUse( (byte) headerByte );
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

            record.initialize( inUse,
                    longFromIntAndMod( nextProp, nextPropMod ),
                    longFromIntAndMod( firstNode, firstNodeMod ),
                    longFromIntAndMod( secondNode, secondNodeMod ),
                    type,
                    longFromIntAndMod( firstPrevRel, firstPrevRelMod ),
                    longFromIntAndMod( firstNextRel, firstNextRelMod ),
                    longFromIntAndMod( secondPrevRel, secondPrevRelMod ),
                    longFromIntAndMod( secondNextRel, secondNextRelMod ),
                    false, false );
        }
    }

    @Override
    public void write( RelationshipRecord record, PageCursor cursor, int recordSize )
            throws IOException
    {
        throw new UnsupportedOperationException();
    }
}
