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
package org.neo4j.kernel.impl.store.format.highlimit.v300;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;

/**
 * LEGEND:
 * V: variable between 3B-8B
 *
 * Record format:
 * 1B   header
 * 2B   relationship type
 * VB   first outgoing relationships
 * VB   first incoming relationships
 * VB   first loop relationships
 * VB   owning node
 * VB   next relationship group record
 *
 * => 18B-43B
 */
public class RelationshipGroupRecordFormatV3_0_0 extends BaseHighLimitRecordFormatV3_0_0<RelationshipGroupRecord>
{
    public static final int RECORD_SIZE = 32;

    private static final int HAS_OUTGOING_BIT = 0b0000_1000;
    private static final int HAS_INCOMING_BIT = 0b0001_0000;
    private static final int HAS_LOOP_BIT     = 0b0010_0000;
    private static final int HAS_NEXT_BIT     = 0b0100_0000;

    public RelationshipGroupRecordFormatV3_0_0()
    {
        this( RECORD_SIZE );
    }

    private RelationshipGroupRecordFormatV3_0_0( int recordSize )
    {
        super( fixedRecordSize( recordSize ), 0 );
    }

    @Override
    public RelationshipGroupRecord newRecord()
    {
        return new RelationshipGroupRecord( -1 );
    }

    @Override
    protected void doReadInternal( RelationshipGroupRecord record, PageCursor cursor, int recordSize, long headerByte,
            boolean inUse )
    {
        record.initialize( inUse,
                cursor.getShort() & 0xFFFF,
                decodeCompressedReference( cursor, headerByte, HAS_OUTGOING_BIT, NULL ),
                decodeCompressedReference( cursor, headerByte, HAS_INCOMING_BIT, NULL ),
                decodeCompressedReference( cursor, headerByte, HAS_LOOP_BIT, NULL ),
                decodeCompressedReference( cursor ),
                decodeCompressedReference( cursor, headerByte, HAS_NEXT_BIT, NULL ) );
    }

    @Override
    protected byte headerBits( RelationshipGroupRecord record )
    {
        byte header = 0;
        header = set( header, HAS_OUTGOING_BIT, record.getFirstOut(), NULL );
        header = set( header, HAS_INCOMING_BIT, record.getFirstIn(), NULL );
        header = set( header, HAS_LOOP_BIT, record.getFirstLoop(), NULL );
        header = set( header, HAS_NEXT_BIT, record.getNext(), NULL );
        return header;
    }

    @Override
    protected int requiredDataLength( RelationshipGroupRecord record )
    {
        return  2 + // type
                length( record.getFirstOut(), NULL ) +
                length( record.getFirstIn(), NULL ) +
                length( record.getFirstLoop(), NULL ) +
                length( record.getOwningNode() ) +
                length( record.getNext(), NULL );
    }

    @Override
    protected void doWriteInternal( RelationshipGroupRecord record, PageCursor cursor )
    {
        cursor.putShort( (short) record.getType() );
        encode( cursor, record.getFirstOut(), NULL );
        encode( cursor, record.getFirstIn(), NULL );
        encode( cursor, record.getFirstLoop(), NULL );
        encode( cursor, record.getOwningNode() );
        encode( cursor, record.getNext(), NULL );
    }
}
