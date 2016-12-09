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

import java.io.IOException;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.format.BaseRecordFormat;
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
 * => 18B-43B
 *
 * Fixed reference format:
 * 1B   header
 * 1B   modifiers
 * 2B   relationship type
 * 4B   next relationship
 * 4B   first outgoing relationship
 * 4B   first incoming relationship
 * 4B   first loop
 * 4B   owning node
 * => 24B
 */
public class RelationshipGroupRecordFormat extends BaseHighLimitRecordFormat<RelationshipGroupRecord>
{
    static final int RECORD_SIZE = 32;
    static final int FIXED_FORMAT_RECORD_SIZE = HEADER_BYTE +
                                                Byte.BYTES /* modifiers */ +
                                                Short.BYTES /* type */ +
                                                Integer.BYTES /* next */ +
                                                Integer.BYTES /* first out */ +
                                                Integer.BYTES /* first in */ +
                                                Integer.BYTES /* first loop */ +
                                                Integer.BYTES /* owning node */;

    private static final int HAS_OUTGOING_BIT = 0b0000_1000;
    private static final int HAS_INCOMING_BIT = 0b0001_0000;
    private static final int HAS_LOOP_BIT     = 0b0010_0000;
    private static final int HAS_NEXT_BIT     = 0b0100_0000;

    private static final int NEXT_RECORD_BIT = 0b0000_0001;
    private static final int FIRST_OUT_BIT = 0b0000_0010;
    private static final int FIRST_IN_BIT = 0b0000_0100;
    private static final int FIRST_LOOP_BIT = 0b0000_1000;
    private static final int OWNING_NODE_BIT = 0b0001_0000;

    private static final long ONE_BIT_OVERFLOW_BIT_MASK = 0xFFFF_FFFE_0000_0000L;
    private static final long HIGH_DWORD_LAST_BIT_MASK = 0x100000000L;

    public RelationshipGroupRecordFormat()
    {
        this( RECORD_SIZE );
    }

    RelationshipGroupRecordFormat( int recordSize )
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
        if ( record.isUseFixedReferences() )
        {
            // read record in fixed references format
            readFixedReferencesMethod( record, cursor, inUse );
            record.setUseFixedReferences( true );
        }
        else
        {
            record.initialize( inUse,
                    cursor.getShort() & 0xFFFF,
                    decodeCompressedReference( cursor, headerByte, HAS_OUTGOING_BIT, NULL ),
                    decodeCompressedReference( cursor, headerByte, HAS_INCOMING_BIT, NULL ),
                    decodeCompressedReference( cursor, headerByte, HAS_LOOP_BIT, NULL ),
                    decodeCompressedReference( cursor ),
                    decodeCompressedReference( cursor, headerByte, HAS_NEXT_BIT, NULL ) );
        }
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
            throws IOException
    {
        if ( record.isUseFixedReferences() )
        {
            // write record in fixed references format
            writeFixedReferencesRecord( record, cursor );
        }
        else
        {
            cursor.putShort( (short) record.getType() );
            encode( cursor, record.getFirstOut(), NULL );
            encode( cursor, record.getFirstIn(), NULL );
            encode( cursor, record.getFirstLoop(), NULL );
            encode( cursor, record.getOwningNode() );
            encode( cursor, record.getNext(), NULL );
        }
    }

    @Override
    protected boolean canUseFixedReferences( RelationshipGroupRecord record, int recordSize )
    {
        return (isRecordBigEnoughForFixedReferences( recordSize ) &&
                ((record.getNext() == NULL) || ((record.getNext() & ONE_BIT_OVERFLOW_BIT_MASK) == 0)) &&
                ((record.getFirstOut() == NULL) || ((record.getFirstOut() & ONE_BIT_OVERFLOW_BIT_MASK) == 0)) &&
                ((record.getFirstIn() == NULL) || ((record.getFirstIn() & ONE_BIT_OVERFLOW_BIT_MASK) == 0)) &&
                ((record.getFirstLoop() == NULL) || ((record.getFirstLoop() & ONE_BIT_OVERFLOW_BIT_MASK) == 0)) &&
                ((record.getOwningNode() == NULL) || ((record.getOwningNode() & ONE_BIT_OVERFLOW_BIT_MASK) == 0)));
    }

    private boolean isRecordBigEnoughForFixedReferences( int recordSize )
    {
        return FIXED_FORMAT_RECORD_SIZE <= recordSize;
    }

    private void readFixedReferencesMethod( RelationshipGroupRecord record, PageCursor cursor, boolean inUse )
    {
        // [    ,   x] high next bits
        // [    ,  x ] high firstOut bits
        // [    , x  ] high firstIn bits
        // [    ,x   ] high firstLoop bits
        // [   x,    ] high owner bits
        long modifiers = cursor.getByte();

        int type = cursor.getShort() & 0xFFFF;

        long nextLowBits = cursor.getInt() & 0xFFFFFFFFL;
        long firstOutLowBits = cursor.getInt() & 0xFFFFFFFFL;
        long firstInLowBits = cursor.getInt() & 0xFFFFFFFFL;
        long firstLoopLowBits = cursor.getInt() & 0xFFFFFFFFL;
        long owningNodeLowBits = cursor.getInt() & 0xFFFFFFFFL;

        long nextMod = (modifiers & NEXT_RECORD_BIT) << 32;
        long firstOutMod = (modifiers & FIRST_OUT_BIT) << 31;
        long firstInMod = (modifiers & FIRST_IN_BIT) << 30;
        long firstLoopMod = (modifiers & FIRST_LOOP_BIT) << 29;
        long owningNodeMod = (modifiers & OWNING_NODE_BIT) << 28;

        record.initialize( inUse, type,
                BaseRecordFormat.longFromIntAndMod( firstOutLowBits, firstOutMod ),
                BaseRecordFormat.longFromIntAndMod( firstInLowBits, firstInMod ),
                BaseRecordFormat.longFromIntAndMod( firstLoopLowBits, firstLoopMod ),
                BaseRecordFormat.longFromIntAndMod( owningNodeLowBits, owningNodeMod ),
                BaseRecordFormat.longFromIntAndMod( nextLowBits, nextMod ) );
    }

    private void writeFixedReferencesRecord( RelationshipGroupRecord record, PageCursor cursor )
    {
        long nextMod = record.getNext() == NULL ? 0 : (record.getNext() & HIGH_DWORD_LAST_BIT_MASK) >> 32;
        long firstOutMod = record.getFirstOut() == NULL ? 0 : (record.getFirstOut() & HIGH_DWORD_LAST_BIT_MASK) >> 31;
        long firstInMod = record.getFirstIn() == NULL ? 0 : (record.getFirstIn() & HIGH_DWORD_LAST_BIT_MASK) >> 30;
        long firstLoopMod = record.getFirstLoop() == NULL ? 0 : (record.getFirstLoop() & HIGH_DWORD_LAST_BIT_MASK) >> 29;
        long owningNodeMod = record.getOwningNode() == NULL ? 0 : (record.getOwningNode() & HIGH_DWORD_LAST_BIT_MASK) >> 28;

        // [    ,   x] high next bits
        // [    ,  x ] high firstOut bits
        // [    , x  ] high firstIn bits
        // [    ,x   ] high firstLoop bits
        // [   x,    ] high owner bits
        cursor.putByte( (byte) (nextMod | firstOutMod | firstInMod | firstLoopMod | owningNodeMod) );

        cursor.putShort( (short) record.getType() );
        cursor.putInt( (int) record.getNext() );
        cursor.putInt( (int) record.getFirstOut() );
        cursor.putInt( (int) record.getFirstIn() );
        cursor.putInt( (int) record.getFirstLoop() );
        cursor.putInt( (int) record.getOwningNode() );
    }
}
