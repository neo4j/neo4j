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
package org.neo4j.kernel.impl.store.format.highlimit.v320;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.format.BaseRecordFormat;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

import static org.neo4j.kernel.impl.store.format.highlimit.Reference.toAbsolute;
import static org.neo4j.kernel.impl.store.format.highlimit.Reference.toRelative;

/**
 * LEGEND:
 * V: variable between 3B-8B
 *
 * Record format:
 * 1B   header
 * 2B   relationship type
 * VB   first property
 * VB   start node
 * VB   end node
 * VB   start node chain previous relationship
 * VB   start node chain next relationship
 * VB   end node chain previous relationship
 * VB   end node chain next relationship
 * => 24B-59B
 *
 * Fixed reference format:
 * 1B   header
 * 2B   relationship type
 * 1B   modifiers
 * 4B   start node
 * 4B   end node
 * 4B   start node chain previous relationship
 * 4B   start node chain next relationship
 * 4B   end node chain previous relationship
 * 4B   end node chain next relationship
 * => 28B
 */
class RelationshipRecordFormatV3_2_0 extends BaseHighLimitRecordFormatV3_2_0<RelationshipRecord>
{
    static final int RECORD_SIZE = 32;
    static final int FIXED_FORMAT_RECORD_SIZE = HEADER_BYTE +
                                                Short.BYTES /* type */ +
                                                Byte.BYTES /* modifiers */ +
                                                Integer.BYTES /* start node */ +
                                                Integer.BYTES /* end node */ +
                                                Integer.BYTES /* first prev rel */ +
                                                Integer.BYTES /* first next rel */ +
                                                Integer.BYTES /* second prev rel */ +
                                                Integer.BYTES /* second next rel */ +
                                                Integer.BYTES /* next property */;

    private static final int TYPE_FIELD_BYTES = 3;

    private static final int FIRST_IN_FIRST_CHAIN_BIT = 0b0000_1000;
    private static final int FIRST_IN_SECOND_CHAIN_BIT = 0b0001_0000;
    private static final int HAS_FIRST_CHAIN_NEXT_BIT = 0b0010_0000;
    private static final int HAS_SECOND_CHAIN_NEXT_BIT = 0b0100_0000;
    private static final int HAS_PROPERTY_BIT = 0b1000_0000;

    private static final long FIRST_NODE_BIT = 0b0000_0001L;
    private static final long SECOND_NODE_BIT = 0b0000_0010L;
    private static final long FIRST_PREV_REL_BIT = 0b0000_0100L;
    private static final long FIRST_NEXT_REL_BIT = 0b0000_1000L;
    private static final long SECOND_RREV_REL_BIT = 0b0001_0000L;
    private static final long SECOND_NEXT_REL_BIT = 0b0010_0000L;
    private static final long NEXT_PROP_BIT = 0b1100_0000L;

    private static final long ONE_BIT_OVERFLOW_BIT_MASK = 0xFFFF_FFFE_0000_0000L;
    private static final long THREE_BITS_OVERFLOW_BIT_MASK = 0xFFFF_FFFC_0000_0000L;
    private static final long HIGH_DWORD_LAST_BIT_MASK = 0x100000000L;

    private static final long TWO_BIT_FIXED_REFERENCE_BIT_MASK = 0x300000000L;

    RelationshipRecordFormatV3_2_0()
    {
        this( RECORD_SIZE );
    }

    RelationshipRecordFormatV3_2_0( int recordSize )
    {
        super( fixedRecordSize( recordSize ), 0, HighLimitFormatSettingsV3_2_0.RELATIONSHIP_MAXIMUM_ID_BITS );
    }

    @Override
    public RelationshipRecord newRecord()
    {
        return new RelationshipRecord( -1 );
    }

    @Override
    protected void doReadInternal(
            RelationshipRecord record, PageCursor cursor, int recordSize, long headerByte, boolean inUse )
    {
        if ( record.isUseFixedReferences() )
        {
            int type = cursor.getShort() & 0xFFFF;
            // read record in fixed reference format
            readFixedReferencesRecord( record, cursor, headerByte, inUse, type );
            record.setUseFixedReferences( true );
        }
        else
        {
            int typeLowWord = cursor.getShort() & 0xFFFF;
            int typeHighWord = cursor.getByte() & 0xFF;
            int type = (typeHighWord << Short.SIZE) | typeLowWord;
            long recordId = record.getId();
            record.initialize( inUse,
                    decodeCompressedReference( cursor, headerByte, HAS_PROPERTY_BIT, NULL ),
                    decodeCompressedReference( cursor ),
                    decodeCompressedReference( cursor ),
                    type,
                    decodeAbsoluteOrRelative( cursor, headerByte, FIRST_IN_FIRST_CHAIN_BIT, recordId ),
                    decodeAbsoluteIfPresent( cursor, headerByte, HAS_FIRST_CHAIN_NEXT_BIT, recordId ),
                    decodeAbsoluteOrRelative( cursor, headerByte, FIRST_IN_SECOND_CHAIN_BIT, recordId ),
                    decodeAbsoluteIfPresent( cursor, headerByte, HAS_SECOND_CHAIN_NEXT_BIT, recordId ),
                    has( headerByte, FIRST_IN_FIRST_CHAIN_BIT ),
                    has( headerByte, FIRST_IN_SECOND_CHAIN_BIT ) );
        }
    }

    @Override
    protected byte headerBits( RelationshipRecord record )
    {
        byte header = 0;
        header = set( header, FIRST_IN_FIRST_CHAIN_BIT, record.isFirstInFirstChain() );
        header = set( header, FIRST_IN_SECOND_CHAIN_BIT, record.isFirstInSecondChain() );
        header = set( header, HAS_PROPERTY_BIT, record.getNextProp(), NULL );
        header = set( header, HAS_FIRST_CHAIN_NEXT_BIT, record.getFirstNextRel(), NULL );
        header = set( header, HAS_SECOND_CHAIN_NEXT_BIT, record.getSecondNextRel(), NULL );
        return header;
    }

    @Override
    protected int requiredDataLength( RelationshipRecord record )
    {
        long recordId = record.getId();
        return TYPE_FIELD_BYTES +
               length( record.getNextProp(), NULL ) +
               length( record.getFirstNode() ) +
               length( record.getSecondNode() ) +
               length( getFirstPrevReference( record, recordId ) ) +
               getRelativeReferenceLength( record.getFirstNextRel(), recordId ) +
               length( getSecondPrevReference( record, recordId ) ) +
               getRelativeReferenceLength( record.getSecondNextRel(), recordId );
    }

    @Override
    protected void doWriteInternal( RelationshipRecord record, PageCursor cursor )
    {
        if ( record.isUseFixedReferences() )
        {
            // write record in fixed reference format
            writeFixedReferencesRecord( record, cursor );
        }
        else
        {
            int type = record.getType();
            cursor.putShort( (short) type );
            cursor.putByte( (byte) (type >>> Short.SIZE) );

            long recordId = record.getId();
            encode( cursor, record.getNextProp(), NULL );
            encode( cursor, record.getFirstNode() );
            encode( cursor, record.getSecondNode() );

            encode( cursor, getFirstPrevReference( record, recordId ) );
            if ( record.getFirstNextRel() != NULL )
            {
                encode( cursor, toRelative( record.getFirstNextRel(), recordId ) );
            }
            encode( cursor, getSecondPrevReference( record, recordId ) );
            if ( record.getSecondNextRel() != NULL )
            {
                encode( cursor, toRelative( record.getSecondNextRel(), recordId ) );
            }
        }
    }

    @Override
    protected boolean canUseFixedReferences( RelationshipRecord record, int recordSize )
    {
           return (isRecordBigEnoughForFixedReferences( recordSize ) &&
                  (record.getType() < (1 << Short.SIZE)) &&
                  (record.getFirstNode() & ONE_BIT_OVERFLOW_BIT_MASK) == 0) &&
                  ((record.getSecondNode() & ONE_BIT_OVERFLOW_BIT_MASK) == 0) &&
                  ((record.getFirstPrevRel() == NULL) || ((record.getFirstPrevRel() & ONE_BIT_OVERFLOW_BIT_MASK) == 0)) &&
                  ((record.getFirstNextRel() == NULL) || ((record.getFirstNextRel() & ONE_BIT_OVERFLOW_BIT_MASK) == 0)) &&
                  ((record.getSecondPrevRel() == NULL) || ((record.getSecondPrevRel() & ONE_BIT_OVERFLOW_BIT_MASK) == 0)) &&
                  ((record.getSecondNextRel() == NULL) || ((record.getSecondNextRel() & ONE_BIT_OVERFLOW_BIT_MASK) == 0)) &&
                  ((record.getNextProp() == NULL) || ((record.getNextProp() & THREE_BITS_OVERFLOW_BIT_MASK) == 0));
    }

    private boolean isRecordBigEnoughForFixedReferences( int recordSize )
    {
        return FIXED_FORMAT_RECORD_SIZE <= recordSize;
    }

    private long decodeAbsoluteOrRelative( PageCursor cursor, long headerByte, int firstInStartBit, long recordId )
    {
        return has( headerByte, firstInStartBit ) ?
               decodeCompressedReference( cursor ) :
               toAbsolute( decodeCompressedReference( cursor ), recordId );
    }

    private long getSecondPrevReference( RelationshipRecord record, long recordId )
    {
        return record.isFirstInSecondChain() ? record.getSecondPrevRel() :
                                   toRelative( record.getSecondPrevRel(), recordId );
    }

    private long getFirstPrevReference( RelationshipRecord record, long recordId )
    {
        return record.isFirstInFirstChain() ? record.getFirstPrevRel()
                                              : toRelative( record.getFirstPrevRel(), recordId );
    }

    private int getRelativeReferenceLength( long absoluteReference, long recordId )
    {
        return absoluteReference != NULL ? length( toRelative( absoluteReference, recordId ) ) : 0;
    }

    private long decodeAbsoluteIfPresent( PageCursor cursor, long headerByte, int conditionBit, long recordId )
    {
        return has( headerByte, conditionBit ) ? toAbsolute( decodeCompressedReference( cursor ), recordId ) : NULL;
    }

    private void readFixedReferencesRecord( RelationshipRecord record, PageCursor cursor, long headerByte,
            boolean inUse, int type )
    {
        // [    ,   x] first node higher order bits
        // [    ,  x ] second node high order bits
        // [    , x  ] first prev high order bits
        // [    ,x   ] first next high order bits
        // [   x,    ] second prev high order bits
        // [  x ,    ] second next high order bits
        // [xx  ,    ] next prop high order bits
        long modifiers = cursor.getByte();

        long firstNode = cursor.getInt() & 0xFFFFFFFFL;
        long firstNodeMod = (modifiers & FIRST_NODE_BIT) << 32;

        long secondNode = cursor.getInt() & 0xFFFFFFFFL;
        long secondNodeMod = (modifiers & SECOND_NODE_BIT) << 31;

        long firstPrevRel = cursor.getInt() & 0xFFFFFFFFL;
        long firstPrevRelMod = (modifiers & FIRST_PREV_REL_BIT) << 30;

        long firstNextRel = cursor.getInt() & 0xFFFFFFFFL;
        long firstNextRelMod = (modifiers & FIRST_NEXT_REL_BIT) << 29;

        long secondPrevRel = cursor.getInt() & 0xFFFFFFFFL;
        long secondPrevRelMod = (modifiers & SECOND_RREV_REL_BIT) << 28;

        long secondNextRel = cursor.getInt() & 0xFFFFFFFFL;
        long secondNextRelMod = (modifiers & SECOND_NEXT_REL_BIT) << 27;

        long nextProp = cursor.getInt() & 0xFFFFFFFFL;
        long nextPropMod = (modifiers & NEXT_PROP_BIT) << 26;

        record.initialize( inUse,
                BaseRecordFormat.longFromIntAndMod( nextProp, nextPropMod ),
                BaseRecordFormat.longFromIntAndMod( firstNode, firstNodeMod ),
                BaseRecordFormat.longFromIntAndMod( secondNode, secondNodeMod ),
                type,
                BaseRecordFormat.longFromIntAndMod( firstPrevRel, firstPrevRelMod ),
                BaseRecordFormat.longFromIntAndMod( firstNextRel, firstNextRelMod ),
                BaseRecordFormat.longFromIntAndMod( secondPrevRel, secondPrevRelMod ),
                BaseRecordFormat.longFromIntAndMod( secondNextRel, secondNextRelMod ),
                has( headerByte, FIRST_IN_FIRST_CHAIN_BIT ),
                has( headerByte, FIRST_IN_SECOND_CHAIN_BIT ) );
    }

    private void writeFixedReferencesRecord( RelationshipRecord record, PageCursor cursor )
    {
        cursor.putShort( (short) record.getType() );

        long firstNode = record.getFirstNode();
        short firstNodeMod = (short)((firstNode & HIGH_DWORD_LAST_BIT_MASK) >> 32);

        long secondNode = record.getSecondNode();
        long secondNodeMod = (secondNode & HIGH_DWORD_LAST_BIT_MASK) >> 31;

        long firstPrevRel = record.getFirstPrevRel();
        long firstPrevRelMod = firstPrevRel == NULL ? 0 : (firstPrevRel & HIGH_DWORD_LAST_BIT_MASK) >> 30;

        long firstNextRel = record.getFirstNextRel();
        long firstNextRelMod = firstNextRel == NULL ? 0 : (firstNextRel & HIGH_DWORD_LAST_BIT_MASK) >> 29;

        long secondPrevRel = record.getSecondPrevRel();
        long secondPrevRelMod = secondPrevRel == NULL ? 0 : (secondPrevRel & HIGH_DWORD_LAST_BIT_MASK) >> 28;

        long secondNextRel = record.getSecondNextRel();
        long secondNextRelMod = secondNextRel == NULL ? 0 : (secondNextRel & HIGH_DWORD_LAST_BIT_MASK) >> 27;

        long nextProp = record.getNextProp();
        long nextPropMod = nextProp == NULL ? 0 : (nextProp & TWO_BIT_FIXED_REFERENCE_BIT_MASK) >> 26;

        // [    ,   x] first node higher order bits
        // [    ,  x ] second node high order bits
        // [    , x  ] first prev high order bits
        // [    ,x   ] first next high order bits
        // [   x,    ] second prev high order bits
        // [  x ,    ] second next high order bits
        // [xx  ,    ] next prop high order bits
        short modifiers = (short) (firstNodeMod | secondNodeMod | firstPrevRelMod | firstNextRelMod |
                                   secondPrevRelMod | secondNextRelMod | nextPropMod);

        cursor.putByte( (byte) modifiers );
        cursor.putInt( (int) firstNode );
        cursor.putInt( (int) secondNode );
        cursor.putInt( (int) firstPrevRel );
        cursor.putInt( (int) firstNextRel );
        cursor.putInt( (int) secondPrevRel );
        cursor.putInt( (int) secondNextRel );
        cursor.putInt( (int) nextProp );
    }
}
