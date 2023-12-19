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
 *
 * => 24B-59B
 */
public class RelationshipRecordFormatV3_0_0 extends BaseHighLimitRecordFormatV3_0_0<RelationshipRecord>
{
    public static final int RECORD_SIZE = 32;

    private static final int FIRST_IN_FIRST_CHAIN_BIT = 0b0000_1000;
    private static final int FIRST_IN_SECOND_CHAIN_BIT = 0b0001_0000;
    private static final int HAS_FIRST_CHAIN_NEXT_BIT = 0b0010_0000;
    private static final int HAS_SECOND_CHAIN_NEXT_BIT = 0b0100_0000;
    private static final int HAS_PROPERTY_BIT = 0b1000_0000;

    public RelationshipRecordFormatV3_0_0()
    {
        this( RECORD_SIZE );
    }

    RelationshipRecordFormatV3_0_0( int recordSize )
    {
        super( fixedRecordSize( recordSize ), 0 );
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
        int type = cursor.getShort() & 0xFFFF;
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

    private long decodeAbsoluteOrRelative( PageCursor cursor, long headerByte, int firstInStartBit, long recordId )
    {
        return has( headerByte, firstInStartBit ) ?
               decodeCompressedReference( cursor ) :
               toAbsolute( decodeCompressedReference( cursor ), recordId );
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
        return Short.BYTES + // type
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
        cursor.putShort( (short) record.getType() );
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
}
