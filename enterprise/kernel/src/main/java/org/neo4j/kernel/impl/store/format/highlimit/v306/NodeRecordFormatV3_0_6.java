/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.kernel.impl.store.format.highlimit.v306;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.Record;

/**
 * LEGEND:
 * V: variable between 3B-8B
 *
 * Record format:
 * 1B   header
 * VB   first relationship
 * VB   first property
 * 5B   labels
 * => 12B-22B
 *
 * Fixed reference record format:
 * 1B   header
 * 1B   modifiers
 * 4B   first relationship
 * 4B   first property
 * 5B   labels
 * => 15B
 */
class NodeRecordFormatV3_0_6 extends BaseHighLimitRecordFormatV3_0_6<NodeRecord>
{
    static final int RECORD_SIZE = 16;
    // size of the record in fixed references format;
    static final int FIXED_FORMAT_RECORD_SIZE = HEADER_BYTE +
                                                Byte.BYTES /* modifiers */ +
                                                Integer.BYTES /* first relationship */ +
                                                Integer.BYTES /* first property */ +
                                                Integer.BYTES /* labels */ +
                                                Byte.BYTES /* labels */;

    private static final long NULL_LABELS = Record.NO_LABELS_FIELD.intValue();
    private static final int DENSE_NODE_BIT       = 0b0000_1000;
    private static final int HAS_RELATIONSHIP_BIT = 0b0001_0000;
    private static final int HAS_PROPERTY_BIT     = 0b0010_0000;
    private static final int HAS_LABELS_BIT       = 0b0100_0000;

    private static final long HIGH_DWORD_LOWER_NIBBLE_CHECK_MASK = 0xF_0000_0000L;
    private static final long HIGH_DWORD_LOWER_NIBBLE_MASK = 0xFFFF_FFF0_0000_0000L;
    private static final long LOWER_NIBBLE_READ_MASK = 0xFL;
    private static final long HIGHER_NIBBLE_READ_MASK = 0xF0L;

    NodeRecordFormatV3_0_6()
    {
        this( RECORD_SIZE );
    }

    NodeRecordFormatV3_0_6( int recordSize )
    {
        super( fixedRecordSize( recordSize ), 0 );
    }

    @Override
    public NodeRecord newRecord()
    {
        return new NodeRecord( -1 );
    }

    @Override
    protected void doReadInternal( NodeRecord record, PageCursor cursor, int recordSize, long headerByte,
            boolean inUse )
    {
        // Interpret the header byte
        boolean dense = has( headerByte, DENSE_NODE_BIT );
        if ( record.isUseFixedReferences() )
        {
            // read record in a fixed reference format
            readFixedReferencesRecord( record, cursor, inUse, dense );
            record.setUseFixedReferences( true );
        }
        else
        {
            // Now read the rest of the data. The adapter will take care of moving the cursor over to the
            // other unit when we've exhausted the first one.
            long nextRel = decodeCompressedReference( cursor, headerByte, HAS_RELATIONSHIP_BIT, NULL );
            long nextProp = decodeCompressedReference( cursor, headerByte, HAS_PROPERTY_BIT, NULL );
            long labelField = decodeCompressedReference( cursor, headerByte, HAS_LABELS_BIT, NULL_LABELS );
            record.initialize( inUse, nextProp, dense, nextRel, labelField );
        }
    }

    @Override
    public int requiredDataLength( NodeRecord record )
    {
        return  length( record.getNextRel(), NULL ) +
                length( record.getNextProp(), NULL ) +
                length( record.getLabelField(), NULL_LABELS );
    }

    @Override
    protected byte headerBits( NodeRecord record )
    {
        byte header = 0;
        header = set( header, DENSE_NODE_BIT, record.isDense() );
        header = set( header, HAS_RELATIONSHIP_BIT, record.getNextRel(), NULL );
        header = set( header, HAS_PROPERTY_BIT, record.getNextProp(), NULL );
        header = set( header, HAS_LABELS_BIT, record.getLabelField(), NULL_LABELS );
        return header;
    }

    @Override
    protected boolean canUseFixedReferences( NodeRecord record, int recordSize )
    {
        return isRecordBigEnoughForFixedReferences( recordSize ) &&
                (record.getNextProp() == NULL || (record.getNextProp() & HIGH_DWORD_LOWER_NIBBLE_MASK) == 0) &&
                (record.getNextRel() == NULL || (record.getNextRel() & HIGH_DWORD_LOWER_NIBBLE_MASK) == 0);
    }

    private boolean isRecordBigEnoughForFixedReferences( int recordSize )
    {
        return FIXED_FORMAT_RECORD_SIZE <= recordSize;
    }

    @Override
    protected void doWriteInternal( NodeRecord record, PageCursor cursor )
    {
        if ( record.isUseFixedReferences() )
        {
            // write record in fixed reference format
            writeFixedReferencesRecord( record, cursor );
        }
        else
        {
            encode( cursor, record.getNextRel(), NULL );
            encode( cursor, record.getNextProp(), NULL );
            encode( cursor, record.getLabelField(), NULL_LABELS );
        }
    }

    private void readFixedReferencesRecord( NodeRecord record, PageCursor cursor, boolean inUse, boolean dense )
    {
        byte modifiers = cursor.getByte();
        long relModifier = (modifiers & LOWER_NIBBLE_READ_MASK) << 32;
        long propModifier = (modifiers & HIGHER_NIBBLE_READ_MASK) << 28;

        long nextRel = cursor.getInt() & 0xFFFFFFFFL;
        long nextProp = cursor.getInt() & 0xFFFFFFFFL;

        long lsbLabels = cursor.getInt() & 0xFFFFFFFFL;
        long hsbLabels = cursor.getByte() & 0xFF; // so that a negative byte won't fill the "extended" bits with ones.
        long labels = lsbLabels | (hsbLabels << 32);

        record.initialize( inUse,
                BaseHighLimitRecordFormatV3_0_6.longFromIntAndMod( nextProp, propModifier ), dense,
                BaseHighLimitRecordFormatV3_0_6.longFromIntAndMod( nextRel, relModifier ), labels );
    }

    private void writeFixedReferencesRecord( NodeRecord record, PageCursor cursor )
    {
        long nextRel = record.getNextRel();
        long nextProp = record.getNextProp();

        short relModifier = nextRel == NULL ? 0 : (short)((nextRel & HIGH_DWORD_LOWER_NIBBLE_CHECK_MASK) >> 32);
        short propModifier = nextProp == NULL ? 0 : (short) ((nextProp & HIGH_DWORD_LOWER_NIBBLE_CHECK_MASK) >> 28);

        // [    ,xxxx] higher bits for rel id
        // [xxxx,    ] higher bits for prop id
        short modifiers = (short) ( relModifier | propModifier );

        cursor.putByte( (byte) modifiers );
        cursor.putInt( (int) nextRel );
        cursor.putInt( (int) nextProp );

        // lsb of labels
        long labelField = record.getLabelField();
        cursor.putInt( (int) labelField );
        // msb of labels
        cursor.putByte( (byte) ((labelField & 0xFF_0000_0000L) >> 32) );
    }
}
