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
import org.neo4j.kernel.impl.store.format.BaseOneByteHeaderRecordFormat;
import org.neo4j.kernel.impl.store.format.BaseRecordFormat;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;

import static org.neo4j.kernel.impl.store.format.highlimit.BaseHighLimitRecordFormat.HEADER_BIT_FIXED_REFERENCE;
import static org.neo4j.kernel.impl.store.format.highlimit.BaseHighLimitRecordFormat.HEADER_BYTE;
import static org.neo4j.kernel.impl.store.format.highlimit.BaseHighLimitRecordFormat.NULL;
import static org.neo4j.kernel.impl.store.format.highlimit.Reference.toAbsolute;
import static org.neo4j.kernel.impl.store.format.highlimit.Reference.toRelative;


/**
 * <pre>
 * LEGEND:
 * V: variable between 3B-8B
 *
 * Record format:
 * 1B   header
 * VB   previous property
 * VB   next property
 * 8B   property block
 * 8B   property block
 * 8B   property block
 * 8B   property block
 * => 39B-49B
 *
 * Fixed reference format:
 * 1B   header
 * 6B   previous property
 * 6B   next property
 * 3B   padding
 * 8B   property block
 * 8B   property block
 * 8B   property block
 * 8B   property block
 * => 48B
 *
 * </pre>
 * Unlike other high limit records {@link BaseHighLimitRecordFormat} fixed reference marker in property record
 * format header is not inverted: 1 - fixed reference format used; 0 - variable length format used.
 */
public class PropertyRecordFormat extends BaseOneByteHeaderRecordFormat<PropertyRecord>
{
    static final int RECORD_SIZE = 48;
    private static final int PROPERTY_BLOCKS_PADDING = 3;
    static final int FIXED_FORMAT_RECORD_SIZE = HEADER_BYTE +
                                                Short.BYTES   /* prev prop modifiers */ +
                                                Integer.BYTES /* prev prop */ +
                                                Short.BYTES /* next prop modifiers */ +
                                                Integer.BYTES /* next prop */ +
                                                PROPERTY_BLOCKS_PADDING /* padding */;

    private static final long HIGH_DWORD_LOWER_WORD_MASK = 0xFFFF_0000_0000L;
    private static final long HIGH_DWORD_LOWER_WORD_CHECK_MASK = 0xFFFF_0000_0000_0000L;

    public PropertyRecordFormat()
    {
        super( fixedRecordSize( RECORD_SIZE ), 0, IN_USE_BIT, HighLimit.DEFAULT_MAXIMUM_BITS_PER_ID );
    }

    @Override
    public PropertyRecord newRecord()
    {
        return new PropertyRecord( -1 );
    }

    @Override
    public void read( PropertyRecord record, PageCursor cursor, RecordLoad mode, int recordSize )
            throws IOException
    {
        int offset = cursor.getOffset();
        byte headerByte = cursor.getByte();
        boolean inUse = isInUse( headerByte );
        boolean useFixedReferences = has( headerByte, HEADER_BIT_FIXED_REFERENCE );
        if ( mode.shouldLoad( inUse ) )
        {
            int blockCount = headerByte >>> 4;
            long recordId = record.getId();

            if ( useFixedReferences )
            {
                // read record in a fixed reference format
                readFixedReferencesRecord( record, cursor );
            }
            else
            {
                record.initialize( inUse,
                        toAbsolute( Reference.decode( cursor ), recordId ),
                        toAbsolute( Reference.decode( cursor ), recordId ) );
            }
            record.setUseFixedReferences( useFixedReferences );
            if ( (blockCount > record.getBlockCapacity()) | (RECORD_SIZE - (cursor.getOffset() - offset) < blockCount * Long.BYTES) )
            {
                cursor.setCursorException( "PropertyRecord claims to contain more blocks than can fit in a record" );
                return;
            }
            while ( blockCount-- > 0 )
            {
                record.addLoadedBlock( cursor.getLong() );
            }
        }
    }

    @Override
    public void write( PropertyRecord record, PageCursor cursor, int recordSize )
            throws IOException
    {
        if ( record.inUse() )
        {
            byte headerByte = (byte) ((record.inUse() ? IN_USE_BIT : 0) | numberOfBlocks( record ) << 4);
            boolean canUseFixedReferences = canUseFixedReferences( record, recordSize );
            record.setUseFixedReferences( canUseFixedReferences );
            headerByte = set( headerByte, HEADER_BIT_FIXED_REFERENCE, canUseFixedReferences );
            cursor.putByte( headerByte );

            long recordId = record.getId();

            if ( canUseFixedReferences )
            {
                // write record in fixed reference format
                writeFixedReferencesRecord( record, cursor );
            }
            else
            {
                Reference.encode( toRelative( record.getPrevProp(), recordId ), cursor );
                Reference.encode( toRelative( record.getNextProp(), recordId ), cursor );
            }
            for ( PropertyBlock block : record )
            {
                for ( long propertyBlock : block.getValueBlocks() )
                {
                    cursor.putLong( propertyBlock );
                }
            }
        }
        else
        {
            markAsUnused( cursor );
        }
    }

    private int numberOfBlocks( PropertyRecord record )
    {
        int count = 0;
        for ( PropertyBlock block : record )
        {
            count += block.getValueBlocks().length;
        }
        return count;
    }

    @Override
    public long getNextRecordReference( PropertyRecord record )
    {
        return record.getNextProp();
    }

    private boolean canUseFixedReferences( PropertyRecord record, int recordSize )
    {
        return ( isRecordBigEnoughForFixedReferences( recordSize ) &&
                ((record.getNextProp() == NULL) || ((record.getNextProp() & HIGH_DWORD_LOWER_WORD_CHECK_MASK) == 0)) &&
                ((record.getPrevProp() == NULL) || ((record.getPrevProp() & HIGH_DWORD_LOWER_WORD_CHECK_MASK) == 0)));
    }

    private boolean isRecordBigEnoughForFixedReferences( int recordSize )
    {
        return FIXED_FORMAT_RECORD_SIZE <= recordSize;
    }

    private void readFixedReferencesRecord( PropertyRecord record, PageCursor cursor )
    {
        // since fixed reference limits property reference to 34 bits, 6 bytes is ample.
        long prevMod = cursor.getShort() & 0xFFFFL;
        long prevProp = cursor.getInt() & 0xFFFFFFFFL;
        long nextMod = cursor.getShort() & 0xFFFFL;
        long nextProp = cursor.getInt() & 0xFFFFFFFFL;
        record.initialize( true,
                BaseRecordFormat.longFromIntAndMod( prevProp, prevMod << 32 ),
                BaseRecordFormat.longFromIntAndMod( nextProp, nextMod << 32 ) );
        // skip padding bytes
        cursor.setOffset( cursor.getOffset() + PROPERTY_BLOCKS_PADDING );
    }

    private void writeFixedReferencesRecord( PropertyRecord record, PageCursor cursor )
    {
        // Set up the record header
        short prevModifier = record.getPrevProp() == NULL ? 0 : (short) ((record.getPrevProp() & HIGH_DWORD_LOWER_WORD_MASK) >> 32);
        short nextModifier = record.getNextProp() == NULL ? 0 : (short) ((record.getNextProp() & HIGH_DWORD_LOWER_WORD_MASK) >> 32);
        cursor.putShort( prevModifier );
        cursor.putInt( (int) record.getPrevProp() );
        cursor.putShort( nextModifier );
        cursor.putInt( (int) record.getNextProp() );
        // skip bytes before start reading property blocks to have
        // aligned access and fixed position of property blocks
        cursor.setOffset( cursor.getOffset() + PROPERTY_BLOCKS_PADDING );
    }
}
