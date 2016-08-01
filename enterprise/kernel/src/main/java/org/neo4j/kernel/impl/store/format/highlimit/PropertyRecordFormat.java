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
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RecordLoad;

import static org.neo4j.kernel.impl.store.format.highlimit.Reference.toAbsolute;
import static org.neo4j.kernel.impl.store.format.highlimit.Reference.toRelative;
import static org.neo4j.kernel.impl.store.record.Record.NULL_REFERENCE;


/**
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
 *
 * => 39B-49B
 */
class PropertyRecordFormat extends BaseOneByteHeaderRecordFormat<PropertyRecord>
{
    private static final int RECORD_SIZE = 48;

    protected PropertyRecordFormat()
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
        record.setUseFixedReferences( has( headerByte, 0b0000_0100 ));
        if ( mode.shouldLoad( inUse ) )
        {
            int blockCount = headerByte >>> 4;
            long recordId = record.getId();

            if (record.isUseFixedReferences())
            {
                // since fixed reference limits property reference to 34 bits, 6 bytes is ample.
                long prevMod = (cursor.getShort() & 0xFFFFL);
                long prevProp = cursor.getInt() & 0xFFFFFFFFL;
                long nextMod = (cursor.getShort() & 0xFFFFL);
                long nextProp = cursor.getInt() & 0xFFFFFFFFL;
                record.initialize( true,
                        BaseRecordFormat.longFromIntAndMod( prevProp, prevMod << 32 ),
                        BaseRecordFormat.longFromIntAndMod( nextProp, nextMod << 32 ) );
                //skip 3 bytes before start reading property blocks
                cursor.setOffset(cursor.getOffset() + 3);
            }
            else
            {
                record.initialize( inUse,
                        toAbsolute( Reference.decode( cursor ), recordId ),
                        toAbsolute( Reference.decode( cursor ), recordId ) );
            }
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
            boolean canUseFixedReferences = canUseFixedReferences( record );
            record.setUseFixedReferences( canUseFixedReferences );
            headerByte = set( headerByte, 0b0000_0100, canUseFixedReferences );
            cursor.putByte( headerByte );

            long recordId = record.getId();

            if ( canUseFixedReferences )
            {
            // Set up the record header
                short prevModifier = record.getPrevProp() == Record.NO_NEXT_PROPERTY.intValue() ? 0 : (short) ((record.getPrevProp() & 0xFFFF_0000_0000L) >> 32);
                short nextModifier = record.getNextProp() == Record.NO_NEXT_PROPERTY.intValue() ? 0 : (short) ((record.getNextProp() & 0xFFFF_0000_0000L) >> 32);
                cursor.putShort( prevModifier );
                cursor.putInt( (int) record.getPrevProp() );
                cursor.putShort( nextModifier );
                cursor.putInt( (int) record.getNextProp() );
                //just stuff 3 bytes
                cursor.putBytes(new byte[]{0,0,0});
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

    private boolean canUseFixedReferences( PropertyRecord record )
    {
        return (((record.getNextProp() == NULL_REFERENCE.intValue()) || ((record.getNextProp() & 0xFFFF_FFFC_0000_0000L) == 0)) &&
                ((record.getPrevProp() == NULL_REFERENCE.intValue()) || ((record.getPrevProp() & 0xFFFF_FFFE_0000_0000L) == 0)));
    }
}
