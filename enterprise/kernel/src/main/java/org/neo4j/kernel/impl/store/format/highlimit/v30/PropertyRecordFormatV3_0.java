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
package org.neo4j.kernel.impl.store.format.highlimit.v30;

import java.io.IOException;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.format.BaseOneByteHeaderRecordFormat;
import org.neo4j.kernel.impl.store.format.highlimit.Reference;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;

import static org.neo4j.kernel.impl.store.format.highlimit.Reference.toAbsolute;
import static org.neo4j.kernel.impl.store.format.highlimit.Reference.toRelative;


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
public class PropertyRecordFormatV3_0 extends BaseOneByteHeaderRecordFormat<PropertyRecord>
{
    public static final int RECORD_SIZE = 48;

    public PropertyRecordFormatV3_0()
    {
        super( fixedRecordSize( RECORD_SIZE ), 0, IN_USE_BIT, HighLimitV3_0.DEFAULT_MAXIMUM_BITS_PER_ID );
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
        if ( mode.shouldLoad( inUse ) )
        {
            int blockCount = headerByte >>> 4;
            long recordId = record.getId();
            record.initialize( inUse,
                    toAbsolute( Reference.decode( cursor ), recordId ),
                    toAbsolute( Reference.decode( cursor ), recordId ) );
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
            cursor.putByte( (byte) ((record.inUse() ? IN_USE_BIT : 0) | numberOfBlocks( record ) << 4) );
            long recordId = record.getId();
            Reference.encode( toRelative( record.getPrevProp(), recordId), cursor );
            Reference.encode( toRelative( record.getNextProp(), recordId), cursor );
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
}
