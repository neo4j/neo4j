/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.store.format.aligned;

import java.io.IOException;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.impl.store.format.BaseOneByteHeaderRecordFormat;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;

import static org.neo4j.kernel.impl.store.format.aligned.Reference.PAGE_CURSOR_ADAPTER;

/**
 * {@link PropertyRecord} format which currently has some wasted space in the end due to hard coded
 * limit of 4 blocks per record, whereas the record size is 64.
 */
class PropertyRecordFormat extends BaseOneByteHeaderRecordFormat<PropertyRecord>
{
    private static final int RECORD_SIZE = 48;

    protected PropertyRecordFormat()
    {
        super( fixedRecordSize( RECORD_SIZE ), 0, IN_USE_BIT );
    }

    @Override
    public PropertyRecord newRecord()
    {
        return new PropertyRecord( -1 );
    }

    @Override
    protected void doRead( PropertyRecord record, PageCursor cursor, int recordSize, PagedFile storeFile,
            long headerByte, boolean inUse ) throws IOException
    {
        int blockCount = (int) (headerByte >>> 4);
        record.initialize( inUse,
                Reference.decode( cursor, PAGE_CURSOR_ADAPTER ),
                Reference.decode( cursor, PAGE_CURSOR_ADAPTER ) );
        while ( blockCount-- > 0 )
        {
            record.addLoadedBlock( cursor.getLong() );
        }
    }

    @Override
    protected void doWrite( PropertyRecord record, PageCursor cursor, int recordSize, PagedFile storeFile )
            throws IOException
    {
        cursor.putByte( (byte) ((record.inUse() ? IN_USE_BIT : 0) | numberOfBlocks( record ) << 4) );
        Reference.encode( record.getPrevProp(), cursor, PAGE_CURSOR_ADAPTER );
        Reference.encode( record.getNextProp(), cursor, PAGE_CURSOR_ADAPTER );
        for ( PropertyBlock block : record )
        {
            for ( long propertyBlock : block.getValueBlocks() )
            {
                cursor.putLong( propertyBlock );
            }
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
