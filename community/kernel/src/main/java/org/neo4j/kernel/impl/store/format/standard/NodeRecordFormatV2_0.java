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
package org.neo4j.kernel.impl.store.format.standard;

import java.io.IOException;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.format.BaseOneByteHeaderRecordFormat;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;

class NodeRecordFormatV2_0 extends BaseOneByteHeaderRecordFormat<NodeRecord>
{
    NodeRecordFormatV2_0()
    {
        super( fixedRecordSize( 14 ), 0, IN_USE_BIT, StandardFormatSettings.NODE_RECORD_MAXIMUM_ID_BITS );
    }

    @Override
    public NodeRecord newRecord()
    {
        return new NodeRecord( -1 );
    }

    public void read( NodeRecord record, PageCursor cursor, RecordLoad mode, int recordSize )
            throws IOException
    {
        long headerByte = cursor.getByte();
        boolean inUse = isInUse( (byte) headerByte );
        if ( mode.shouldLoad( inUse ) )
        {
            // [    ,   x] in use bit
            // [    ,xxx ] higher bits for rel id
            // [xxxx,    ] higher bits for prop id
            long nextRel = cursor.getInt() & 0xFFFFFFFFL;
            long nextProp = cursor.getInt() & 0xFFFFFFFFL;

            long relModifier = (headerByte & 0xEL) << 31;
            long propModifier = (headerByte & 0xF0L) << 28;

            long lsbLabels = cursor.getInt() & 0xFFFFFFFFL;
            long hsbLabels = cursor.getByte() & 0xFF; // so that a negative bye won't fill the "extended" bits with ones.
            long labels = lsbLabels | (hsbLabels << 32);

            record.initialize( inUse,
                    longFromIntAndMod( nextProp, propModifier ),
                    false,
                    longFromIntAndMod( nextRel, relModifier ),
                    labels );
        }
    }

    @Override
    public void write( NodeRecord record, PageCursor cursor, int recordSize ) throws IOException
    {
        throw new UnsupportedOperationException();
    }
}
