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
package org.neo4j.kernel.impl.store.format.lowlimit;

import java.io.IOException;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.impl.store.format.BaseOneByteHeaderRecordFormat;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.Record;

class NodeRecordFormatV1_9 extends BaseOneByteHeaderRecordFormat<NodeRecord>
{
    NodeRecordFormatV1_9()
    {
        super( fixedRecordSize( 9 ), 0, 0 );
    }

    @Override
    public NodeRecord newRecord()
    {
        return new NodeRecord( -1 );
    }

    @Override
    protected void doRead( NodeRecord record, PageCursor cursor, int recordSize, PagedFile storeFile, long headerByte,
            boolean inUse ) throws IOException
    {
        // [    ,   x] in use bit
        // [    ,xxx ] higher bits for rel id
        // [xxxx,    ] higher bits for prop id
        long nextRel = cursor.getUnsignedInt();
        long nextProp = cursor.getUnsignedInt();

        long relModifier = (headerByte & 0xEL) << 31;
        long propModifier = (headerByte & 0xF0L) << 28;

        record.initialize( inUse,
                longFromIntAndMod( nextProp, propModifier ),
                false,
                longFromIntAndMod( nextRel, relModifier ),
                Record.NO_LABELS_FIELD.intValue() );
    }

    @Override
    protected void doWrite( NodeRecord record, PageCursor cursor, int recordSize, PagedFile storeFile )
            throws IOException
    {
        throw new UnsupportedOperationException();
    }
}
