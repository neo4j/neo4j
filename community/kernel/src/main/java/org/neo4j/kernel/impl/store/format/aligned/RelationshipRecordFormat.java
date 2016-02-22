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
import org.neo4j.kernel.impl.store.format.aligned.Reference.DataAdapter;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

import static org.neo4j.kernel.impl.store.format.aligned.Reference.toAbsolute;
import static org.neo4j.kernel.impl.store.format.aligned.Reference.toRelative;

class RelationshipRecordFormat extends BaseAlignedRecordFormat<RelationshipRecord>
{
    private static final int RECORD_SIZE = 32;
    private static final int FIRST_IN_START_BIT = 0b0000_1000;
    private static final int FIRST_IN_END_BIT   = 0b0001_0000;
    private static final int HAS_START_NEXT_BIT = 0b0010_0000;
    private static final int HAS_END_NEXT_BIT   = 0b0100_0000;
    private static final int HAS_PROPERTY_BIT   = 0b1000_0000;

    public RelationshipRecordFormat( RecordIO<RelationshipRecord> recordIO )
    {
        super( fixedRecordSize( RECORD_SIZE ), 0, recordIO );
    }

    @Override
    public RelationshipRecord newRecord()
    {
        return new RelationshipRecord( -1 );
    }

    @Override
    protected void doReadInternal( RelationshipRecord record, PageCursor cursor, int recordSize, long headerByte,
            boolean inUse, DataAdapter<PageCursor> adapter )
    {
        int type = cursor.getShort() & 0xFFFF;
        long recordId = record.getId();
        record.initialize( inUse,
                decode( cursor, adapter, headerByte, HAS_PROPERTY_BIT, NULL  ),
                decode( cursor, adapter  ),
                decode( cursor, adapter  ),
                type,
                toAbsolute( decode( cursor, adapter ), recordId ),
                toAbsolute( decode( cursor, adapter, headerByte, HAS_START_NEXT_BIT, NULL ), recordId ),
                toAbsolute( decode( cursor, adapter ), recordId ),
                toAbsolute( decode( cursor, adapter, headerByte, HAS_END_NEXT_BIT, NULL ), recordId ),
                has( headerByte, FIRST_IN_START_BIT ),
                has( headerByte, FIRST_IN_END_BIT ) );
    }

    @Override
    protected byte headerBits( RelationshipRecord record )
    {
        byte header = 0;
        header = set( header, FIRST_IN_START_BIT, record.isFirstInFirstChain() );
        header = set( header, FIRST_IN_END_BIT, record.isFirstInSecondChain() );
        header = set( header, HAS_PROPERTY_BIT, record.getNextProp(), NULL );
        header = set( header, HAS_START_NEXT_BIT, record.getFirstNextRel(), NULL );
        header = set( header, HAS_END_NEXT_BIT, record.getSecondNextRel(), NULL );
        return header;
    }

    @Override
    protected int requiredDataLength( RelationshipRecord record )
    {
        long id = record.getId();
        return Short.BYTES + // type
               length( record.getNextProp(), NULL ) +
               length( record.getFirstNode() ) +
               length( record.getSecondNode() ) +
               length( toRelative( record.getFirstPrevRel(), id ) ) +
               length( toRelative( record.getFirstNextRel(), id ), NULL ) +
               length( toRelative( record.getSecondPrevRel(), id ) ) +
               length( toRelative( record.getSecondNextRel(), id ), NULL );
    }

    @Override
    protected void doWriteInternal( RelationshipRecord record, PageCursor cursor, DataAdapter<PageCursor> adapter )
            throws IOException
    {
        cursor.putShort( (short) record.getType() );
        long recordId = record.getId();
        encode( cursor, adapter, record.getNextProp(), NULL );
        encode( cursor, adapter, record.getFirstNode() );
        encode( cursor, adapter, record.getSecondNode() );
        encode( cursor, adapter, toRelative( record.getFirstPrevRel(), recordId ) );
        encode( cursor, adapter, toRelative( record.getFirstNextRel(), recordId ), NULL );
        encode( cursor, adapter, toRelative( record.getSecondPrevRel(), recordId ) );
        encode( cursor, adapter, toRelative( record.getSecondNextRel(), recordId ), NULL );
    }


}
