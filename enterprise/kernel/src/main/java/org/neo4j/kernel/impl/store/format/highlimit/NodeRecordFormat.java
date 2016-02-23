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
import org.neo4j.kernel.impl.store.format.highlimit.Reference.DataAdapter;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.Record;

class NodeRecordFormat extends BaseHighLimitRecordFormat<NodeRecord>
{
    private static final long NULL_LABELS = Record.NO_LABELS_FIELD.intValue();
    private static final int RECORD_SIZE = 16;
    private static final int DENSE_NODE_BIT       = 0b0000_1000;
    private static final int HAS_RELATIONSHIP_BIT = 0b0001_0000;
    private static final int HAS_PROPERTY_BIT     = 0b0010_0000;
    private static final int HAS_LABELS_BIT       = 0b0100_0000;

    public NodeRecordFormat()
    {
        super( fixedRecordSize( RECORD_SIZE ), 0 );
    }

    @Override
    public NodeRecord newRecord()
    {
        return new NodeRecord( -1 );
    }

    @Override
    protected void doReadInternal( NodeRecord record, PageCursor cursor, int recordSize, long headerByte, boolean inUse,
            DataAdapter<PageCursor> adapter )
    {
        // Interpret the header byte
        boolean dense = has( headerByte, DENSE_NODE_BIT );

        // Now read the rest of the data. The adapter will take care of moving the cursor over to the
        // other unit when we've exhausted the first one.
        long nextRel = decode( cursor, adapter, headerByte, HAS_RELATIONSHIP_BIT, NULL );
        long nextProp = decode( cursor, adapter, headerByte, HAS_PROPERTY_BIT, NULL );
        long labelField = decode( cursor, adapter, headerByte, HAS_LABELS_BIT, NULL_LABELS );
        record.initialize( inUse, nextProp, dense, nextRel, labelField );
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
    protected void doWriteInternal( NodeRecord record, PageCursor cursor, DataAdapter<PageCursor> adapter )
            throws IOException
    {
        encode( cursor, adapter, record.getNextRel(), NULL );
        encode( cursor, adapter, record.getNextProp(), NULL );
        encode( cursor, adapter, record.getLabelField(), NULL_LABELS );
    }
}
