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

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import org.neo4j.io.pagecache.StubPageCursor;
import org.neo4j.kernel.impl.store.IntStoreHeader;
import org.neo4j.kernel.impl.store.format.RecordFormat;
import org.neo4j.kernel.impl.store.record.NodeRecord;

import static org.junit.Assert.assertEquals;

public class BaseHighLimitRecordFormatTest
{
    StubPageCursor pageCursor;

    @Before
    public void setUp()
    {
        pageCursor = new StubPageCursor( 0, 8192 );
    }
    @Test
    public void secondHeaderByteEmptyTest() throws IOException
    {
        //GIVEN
        RecordFormat<NodeRecord> nodeRecordFormat = HighLimit.RECORD_FORMATS.node();
        NodeRecord nodeRecord = new NodeRecord( 1 );
        nodeRecord.setInUse( true );
        nodeRecord.setNextRel( Integer.MAX_VALUE );
        nodeRecord.setNextProp( Integer.MAX_VALUE );
        IntStoreHeader intStoreHeader = new IntStoreHeader( 42 );
        int recordSize = nodeRecordFormat.getRecordSize( intStoreHeader);
        nodeRecordFormat.write( nodeRecord, pageCursor, recordSize, null);

        //WHEN
        pageCursor.setOffset( 0 );
        byte firstHeaderByte = pageCursor.getByte();
        byte secondHeaderByte = pageCursor.getByte();

        //THEN
        assertEquals((byte) 0 , secondHeaderByte);
    }
}