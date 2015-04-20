/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.store.format.v2_2;

import org.junit.Test;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.format.Store;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;

public class NodeFormatTest extends RecordFormatTest<NodeStoreFormat_v2_2, NodeRecord, NodeStoreFormat_v2_2.NodeRecordCursor>
{
    public NodeFormatTest( )
    {
        super( new NodeStoreFormat_v2_2() );
    }

    @Test
    public void testDirectRecordSerialization() throws Exception
    {
        // When & Then
        assertSerializes( new NodeRecord( 12, /*inUse*/false, /*dense*/false, 0, 0, 0 ) );
        assertSerializes( new NodeRecord( 12, /*inUse*/true, /*dense*/false, 13, 14, 1337 ) );
        assertSerializes( new NodeRecord( 12, /*inUse*/true, /*dense*/true, 13, 14, 1337 ) );
        assertSerializes( new NodeRecord( 12, /*inUse*/true, /*dense*/true, 13, 14, 0 ) );
        assertSerializes( new NodeRecord( 12, /*inUse*/true, /*dense*/false, IdType.RELATIONSHIP.getMaxValue(), IdType.PROPERTY.getMaxValue(), IdType.LABEL_TOKEN.getMaxValue() ) );
        assertSerializes( new NodeRecord( 12, /*inUse*/true, /*dense*/true,  IdType.RELATIONSHIP.getMaxValue(), IdType.PROPERTY.getMaxValue(), 1l << (8*5 - 1) ) /* Max label field == 5 bytes */ );
    }

    @Test
    public void testCursorFieldReading() throws Exception
    {
        // Given
        NodeStoreFormat_v2_2.NodeRecordCursor cursor = format.createCursor( pagedFile, storeToolkit, Store.SF_NO_FLAGS );

        NodeRecord record = new NodeRecord( 12, true, true, 1, 2, 3 );
        writeToPagedFile( record );

        // When
        cursor.position( 12 );

        // Then
        long recordId;
        long firstRelationship;
        boolean inUse;
        String reuseToString;
        do
        {
            recordId = cursor.recordId();
            firstRelationship = cursor.firstRelationship();
            inUse = cursor.inUse();
            reuseToString = cursor.reusedRecord().toString();
        }
        while ( cursor.shouldRetry() );

        assertThat( recordId,          equalTo( 12l ) );
        assertThat( firstRelationship, equalTo( 1l ) );
        assertThat( inUse,             equalTo( true ) );
        assertThat( reuseToString,     equalTo( record.toString() ) );
    }
}
