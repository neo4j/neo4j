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
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.format.Store;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class RelationshipFormatTest extends RecordFormatTest<RelationshipStoreFormat_v2_2, RelationshipRecord, RelationshipStoreFormat_v2_2.RelationshipRecordCursor>
{
    public RelationshipFormatTest()
    {
        super( new RelationshipStoreFormat_v2_2() );
    }

    @Test
    public void testDirectRecordSerialization() throws Exception
    {
        // When & Then
        assertSerializes( new RelationshipRecord( 12, true, 1,2,3,4,5,6,7, true, false));
        assertSerializes( new RelationshipRecord( 12, true, IdType.NODE.getMaxValue(),IdType.NODE.getMaxValue(),(int)IdType.RELATIONSHIP_TYPE_TOKEN.getMaxValue(),IdType.RELATIONSHIP.getMaxValue(),IdType.RELATIONSHIP.getMaxValue(),IdType.RELATIONSHIP.getMaxValue(),IdType.RELATIONSHIP.getMaxValue(), false, false));
        assertSerializes( new RelationshipRecord( 12, true, IdType.NODE.getMaxValue(),IdType.NODE.getMaxValue(),(int)IdType.RELATIONSHIP_TYPE_TOKEN.getMaxValue(),IdType.RELATIONSHIP.getMaxValue(),0,IdType.RELATIONSHIP.getMaxValue(),IdType.RELATIONSHIP.getMaxValue(), false, true));
    }

    @Test
    public void testCursorFieldReading() throws Exception
    {
        // Given
        RelationshipStoreFormat_v2_2.RelationshipRecordCursor cursor = format.createCursor( pagedFile, storeToolkit, Store.SF_NO_FLAGS );
        RelationshipRecord record = new RelationshipRecord( 12, true, 1,2,3,4,5,6,7, true, false);
        writeToPagedFile( record );

        // When
        cursor.position( 12 );

        // Then
        long recordId;
        boolean inUse;
        String reuseToString;
        do
        {
            recordId = cursor.recordId();
            inUse = cursor.inUse();
            reuseToString = cursor.reusedRecord().toString();
        }
        while ( cursor.shouldRetry() );
        assertThat( recordId,          equalTo(12l));
        assertThat( inUse,             equalTo(true));
        assertThat( reuseToString, equalTo( record.toString() ) );
    }
}
