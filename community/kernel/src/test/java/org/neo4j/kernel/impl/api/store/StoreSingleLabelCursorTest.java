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
package org.neo4j.kernel.impl.api.store;

import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.stream.LongStream;

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.impl.store.DynamicNodeLabels;
import org.neo4j.kernel.impl.store.InlineNodeLabels;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.RecordCursor;
import org.neo4j.kernel.impl.store.StandaloneDynamicRecordAllocator;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.util.InstanceCache;

import static java.util.stream.LongStream.iterate;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.store.record.Record.NO_LABELS_FIELD;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_PROPERTY;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_RELATIONSHIP;

public class StoreSingleLabelCursorTest
{
    @Test
    public void readNoLabels()
    {
        StoreSingleLabelCursor cursor = newCursor();
        NodeRecord node = new NodeRecord( 1 ).initialize( true, NO_NEXT_PROPERTY.intValue(), false,
                NO_NEXT_RELATIONSHIP.intValue(), NO_LABELS_FIELD.intValue() );

        cursor.init( node, 42 );

        assertFalse( cursor.next() );
    }

    @Test
    public void readInlinedLabelsWithoutSpecifiedLabel()
    {
        long[] labels = {1, 2, 42};
        StoreSingleLabelCursor cursor = newCursor();
        NodeRecord node = new NodeRecord( 1 ).initialize( true, NO_NEXT_PROPERTY.intValue(), false,
                NO_NEXT_RELATIONSHIP.intValue(), NO_LABELS_FIELD.intValue() );
        Collection<DynamicRecord> allocatedDynamicRecords = InlineNodeLabels.putSorted( node, labels, null, null );
        assertThat( allocatedDynamicRecords, is( empty() ) );

        cursor.init( node, 3 );

        assertFalse( cursor.next() );
    }

    @Test
    public void readInlinedLabelsWithSpecifiedLabel()
    {
        long[] labels = {1, 2, 42};
        StoreSingleLabelCursor cursor = newCursor();
        NodeRecord node = new NodeRecord( 1 ).initialize( true, NO_NEXT_PROPERTY.intValue(), false,
                NO_NEXT_RELATIONSHIP.intValue(), NO_LABELS_FIELD.intValue() );
        Collection<DynamicRecord> allocatedDynamicRecords = InlineNodeLabels.putSorted( node, labels, null, null );
        assertThat( allocatedDynamicRecords, is( empty() ) );

        cursor.init( node, 2 );

        assertTrue( cursor.next() );
        assertEquals( 2, cursor.getAsInt() );
        assertFalse( cursor.next() );
    }

    @Test
    public void readDynamicLabelsWithoutSpecifiedLabel()
    {
        int maxLabelId = 1_000;
        int specifiedLabelId = maxLabelId / 2;
        long[] labels = LongStream.iterate( 1, id -> id + 2 ).limit( maxLabelId ).toArray();
        NodeRecord node = new NodeRecord( 1 ).initialize( true, NO_NEXT_PROPERTY.intValue(), false,
                NO_NEXT_RELATIONSHIP.intValue(), NO_LABELS_FIELD.intValue() );
        Collection<DynamicRecord> allocatedDynamicRecords = DynamicNodeLabels.putSorted( node, labels,
                mock( NodeStore.class ), new StandaloneDynamicRecordAllocator() );
        for ( DynamicRecord dynamicRecord : allocatedDynamicRecords )
        {
            dynamicRecord.setInUse( true );
        }
        node.setLabelField( node.getLabelField(), Collections.emptyList() );
        assertThat( allocatedDynamicRecords, not( empty() ) );

        StoreSingleLabelCursor cursor = newCursor( allocatedDynamicRecords );
        cursor.init( node, specifiedLabelId );

        assertFalse( cursor.next() );
    }

    @Test
    public void readDynamicLabelsWithSpecifiedLabel()
    {
        int maxLabelId = 1_000;
        int specifiedLabelId = maxLabelId / 2 + 1;
        long[] labels = iterate( 1, id -> id + 2 ).limit( maxLabelId ).toArray();
        NodeRecord node = new NodeRecord( 1 ).initialize( true, NO_NEXT_PROPERTY.intValue(), false,
                NO_NEXT_RELATIONSHIP.intValue(), NO_LABELS_FIELD.intValue() );
        Collection<DynamicRecord> allocatedDynamicRecords = DynamicNodeLabels.putSorted( node, labels,
                mock( NodeStore.class ), new StandaloneDynamicRecordAllocator() );
        for ( DynamicRecord dynamicRecord : allocatedDynamicRecords )
        {
            dynamicRecord.setInUse( true );
        }
        node.setLabelField( node.getLabelField(), Collections.emptyList() );
        assertThat( allocatedDynamicRecords, not( empty() ) );

        StoreSingleLabelCursor cursor = newCursor( allocatedDynamicRecords );
        cursor.init( node, specifiedLabelId );

        assertTrue( cursor.next() );
        assertEquals( specifiedLabelId, cursor.getAsInt() );
        assertFalse( cursor.next() );
    }

    @SuppressWarnings( "unchecked" )
    private static StoreSingleLabelCursor newCursor( Collection<DynamicRecord> recordsForRecordCursor )
    {
        RecordCursor<DynamicRecord> recordCursor = mock( RecordCursor.class );
        when( recordCursor.getAll() ).thenReturn( Iterables.asList( recordsForRecordCursor ) );
        return new StoreSingleLabelCursor( recordCursor, mock( InstanceCache.class ) );
    }

    @SuppressWarnings( "unchecked" )
    private static StoreSingleLabelCursor newCursor()
    {
        return new StoreSingleLabelCursor( mock( RecordCursor.class ), mock( InstanceCache.class ) );
    }
}
