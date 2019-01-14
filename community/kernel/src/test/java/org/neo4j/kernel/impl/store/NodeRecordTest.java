/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.impl.store;

import org.junit.Test;

import java.util.Collection;

import org.neo4j.kernel.impl.store.allocator.ReusableRecordsAllocator;
import org.neo4j.kernel.impl.store.id.IdSequence;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.Iterables.asList;
import static org.neo4j.kernel.impl.store.DynamicNodeLabels.allocateRecordsForDynamicLabels;
import static org.neo4j.kernel.impl.store.DynamicNodeLabels.dynamicPointer;
import static org.neo4j.kernel.impl.store.record.DynamicRecord.dynamicRecord;

public class NodeRecordTest
{
    @Test
    public void cloneShouldProduceExactCopy()
    {
        // Given
        long relId = 1337L;
        long propId = 1338L;
        long inlinedLabels = 12L;

        NodeRecord node = new NodeRecord( 1L, false, relId, propId );
        node.setLabelField( inlinedLabels, asList( new DynamicRecord( 1L ), new DynamicRecord( 2L ) ) );
        node.setInUse( true );

        // When
        NodeRecord clone = node.clone();

        // Then
        assertEquals( node.inUse(), clone.inUse() );
        assertEquals( node.getLabelField(), clone.getLabelField() );
        assertEquals( node.getNextProp(), clone.getNextProp() );
        assertEquals( node.getNextRel(), clone.getNextRel() );

        assertThat( clone.getDynamicLabelRecords(), equalTo( node.getDynamicLabelRecords() ) );
    }

    @Test
    public void shouldListLabelRecordsInUse()
    {
        // Given
        NodeRecord node = new NodeRecord( 1, false, -1, -1 );
        long inlinedLabels = 12L;
        DynamicRecord dynamic1 = dynamicRecord( 1L, true );
        DynamicRecord dynamic2 = dynamicRecord( 2L, true );
        DynamicRecord dynamic3 = dynamicRecord( 3L, true );

        node.setLabelField( inlinedLabels, asList( dynamic1, dynamic2, dynamic3 ) );

        dynamic3.setInUse( false );

        // When
        Iterable<DynamicRecord> usedRecords = node.getUsedDynamicLabelRecords();

        // Then
        assertThat( asList( usedRecords ), equalTo( asList( dynamic1, dynamic2 ) ) );
    }

    @Test
    public void shouldToStringBothUsedAndUnusedDynamicLabelRecords()
    {
        // GIVEN
        IdSequence ids = mock( IdSequence.class );
        when( ids.nextId() ).thenReturn( 1L, 2L );
        ReusableRecordsAllocator recordAllocator =
                new ReusableRecordsAllocator( 30, new DynamicRecord( 1 ), new DynamicRecord( 2 ) );
        NodeRecord node = newUsedNodeRecord( 0 );
        long labelId = 10_123;
        // A dynamic label record
        Collection<DynamicRecord> existing = allocateRecordsForDynamicLabels( node.getId(), new long[]{labelId},
                recordAllocator );
        // and a deleted one as well (simulating some deleted labels)
        DynamicRecord unused = newDeletedDynamicRecord( ids.nextId() );
        unused.setInUse( false );
        existing.add( unused );
        node.setLabelField( dynamicPointer( existing ), existing );

        // WHEN
        String toString = node.toString();

        // THEN
        assertThat( toString, containsString( String.valueOf( labelId ) ) );
        assertThat( toString, containsString( unused.toString() ) );
    }

    private DynamicRecord newDeletedDynamicRecord( long id )
    {
        DynamicRecord record = new DynamicRecord( id );
        record.setInUse( false );
        return record;
    }

    private NodeRecord newUsedNodeRecord( long id )
    {
        NodeRecord node = new NodeRecord( id );
        node.setInUse( true );
        return node;
    }
}
