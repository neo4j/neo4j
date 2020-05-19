/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.junit.jupiter.api.Test;

import java.util.Collection;

import org.neo4j.internal.id.IdSequence;
import org.neo4j.kernel.impl.store.allocator.ReusableRecordsAllocator;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.helpers.collection.Iterables.asList;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.kernel.impl.store.DynamicNodeLabels.allocateRecordsForDynamicLabels;
import static org.neo4j.kernel.impl.store.DynamicNodeLabels.dynamicPointer;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

class NodeRecordTest
{
    @Test
    void cloneShouldProduceExactCopy()
    {
        // Given
        long relId = 1337L;
        long propId = 1338L;
        long inlinedLabels = 12L;

        NodeRecord node = new NodeRecord( 1L ).initialize( false, propId, false, relId, 0 );
        node.setLabelField( inlinedLabels, asList( new DynamicRecord( 1L ), new DynamicRecord( 2L ) ) );
        node.setInUse( true );

        // When
        NodeRecord clone = node.copy();

        // Then
        assertEquals( node.inUse(), clone.inUse() );
        assertEquals( node.getLabelField(), clone.getLabelField() );
        assertEquals( node.getNextProp(), clone.getNextProp() );
        assertEquals( node.getNextRel(), clone.getNextRel() );

        assertThat( clone.getDynamicLabelRecords() ).isEqualTo( node.getDynamicLabelRecords() );
    }

    @Test
    void shouldListLabelRecordsInUse()
    {
        // Given
        NodeRecord node = new NodeRecord( 1 ).initialize( false, -1, false, -1, 0 );
        long inlinedLabels = 12L;
        DynamicRecord dynamic1 = new DynamicRecord( 1 ).initialize( true, true, -1, -1 );
        DynamicRecord dynamic2 = new DynamicRecord( 2 ).initialize( true, true, -1, -1 );
        DynamicRecord dynamic3 = new DynamicRecord( 3 ).initialize( true, true, -1, -1 );

        node.setLabelField( inlinedLabels, asList( dynamic1, dynamic2, dynamic3 ) );

        dynamic3.setInUse( false );

        // When
        Iterable<DynamicRecord> usedRecords = node.getUsedDynamicLabelRecords();

        // Then
        assertThat( asList( usedRecords ) ).isEqualTo( asList( dynamic1, dynamic2 ) );
    }

    @Test
    void shouldToStringBothUsedAndUnusedDynamicLabelRecords()
    {
        // GIVEN
        IdSequence ids = mock( IdSequence.class );
        when( ids.nextId( NULL ) ).thenReturn( 1L, 2L );
        ReusableRecordsAllocator recordAllocator =
                new ReusableRecordsAllocator( 30, new DynamicRecord( 1 ), new DynamicRecord( 2 ) );
        NodeRecord node = newUsedNodeRecord( 0 );
        long labelId = 10_123;
        // A dynamic label record
        Collection<DynamicRecord> existing = allocateRecordsForDynamicLabels( node.getId(), new long[]{labelId},
                recordAllocator, NULL, INSTANCE );
        // and a deleted one as well (simulating some deleted labels)
        DynamicRecord unused = newDeletedDynamicRecord( ids.nextId( NULL ) );
        unused.setInUse( false );
        existing.add( unused );
        node.setLabelField( dynamicPointer( existing ), existing );

        // WHEN
        String toString = node.toString();

        // THEN
        assertThat( toString ).contains( String.valueOf( labelId ) );
        assertThat( toString ).contains( unused.toString() );
    }

    private static DynamicRecord newDeletedDynamicRecord( long id )
    {
        DynamicRecord record = new DynamicRecord( id );
        record.setInUse( false );
        return record;
    }

    private static NodeRecord newUsedNodeRecord( long id )
    {
        NodeRecord node = new NodeRecord( id );
        node.setInUse( true );
        return node;
    }
}
