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
package org.neo4j.cypher.internal.kernel.api.helpers;

import org.github.jamm.MemoryMeter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.helpers.CachingExpandInto;
import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.RelationshipSelection;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.graphdb.Direction.OUTGOING;

class CachingExpandIntoTest
{
    private final MemoryMeter meter = new MemoryMeter();
    private final MemoryTracker memoryTracker = new LocalMemoryTracker();

    @AfterEach
    void tearDown()
    {
        memoryTracker.reset();
    }

    @Test
    void shouldComputeDegreeOfStartAndEndNode() throws Exception
    {
        // Given
        CachingExpandInto expandInto = new CachingExpandInto( mock( Read.class ), OUTGOING, memoryTracker );
        NodeCursor cursor = mockCursor();

        // Then
        assertEstimatesCorrectly( expandInto );

        // When
        findConnections( expandInto, cursor, 42, 43 );

        // Then
        verify( cursor, times( 2 ) ).degree( any( RelationshipSelection.class ) );

        assertReleasesHeap( expandInto );
    }

    @Test
    void shouldComputeDegreeOnceIfStartAndEndNodeAreTheSame() throws Exception
    {
        // Given
        CachingExpandInto expandInto = new CachingExpandInto( mock( Read.class ), OUTGOING, memoryTracker );
        NodeCursor cursor = mockCursor();

        // When
        findConnections( expandInto, cursor, 42, 42 );

        // Then
        verify( cursor ).degree( any( RelationshipSelection.class ) );

        assertReleasesHeap( expandInto );
    }

    @Test
    void shouldComputeDegreeOfStartAndEndNodeOnlyOnce() throws Exception
    {
        // Given
        CachingExpandInto expandInto = new CachingExpandInto( mock( Read.class ), OUTGOING, memoryTracker );
        NodeCursor cursor = mockCursor();

        // When, calling multiple times with different types
        findConnections( expandInto, cursor, 42, 43, 3 );
        findConnections( expandInto, cursor, 43, 42, 4 );
        findConnections( expandInto, cursor, 42, 43, 5 );

        // Then, only call once for 42 and once for 43
        verify( cursor, times( 2 ) ).degree( any( RelationshipSelection.class ) );

        assertReleasesHeap( expandInto );
    }

    @Test
    void shouldComputeDegreeOfStartAndEndNodeEveryTimeIfCacheIsFull() throws Exception
    {
        // Given
        CachingExpandInto expandInto = new CachingExpandInto( mock( Read.class ), OUTGOING, memoryTracker, 0 );
        NodeCursor cursor = mockCursor();

        // When
        findConnections( expandInto, cursor, 42, 43 );
        findConnections( expandInto, cursor, 42, 43 );
        findConnections( expandInto, cursor, 42, 43 );
        findConnections( expandInto, cursor, 42, 43 );
        findConnections( expandInto, cursor, 42, 43 );

        // Then, only call 5 times for 42 and 5 times for 43
        verify( cursor, times( 10 ) ).degree( any( RelationshipSelection.class ) );

        assertReleasesHeap( expandInto );
    }

    @Test
    void shouldNotRecomputeAnythingIfSameNodesAndTypes() throws Exception
    {
        // Given
        CachingExpandInto expandInto = new CachingExpandInto( mock( Read.class ), OUTGOING, memoryTracker );
        findConnections( expandInto, mockCursor(), 42, 43, 100, 101 );
        NodeCursor cursor = mockCursor();

        // When
        findConnections( expandInto, cursor, 42, 43, 100, 101 );

        // Then
        verifyNoInteractions( cursor );

        assertReleasesHeap( expandInto );
    }

    @Test
    void shouldRecomputeIfSameNodesAndTypesIfCacheIsFull() throws Exception
    {
        // Given
        CachingExpandInto expandInto = new CachingExpandInto( mock( Read.class ), OUTGOING, memoryTracker, 0 );
        findConnections( expandInto, mockCursor(), 42, 43, 100, 101 );
        NodeCursor cursor = mockCursor();

        // When
        findConnections( expandInto, cursor, 42, 43, 100, 101 );

        // Then
        verify( cursor, atLeastOnce() ).next();

        assertReleasesHeap( expandInto );
    }

    private void findConnections( CachingExpandInto expandInto, NodeCursor cursor, long from, long to, int... types )
    {
        RelationshipTraversalCursor relationships =
                expandInto.connectingRelationships( cursor, mock( RelationshipTraversalCursor.class ), from,
                       types, to );

        // While we traverse the relationships, we estimate with the cursor, which references the CachingExpandInto
        assertEstimatesCorrectly( relationships );

        while ( relationships.next() )
        {
            assertEstimatesCorrectly( relationships );
        }
        // Once the cursor is exhausted, it will close itself. Now we need to measure using expandInto directly again
        assertEstimatesCorrectly( expandInto );

        // Make sure that next is idempotent, even if cursor is already closed
        relationships.next();
        assertEstimatesCorrectly( expandInto );
    }

    private NodeCursor mockCursor()
    {
        NodeCursor mock = mock( NodeCursor.class, RETURNS_DEEP_STUBS );
        when( mock.next() ).thenReturn( true );
        when( mock.supportsFastDegreeLookup()).thenReturn( true );
        when( mock.degree( any( RelationshipSelection.class ) ) ).thenReturn( 7 );
        return mock;
    }

    private void assertEstimatesCorrectly( Object toMeasure )
    {
        long actualSize = meter.measureDeep( toMeasure ) - meter.measureDeep( memoryTracker );
        assertThat( memoryTracker.estimatedHeapMemory(), equalTo( actualSize ) );
    }

    private void assertReleasesHeap( CachingExpandInto expandInto ) throws Exception
    {
        expandInto.close();
        assertThat( memoryTracker.estimatedHeapMemory(), equalTo( 0L ) );
    }
}
