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

import org.junit.jupiter.api.Test;

import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.helpers.CachingExpandInto;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.RelationshipSelection;

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
    @Test
    void shouldComputeDegreeOfStartAndEndNode()
    {
        // Given
        CachingExpandInto expandInto = new CachingExpandInto( mock( Read.class ), OUTGOING, EmptyMemoryTracker.INSTANCE );
        NodeCursor cursor = mockCursor();

        // When
       findConnections( expandInto, cursor, 42, 43 );

        // Then
        verify( cursor, times( 2 ) ).degree( any( RelationshipSelection.class ) );
    }

    @Test
    void shouldComputeDegreeOnceIfStartAndEndNodeAreTheSame()
    {
        // Given
        CachingExpandInto expandInto = new CachingExpandInto( mock( Read.class ), OUTGOING, EmptyMemoryTracker.INSTANCE );
        NodeCursor cursor = mockCursor();

        // When
        findConnections( expandInto, cursor, 42, 42 );

        // Then
        verify( cursor ).degree( any( RelationshipSelection.class ) );
    }

    @Test
    void shouldComputeDegreeOfStartAndEndNodeOnlyOnce()
    {
        // Given
        CachingExpandInto expandInto = new CachingExpandInto( mock( Read.class ), OUTGOING, EmptyMemoryTracker.INSTANCE );
        NodeCursor cursor = mockCursor();

        // When, calling multiple times with different types
        findConnections( expandInto, cursor, 42, 43, 3 );
        findConnections( expandInto, cursor, 43, 42, 4 );
        findConnections( expandInto, cursor, 42, 43, 5 );

        // Then, only call once for 42 and once for 43
        verify( cursor, times( 2 ) ).degree( any( RelationshipSelection.class ) );
    }

    @Test
    void shouldComputeDegreeOfStartAndEndNodeEveryTimeIfCacheIsFull()
    {
        // Given
        CachingExpandInto expandInto = new CachingExpandInto( mock( Read.class ), OUTGOING, EmptyMemoryTracker.INSTANCE, 0 );
        NodeCursor cursor = mockCursor();

        // When
        findConnections( expandInto, cursor, 42, 43 );
        findConnections( expandInto, cursor, 42, 43 );
        findConnections( expandInto, cursor, 42, 43 );
        findConnections( expandInto, cursor, 42, 43 );
        findConnections( expandInto, cursor, 42, 43 );

        // Then, only call 5 times for 42 and 5 times for 43
        verify( cursor, times( 10 ) ).degree( any( RelationshipSelection.class ) );
    }

    @Test
    void shouldNotRecomputeAnythingIfSameNodesAndTypes()
    {
        // Given
        CachingExpandInto expandInto = new CachingExpandInto( mock( Read.class ), OUTGOING, EmptyMemoryTracker.INSTANCE );
        findConnections( expandInto, mockCursor(), 42, 43, 100, 101 );
        NodeCursor cursor = mockCursor();

        // When
        findConnections( expandInto, cursor, 42, 43, 100, 101 );

        // Then
        verifyNoInteractions( cursor );
    }

    @Test
    void shouldRecomputeIfSameNodesAndTypesIfCacheIsFull()
    {
        // Given
        CachingExpandInto expandInto = new CachingExpandInto( mock( Read.class ), OUTGOING, EmptyMemoryTracker.INSTANCE, 0 );
        findConnections( expandInto, mockCursor(), 42, 43, 100, 101 );
        NodeCursor cursor = mockCursor();

        // When
        findConnections( expandInto, cursor, 42, 43, 100, 101 );

        // Then
        verify( cursor, atLeastOnce() ).next();
    }

    @SuppressWarnings( "StatementWithEmptyBody" )
    private void findConnections(  CachingExpandInto expandInto, NodeCursor cursor, long from, long to, int...types )
    {
        RelationshipTraversalCursor relationships =
                expandInto.connectingRelationships( cursor, mock( RelationshipTraversalCursor.class ), from,
                       types, to );
        while ( relationships.next() )
        {
            //do nothing
        }
    }

    private NodeCursor mockCursor()
    {
        NodeCursor mock = mock( NodeCursor.class, RETURNS_DEEP_STUBS );
        when( mock.next() ).thenReturn( true );
        when( mock.supportsFastDegreeLookup()).thenReturn( true );
        when( mock.degree( any( RelationshipSelection.class ) ) ).thenReturn( 7 );
        return mock;
    }
}
