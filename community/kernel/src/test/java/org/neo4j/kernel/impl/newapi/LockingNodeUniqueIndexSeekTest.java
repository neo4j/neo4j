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
package org.neo4j.kernel.impl.newapi;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.newapi.LockingNodeUniqueIndexSeek.UniqueNodeIndexSeeker;
import org.neo4j.storageengine.api.lock.LockTracer;
import org.neo4j.storageengine.api.schema.IndexDescriptorFactory;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.kernel.api.IndexQuery.exact;
import static org.neo4j.kernel.impl.locking.ResourceTypes.INDEX_ENTRY;
import static org.neo4j.kernel.impl.locking.ResourceTypes.indexEntryResourceId;

public class LockingNodeUniqueIndexSeekTest
{
    private final int labelId = 1;
    private final int propertyKeyId = 2;
    private IndexReference index = IndexDescriptorFactory.uniqueForSchema( SchemaDescriptorFactory.forLabel( labelId, propertyKeyId ) );

    private final Value value = Values.of( "value" );
    private final IndexQuery.ExactPredicate predicate = exact( propertyKeyId, value );
    private final long resourceId = indexEntryResourceId( labelId, predicate );
    private UniqueNodeIndexSeeker<NodeValueIndexCursor> uniqueNodeIndexSeeker = mock( UniqueNodeIndexSeeker.class );

    private final Locks.Client locks = mock( Locks.Client.class );
    private final Read read = mock( Read.class );
    private InOrder order;

    @Before
    public void setup()
    {
        order = inOrder( locks );
    }

    @Test
    public void shouldHoldSharedIndexLockIfNodeIsExists() throws Exception
    {
        // given
        NodeValueIndexCursor cursor = mock( NodeValueIndexCursor.class );
        when( cursor.next() ).thenReturn( true );
        when( cursor.nodeReference() ).thenReturn( 42L );

        // when
        long nodeId = LockingNodeUniqueIndexSeek.apply( locks,
                                                        LockTracer.NONE,
                                                        () -> cursor,
                                                        uniqueNodeIndexSeeker,
                                                        read,
                                                        index,
                                                        predicate );

        // then
        assertEquals( 42L, nodeId );
        verify( locks ).acquireShared( LockTracer.NONE, INDEX_ENTRY, resourceId );
        verifyNoMoreInteractions( locks );

        verify( cursor ).close();
    }

    @Test
    public void shouldHoldSharedIndexLockIfNodeIsConcurrentlyCreated() throws Exception
    {
        // given
        NodeValueIndexCursor cursor = mock( NodeValueIndexCursor.class );
        when( cursor.next() ).thenReturn( false, true );
        when( cursor.nodeReference() ).thenReturn( 42L );

        // when
        long nodeId = LockingNodeUniqueIndexSeek.apply( locks,
                                                        LockTracer.NONE,
                                                        () -> cursor,
                                                        uniqueNodeIndexSeeker,
                                                        read,
                                                        index,
                                                        predicate );

        // then
        assertEquals( 42L, nodeId );
        order.verify( locks ).acquireShared( LockTracer.NONE, INDEX_ENTRY, resourceId );
        order.verify( locks ).releaseShared( INDEX_ENTRY, resourceId );
        order.verify( locks ).acquireExclusive( LockTracer.NONE, INDEX_ENTRY, resourceId );
        order.verify( locks ).acquireShared( LockTracer.NONE, INDEX_ENTRY, resourceId );
        order.verify( locks ).releaseExclusive( INDEX_ENTRY, resourceId );
        verifyNoMoreInteractions( locks );

        verify( cursor ).close();
    }

    @Test
    public void shouldHoldExclusiveIndexLockIfNodeDoesNotExist() throws Exception
    {
        // given
        NodeValueIndexCursor cursor = mock( NodeValueIndexCursor.class );
        when( cursor.next() ).thenReturn( false, false );
        when( cursor.nodeReference() ).thenReturn( -1L );

        // when
        long nodeId = LockingNodeUniqueIndexSeek.apply( locks,
                                                        LockTracer.NONE,
                                                        () -> cursor,
                                                        uniqueNodeIndexSeeker,
                                                        read,
                                                        index,
                                                        predicate );

        // then
        assertEquals( -1L, nodeId );
        order.verify( locks ).acquireShared( LockTracer.NONE, INDEX_ENTRY, resourceId );
        order.verify( locks ).releaseShared( INDEX_ENTRY, resourceId );
        order.verify( locks ).acquireExclusive( LockTracer.NONE, INDEX_ENTRY, resourceId );
        verifyNoMoreInteractions( locks );

        verify( cursor ).close();
    }
}
