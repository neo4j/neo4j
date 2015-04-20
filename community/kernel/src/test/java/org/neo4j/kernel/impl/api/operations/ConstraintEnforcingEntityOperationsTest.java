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
package org.neo4j.kernel.impl.api.operations;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.impl.api.ConstraintEnforcingEntityOperations;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.locking.Locks;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_NODE;
import static org.neo4j.kernel.impl.locking.ResourceTypes.INDEX_ENTRY;
import static org.neo4j.kernel.impl.locking.ResourceTypes.indexEntryResourceId;

public class ConstraintEnforcingEntityOperationsTest
{
    private final int labelId = 1;
    private final int propertyKeyId = 2;
    private final String value = "value";
    private final IndexDescriptor indexDescriptor = new IndexDescriptor( labelId, propertyKeyId );
    private EntityReadOperations readOps;
    private SchemaReadOperations schemaOps;
    private KernelStatement state;
    private Locks.Client locks;
    private ConstraintEnforcingEntityOperations ops;

    @Before
    public void given_ConstraintEnforcingEntityOperations_with_OnlineIndex() throws Exception
    {
        this.readOps = mock( EntityReadOperations.class );
        this.schemaOps = mock( SchemaReadOperations.class );
        this.state = mock( KernelStatement.class );
        when( schemaOps.indexGetState( state, indexDescriptor ) ).thenReturn( InternalIndexState.ONLINE );
        this.locks = mock( Locks.Client.class );
        when( state.locks() ).thenReturn( locks );

        this.ops = new ConstraintEnforcingEntityOperations( null, readOps, schemaOps );
    }

    @Test
    public void shouldHoldIndexReadLockIfNodeIsExists() throws Exception
    {
        // given
        long expectedNodeId = 15;
        when( readOps.nodeGetUniqueFromIndexLookup( state, indexDescriptor, value ) ).thenReturn( expectedNodeId );

        // when
        long nodeId = ops.nodeGetUniqueFromIndexLookup( state, indexDescriptor, value );

        // then
        assertEquals( expectedNodeId, nodeId );
        verify( locks).acquireShared( INDEX_ENTRY, indexEntryResourceId( labelId, propertyKeyId, value ) );
        verifyNoMoreInteractions( locks );
    }

    @Test
    public void shouldHoldIndexWriteLockIfNodeDoesNotExist() throws Exception
    {
        // given
        when( readOps.nodeGetUniqueFromIndexLookup( state, indexDescriptor, value ) ).thenReturn( NO_SUCH_NODE );

        // when
        long nodeId = ops.nodeGetUniqueFromIndexLookup( state, indexDescriptor, value );

        // then
        assertEquals( NO_SUCH_NODE, nodeId );
        verify( locks ).acquireShared( INDEX_ENTRY, indexEntryResourceId( labelId, propertyKeyId, value ) );
        verify( locks ).acquireExclusive( INDEX_ENTRY, indexEntryResourceId( labelId, propertyKeyId, value ) );
        verify( locks ).releaseShared( INDEX_ENTRY, indexEntryResourceId( labelId, propertyKeyId, value ) );
        verifyNoMoreInteractions( locks );
    }

    @Test
    public void shouldHoldIndexReadLockIfNodeIsConcurrentlyCreated() throws Exception
    {
        // given
        long expectedNodeId = 15;
        when( readOps.nodeGetUniqueFromIndexLookup( state, indexDescriptor, value ) )
                .thenReturn( NO_SUCH_NODE )
                .thenReturn( expectedNodeId );

        // when
        long nodeId = ops.nodeGetUniqueFromIndexLookup( state, indexDescriptor, value );

        // then
        assertEquals( expectedNodeId, nodeId );
        verify( locks, times(2) ).acquireShared( INDEX_ENTRY, indexEntryResourceId( labelId, propertyKeyId, value ) );
        verify( locks ).acquireExclusive( INDEX_ENTRY, indexEntryResourceId( labelId, propertyKeyId, value ) );
        verify( locks ).releaseShared( INDEX_ENTRY, indexEntryResourceId( labelId, propertyKeyId, value ) );
        verify( locks ).releaseExclusive( INDEX_ENTRY, indexEntryResourceId( labelId, propertyKeyId, value ) );
        verifyNoMoreInteractions( locks );
    }
}
