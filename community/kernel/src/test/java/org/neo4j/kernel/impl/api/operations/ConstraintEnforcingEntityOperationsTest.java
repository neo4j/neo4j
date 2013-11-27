/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.impl.api.ConstraintEnforcingEntityOperations;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.api.LockHolder;
import org.neo4j.kernel.impl.api.ReleasableLock;

import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_NODE;

public class ConstraintEnforcingEntityOperationsTest
{
    private final int labelId = 1;
    private final int propertyKeyId = 2;
    private final String value = "value";
    private final IndexDescriptor indexDescriptor = new IndexDescriptor( labelId, propertyKeyId );
    private EntityReadOperations readOps;
    private SchemaReadOperations schemaOps;
    private KernelStatement state;
    private LockHolder locks;
    private ConstraintEnforcingEntityOperations ops;

    @Before
    public void given_ConstraintEnforcingEntityOperations_with_OnlineIndex() throws Exception
    {
        this.readOps = Mockito.mock( EntityReadOperations.class );
        this.schemaOps = Mockito.mock( SchemaReadOperations.class );
        this.state = Mockito.mock( KernelStatement.class );
        Mockito.when( schemaOps.indexGetState( state, indexDescriptor ) ).thenReturn( InternalIndexState.ONLINE );
        this.locks = Mockito.mock( LockHolder.class );
        Mockito.when( state.locks() ).thenReturn( locks );

        this.ops = new ConstraintEnforcingEntityOperations( null, readOps, schemaOps );
    }

    @Test
    public void shouldHoldIndexReadLockIfNodeIsExists() throws Exception
    {
        // given
        long expectedNodeId = 15;
        Mockito.when( readOps.nodeGetUniqueFromIndexLookup( state, indexDescriptor, value ) ).thenReturn( expectedNodeId );
        LockAnswer readLocks = new LockAnswer(), writeLocks = new LockAnswer();
        Mockito.when( locks.getReleasableIndexEntryReadLock( labelId, propertyKeyId, value ) ).then( readLocks );
        Mockito.when( locks.getReleasableIndexEntryWriteLock( labelId, propertyKeyId, value ) ).then( writeLocks );

        // when
        long nodeId = ops.nodeGetUniqueFromIndexLookup( state, indexDescriptor, value );

        // then
        Assert.assertEquals( expectedNodeId, nodeId );
        Assert.assertEquals( 1, readLocks.held() );
        Assert.assertEquals( 0, writeLocks.held() );
    }

    @Test
    public void shouldHoldIndexWriteLockIfNodeDoesNotExist() throws Exception
    {
        // given
        Mockito.when( readOps.nodeGetUniqueFromIndexLookup( state, indexDescriptor, value ) ).thenReturn( NO_SUCH_NODE );
        LockAnswer readLocks = new LockAnswer(), writeLocks = new LockAnswer();
        Mockito.when( locks.getReleasableIndexEntryReadLock( labelId, propertyKeyId, value ) ).then( readLocks );
        Mockito.when( locks.getReleasableIndexEntryWriteLock( labelId, propertyKeyId, value ) ).then( writeLocks );

        // when
        long nodeId = ops.nodeGetUniqueFromIndexLookup( state, indexDescriptor, value );

        // then
        Assert.assertEquals( NO_SUCH_NODE, nodeId );
        Assert.assertEquals( 0, readLocks.held() );
        Assert.assertEquals( 1, writeLocks.held() );
    }

    @Test
    public void shouldHoldIndexReadLockIfNodeIsConcurrentlyCreated() throws Exception
    {
        // given
        long expectedNodeId = 15;
        Mockito.when( readOps.nodeGetUniqueFromIndexLookup( state, indexDescriptor, value ) )
                .thenReturn( NO_SUCH_NODE )
                .thenReturn( expectedNodeId );
        LockAnswer readLocks = new LockAnswer(), writeLocks = new LockAnswer();
        Mockito.when( locks.getReleasableIndexEntryReadLock( labelId, propertyKeyId, value ) ).then( readLocks );
        Mockito.when( locks.getReleasableIndexEntryWriteLock( labelId, propertyKeyId, value ) ).then( writeLocks );

        // when
        long nodeId = ops.nodeGetUniqueFromIndexLookup( state, indexDescriptor, value );

        // then
        Assert.assertEquals( expectedNodeId, nodeId );
        Assert.assertEquals( 1, readLocks.held() );
        Assert.assertEquals( 0, writeLocks.held() );
    }


    private class LockAnswer implements Answer<ReleasableLock>
    {
        public int acquired, txBound, released;

        @Override
        public ReleasableLock answer( InvocationOnMock invocation ) throws Throwable
        {
            acquired++;
            return new ReleasableLock()
            {
                boolean closed;

                @Override
                public void release()
                {
                    if ( closed )
                    {
                        throw new IllegalStateException();
                    }
                    released++;
                    closed = true;
                }

                @Override
                public void registerWithTransaction()
                {
                    if ( closed )
                    {
                        throw new IllegalStateException();
                    }
                    txBound++;
                    closed = true;
                }

                @Override
                public void close()
                {
                    if ( !closed )
                    {
                        registerWithTransaction();
                    }
                }
            };
        }

        public int held()
        {
            Assert.assertEquals( "locking must be balanced", acquired, txBound + released );
            return txBound;
        }
    }
}
