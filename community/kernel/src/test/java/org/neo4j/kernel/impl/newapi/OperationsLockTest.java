/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.newapi;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;
import java.util.Optional;

import org.neo4j.internal.kernel.api.LabelSet;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.kernel.api.exceptions.explicitindex.AutoIndexingKernelException;
import org.neo4j.kernel.api.AssertOpen;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.explicitindex.AutoIndexOperations;
import org.neo4j.kernel.api.explicitindex.AutoIndexing;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.kernel.impl.index.ExplicitIndexStore;
import org.neo4j.kernel.impl.locking.LockTracer;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.kernel.impl.locking.SimpleStatementLocks;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.StorageStatement;
import org.neo4j.storageengine.api.StoreReadLayer;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class OperationsLockTest
{
    private KernelTransactionImplementation transaction = mock( KernelTransactionImplementation.class );
    private Operations operations;
    private final Locks.Client locks = mock( Locks.Client.class );
    private final Write write = mock( Write.class );
    private NodeCursor nodeCursor;
    private PropertyCursor propertyCursor;
    private TransactionState txState;
    private AllStoreHolder allStoreHolder;

    @Before
    public void setUp() throws InvalidTransactionTypeKernelException
    {
        txState = new TxState();
        when( transaction.getReasonIfTerminated() ).thenReturn( Optional.empty() );
        when( transaction.locks() ).thenReturn( new SimpleStatementLocks( locks ) );
        when( transaction.dataWrite() ).thenReturn( write );
        when( transaction.isOpen() ).thenReturn( true );
        when( transaction.lockTracer() ).thenReturn( LockTracer.NONE );
        when( transaction.txState() ).thenReturn( txState );
        Cursors cursors = mock( Cursors.class );
        nodeCursor = mock( NodeCursor.class );
        propertyCursor = mock( PropertyCursor.class );
        when( cursors.allocateNodeCursor() ).thenReturn( nodeCursor );
        when( cursors.allocatePropertyCursor() ).thenReturn( propertyCursor );
        AutoIndexing autoindexing = mock( AutoIndexing.class );
        when( autoindexing.nodes() ).thenReturn( mock( AutoIndexOperations.class ) );
        StorageStatement storageStatement = mock( StorageStatement.class );
        StorageEngine engine = mock( StorageEngine.class );
        StoreReadLayer storeReadLayer = mock( StoreReadLayer.class );
        when( storeReadLayer.nodeExists( anyLong() ) ).thenReturn( true );
        when( engine.storeReadLayer() ).thenReturn( storeReadLayer );
        allStoreHolder = new AllStoreHolder( engine, storageStatement,  transaction, cursors, mock(
                ExplicitIndexStore.class ), AssertOpen.ALWAYS_OPEN );
        operations = new Operations( allStoreHolder, mock( IndexTxStateUpdater.class ),
                storageStatement, transaction, cursors, autoindexing );
        operations.initialize();
    }

    @After
    public void tearDown()
    {
        operations.release();
    }

    @Test
    public void shouldAcquireEntityWriteLockBeforeAddingLabelToNode() throws Exception
    {
        // given
        when( nodeCursor.next() ).thenReturn( true );
        when( nodeCursor.labels() ).thenReturn( LabelSet.NONE );

        // when
        operations.nodeAddLabel( 123L, 456 );

        // then
        verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 123L );
        assertThat( txState.getNodeState( 123L ).labelDiffSets().getAdded(), contains( 456 ) );
    }

    @Test
    public void shouldNotAcquireEntityWriteLockBeforeAddingLabelToJustCreatedNode() throws Exception
    {
        // given
        when( nodeCursor.next() ).thenReturn( true );
        when( nodeCursor.labels() ).thenReturn( LabelSet.NONE );
        when( transaction.hasTxStateWithChanges() ).thenReturn( true );

        // when
        txState.nodeDoCreate( 123 );
        operations.nodeAddLabel( 123, 456 );

        // then
        verify( locks, never() ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 123 );
    }

    @Test
    public void shouldAcquireSchemaReadLockBeforeAddingLabelToNode() throws Exception
    {
        // given
        when( nodeCursor.next() ).thenReturn( true );
        when( nodeCursor.labels() ).thenReturn( LabelSet.NONE );

        // when
        int labelId = 456;
        operations.nodeAddLabel( 123, labelId );

        // then
        verify( locks ).acquireShared( LockTracer.NONE, ResourceTypes.LABEL, labelId );
        assertThat( txState.getNodeState( 123L ).labelDiffSets().getAdded(), contains( 456 ) );
    }

    @Test
    public void shouldAcquireEntityWriteLockBeforeSettingPropertyOnNode() throws Exception
    {
        // given
        when( nodeCursor.next() ).thenReturn( true );
        when( nodeCursor.labels() ).thenReturn( LabelSet.NONE );
        int propertyKeyId = 8;
        Value value = Values.of( 9 );
        when( propertyCursor.next() ).thenReturn( true );
        when( propertyCursor.propertyKey() ).thenReturn( propertyKeyId );
        when( propertyCursor.propertyValue() ).thenReturn( Values.NO_VALUE );

        // when
        operations.nodeSetProperty( 123, propertyKeyId, value );

        // then
        verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 123 );
        Iterator<StorageProperty> properties = txState.getNodeState( 123L ).addedProperties();
        assertThat( properties.next().propertyKeyId(), equalTo( propertyKeyId ) );
        assertThat( properties.hasNext(), equalTo( false ) );
    }

    @Test
    public void shouldNotAcquireEntityWriteLockBeforeSettingPropertyOnJustCreatedNode() throws Exception
    {
        // given
        when( nodeCursor.next() ).thenReturn( true );
        when( nodeCursor.labels() ).thenReturn( LabelSet.NONE );
        when( transaction.hasTxStateWithChanges() ).thenReturn( true );
        txState.nodeDoCreate( 123 );
        int propertyKeyId = 8;
        Value value = Values.of( 9 );

        // when
        operations.nodeSetProperty( 123, propertyKeyId, value );

        // then
        verify( locks, never() ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 123 );
        Iterator<StorageProperty> properties = txState.getNodeState( 123L ).addedProperties();
        assertThat( properties.next().propertyKeyId(), equalTo( propertyKeyId ) );
        assertThat( properties.hasNext(), equalTo( false ) );
    }

    @Test
    public void shouldAcquireEntityWriteLockBeforeDeletingNode()
            throws EntityNotFoundException, AutoIndexingKernelException, InvalidTransactionTypeKernelException
    {
        // GIVEN
        when( nodeCursor.next() ).thenReturn( true );
        when( nodeCursor.labels() ).thenReturn( LabelSet.NONE );
        when( allStoreHolder.nodeExistsInStore( 123 ) ).thenReturn( true );

        // WHEN
        operations.nodeDelete(  123 );

        //THEN
        verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 123 );
        assertThat( txState.nodeIsDeletedInThisTx( 123 ), equalTo( true ) );
    }

    @Test
    public void shouldNotAcquireEntityWriteLockBeforeDeletingJustCreatedNode() throws Exception
    {
        // THEN
        txState.nodeDoCreate( 123 );
        when( transaction.hasTxStateWithChanges() ).thenReturn( true );

        // WHEN
        operations.nodeDelete( 123 );

        //THEN
        verify( locks, never() ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 123 );
        assertThat( txState.nodeIsDeletedInThisTx( 123 ), equalTo( true ) );
    }
}
