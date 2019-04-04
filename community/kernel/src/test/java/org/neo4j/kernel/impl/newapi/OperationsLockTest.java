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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.exceptions.KernelException;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.internal.index.label.LabelScanStore;
import org.neo4j.internal.kernel.api.LabelSet;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.kernel.api.helpers.StubNodeCursor;
import org.neo4j.internal.kernel.api.helpers.TestRelationshipChain;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptorFactory;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.internal.schema.constraints.RelExistenceConstraintDescriptor;
import org.neo4j.internal.schema.constraints.UniquenessConstraintDescriptor;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexingProvidersService;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.index.schema.CapableIndexDescriptor;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.SimpleStatementLocks;
import org.neo4j.kernel.impl.util.DefaultValueMapper;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.ResourceTypes;
import org.neo4j.storageengine.api.CommandCreationContext;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.collection.PrimitiveLongCollections.EMPTY_LONG_ARRAY;
import static org.neo4j.configuration.GraphDatabaseSettings.default_schema_provider;
import static org.neo4j.helpers.collection.Iterators.asList;
import static org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory.existsForRelType;
import static org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory.uniqueForLabel;
import static org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory.uniqueForSchema;
import static org.neo4j.kernel.impl.newapi.TwoPhaseNodeForRelationshipLockingTest.returnRelationships;
import static org.neo4j.test.rule.DatabaseRule.mockedTokenHolders;
import static org.neo4j.values.storable.Values.NO_VALUE;

class OperationsLockTest
{
    private KernelTransactionImplementation transaction = mock( KernelTransactionImplementation.class );
    private Operations operations;
    private final Locks.Client locks = mock( Locks.Client.class );
    private final Write write = mock( Write.class );
    private InOrder order;
    private FullAccessNodeCursor nodeCursor;
    private FullAccessPropertyCursor propertyCursor;
    private DefaultRelationshipScanCursor relationshipCursor;
    private TransactionState txState;
    private AllStoreHolder allStoreHolder;
    private final LabelSchemaDescriptor descriptor = SchemaDescriptorFactory.forLabel( 123, 456 );
    private StorageReader storageReader;
    private ConstraintIndexCreator constraintIndexCreator;
    private IndexingService indexingService;

    @BeforeEach
    void setUp() throws InvalidTransactionTypeKernelException
    {
        txState = Mockito.spy( new TxState() );
        when( transaction.getReasonIfTerminated() ).thenReturn( Optional.empty() );
        when( transaction.statementLocks() ).thenReturn( new SimpleStatementLocks( locks ) );
        when( transaction.dataWrite() ).thenReturn( write );
        when( transaction.isOpen() ).thenReturn( true );
        when( transaction.lockTracer() ).thenReturn( LockTracer.NONE );
        when( transaction.txState() ).thenReturn( txState );
        when( transaction.securityContext() ).thenReturn( SecurityContext.AUTH_DISABLED );

        DefaultPooledCursors cursors = mock( DefaultPooledCursors.class );
        nodeCursor = mock( FullAccessNodeCursor.class );
        propertyCursor = mock( FullAccessPropertyCursor.class );
        relationshipCursor = mock( DefaultRelationshipScanCursor.class );
        when( cursors.allocateFullAccessNodeCursor() ).thenReturn( nodeCursor );
        when( cursors.allocateFullAccessPropertyCursor() ).thenReturn( propertyCursor );
        when( cursors.allocateRelationshipScanCursor() ).thenReturn( relationshipCursor );
        StorageEngine engine = mock( StorageEngine.class );
        storageReader = mock( StorageReader.class );
        when( storageReader.nodeExists( anyLong() ) ).thenReturn( true );
        when( storageReader.constraintsGetForLabel( anyInt() )).thenReturn( Collections.emptyIterator() );
        when( storageReader.constraintsGetAll() ).thenReturn( Collections.emptyIterator() );
        when( engine.newReader() ).thenReturn( storageReader );
        indexingService = mock( IndexingService.class );
        Dependencies dependencies = new Dependencies();
        DefaultValueMapper mapper = mock( DefaultValueMapper.class );
        dependencies.satisfyDependency( mapper );
        allStoreHolder = new AllStoreHolder( storageReader, transaction, cursors, mock( GlobalProcedures.class ), mock( SchemaState.class ), indexingService,
                mock( LabelScanStore.class ), mock( IndexStatisticsStore.class ), dependencies );
        constraintIndexCreator = mock( ConstraintIndexCreator.class );
        CommandCreationContext creationContext = mock( CommandCreationContext.class );
        operations = new Operations( allStoreHolder, storageReader, mock( IndexTxStateUpdater.class ), creationContext,
                 transaction, new KernelToken( storageReader, creationContext, transaction, mockedTokenHolders() ), cursors,
                constraintIndexCreator, mock( ConstraintSemantics.class ), mock( IndexingProvidersService.class ), Config.defaults() );
        operations.initialize();

        this.order = inOrder( locks, txState, storageReader );
    }

    @AfterEach
    void tearDown()
    {
        operations.release();
    }

    @Test
    void shouldAcquireEntityWriteLockCreatingRelationship() throws Exception
    {
        // when
        long rId = operations.relationshipCreate( 1, 2, 3 );

        // then
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 1 );
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 3 );
        order.verify( txState ).relationshipDoCreate( rId, 2, 1, 3 );
    }

    @Test
    void shouldAcquireNodeLocksWhenCreatingRelationshipInOrderOfAscendingId() throws Exception
    {
        // GIVEN
        long lowId = 3;
        long highId = 5;
        int relationshipLabel = 0;

        // WHEN
        operations.relationshipCreate( lowId, relationshipLabel, highId );

        // THEN
        InOrder lockingOrder = inOrder( locks );
        lockingOrder.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, lowId );
        lockingOrder.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, highId );
        lockingOrder.verifyNoMoreInteractions();
        reset( locks );

        // WHEN
        operations.relationshipCreate( highId, relationshipLabel, lowId );

        // THEN
        InOrder lowLockingOrder = inOrder( locks );
        lowLockingOrder.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, lowId );
        lowLockingOrder.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, highId );
        lowLockingOrder.verifyNoMoreInteractions();
    }

    @Test
    void shouldAcquireNodeLocksWhenDeletingRelationshipInOrderOfAscendingId()
    {
        // GIVEN
        final long relationshipId = 10;
        final long lowId = 3;
        final long highId = 5;
        int relationshipLabel = 0;

        // and GIVEN
        setStoreRelationship( relationshipId, lowId, highId, relationshipLabel );

        // WHEN
        operations.relationshipDelete( relationshipId );

        // THEN
        InOrder lockingOrder = inOrder( locks );
        lockingOrder.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, lowId );
        lockingOrder.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, highId );
        lockingOrder.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.RELATIONSHIP, relationshipId );
        lockingOrder.verifyNoMoreInteractions();
        reset( locks );

        // and GIVEN
        setStoreRelationship( relationshipId, highId, lowId, relationshipLabel );

        // WHEN
        operations.relationshipDelete( relationshipId );

        // THEN
        InOrder highLowIdOrder = inOrder( locks );
        highLowIdOrder.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, lowId );
        highLowIdOrder.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, highId );
        highLowIdOrder.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.RELATIONSHIP, relationshipId );
        highLowIdOrder.verifyNoMoreInteractions();
    }

    @Test
    void shouldAcquireEntityWriteLockBeforeAddingLabelToNode() throws Exception
    {
        // given
        when( nodeCursor.next() ).thenReturn( true );
        when( nodeCursor.labels() ).thenReturn( LabelSet.NONE );

        // when
        operations.nodeAddLabel( 123L, 456 );

        // then
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 123L );
        order.verify( txState ).nodeDoAddLabel( 456, 123L );
    }

    @Test
    void shouldNotAcquireEntityWriteLockBeforeAddingLabelToJustCreatedNode() throws Exception
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
    void shouldAcquireSchemaReadLockBeforeAddingLabelToNode() throws Exception
    {
        // given
        when( nodeCursor.next() ).thenReturn( true );
        when( nodeCursor.labels() ).thenReturn( LabelSet.NONE );

        // when
        int labelId = 456;
        operations.nodeAddLabel( 123, labelId );

        // then
        order.verify( locks ).acquireShared( LockTracer.NONE, ResourceTypes.LABEL, labelId );
        order.verify( txState ).nodeDoAddLabel( labelId, 123 );
    }

    @Test
    void shouldAcquireEntityWriteLockBeforeSettingPropertyOnNode() throws Exception
    {
        // given
        when( nodeCursor.next() ).thenReturn( true );
        when( nodeCursor.labels() ).thenReturn( LabelSet.NONE );
        int propertyKeyId = 8;
        Value value = Values.of( 9 );
        when( propertyCursor.next() ).thenReturn( true );
        when( propertyCursor.propertyKey() ).thenReturn( propertyKeyId );
        when( propertyCursor.propertyValue() ).thenReturn( NO_VALUE );

        // when
        operations.nodeSetProperty( 123, propertyKeyId, value );

        // then
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 123 );
        order.verify( txState ).nodeDoAddProperty( 123, propertyKeyId, value );
    }

    @Test
    void shouldAcquireSchemaReadLockBeforeSettingPropertyOnNode() throws Exception
    {
        // given
        int relatedLabelId = 50;
        int unrelatedLabelId = 51;
        int propertyKeyId = 8;
        when( nodeCursor.next() ).thenReturn( true );
        LabelSet labelSet = mock( LabelSet.class );
        when( labelSet.all() ).thenReturn( new long[]{relatedLabelId} );
        when( nodeCursor.labels() ).thenReturn( labelSet );
        Value value = Values.of( 9 );
        when( propertyCursor.next() ).thenReturn( true );
        when( propertyCursor.propertyKey() ).thenReturn( propertyKeyId );
        when( propertyCursor.propertyValue() ).thenReturn( NO_VALUE );

        // when
        operations.nodeSetProperty( 123, propertyKeyId, value );

        // then
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 123 );
        order.verify( locks ).acquireShared( LockTracer.NONE, ResourceTypes.LABEL, relatedLabelId );
        order.verify( locks, never() ).acquireShared( LockTracer.NONE, ResourceTypes.LABEL, unrelatedLabelId );
        order.verify( txState ).nodeDoAddProperty( 123, propertyKeyId, value );
    }

    @Test
    void shouldAcquireEntityWriteLockBeforeSettingPropertyOnRelationship() throws Exception
    {
        // given
        when( relationshipCursor.next() ).thenReturn( true );
        int propertyKeyId = 8;
        Value value = Values.of( 9 );
        when( propertyCursor.next() ).thenReturn( true );
        when( propertyCursor.propertyKey() ).thenReturn( propertyKeyId );
        when( propertyCursor.propertyValue() ).thenReturn( NO_VALUE );

        // when
        operations.relationshipSetProperty( 123, propertyKeyId, value );

        // then
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.RELATIONSHIP, 123 );
        order.verify( txState ).relationshipDoReplaceProperty( 123, propertyKeyId, NO_VALUE, value );
    }

    @Test
    void shouldNotAcquireEntityWriteLockBeforeSettingPropertyOnJustCreatedNode() throws Exception
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
        verify( txState ).nodeDoAddProperty( 123, propertyKeyId, value );
    }

    @Test
    void shouldNotAcquireEntityWriteLockBeforeSettingPropertyOnJustCreatedRelationship() throws Exception
    {
        // given
        when( relationshipCursor.next() ).thenReturn( true );
        when( transaction.hasTxStateWithChanges() ).thenReturn( true );
        txState.relationshipDoCreate( 123, 42, 43, 45 );
        int propertyKeyId = 8;
        Value value = Values.of( 9 );

        // when
        operations.relationshipSetProperty( 123, propertyKeyId, value );

        // then
        verify( locks, never() ).acquireExclusive( LockTracer.NONE, ResourceTypes.RELATIONSHIP, 123 );
        verify( txState ).relationshipDoReplaceProperty( 123, propertyKeyId, NO_VALUE, value );
    }

    @Test
    void shouldAcquireEntityWriteLockBeforeDeletingNode()
    {
        // GIVEN
        when( nodeCursor.next() ).thenReturn( true );
        when( nodeCursor.labels() ).thenReturn( LabelSet.NONE );
        when( allStoreHolder.nodeExistsInStore( 123 ) ).thenReturn( true );

        // WHEN
        operations.nodeDelete(  123 );

        //THEN
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 123 );
        order.verify( txState ).nodeDoDelete( 123 );
    }

    @Test
    void shouldNotAcquireEntityWriteLockBeforeDeletingJustCreatedNode()
    {
        // THEN
        txState.nodeDoCreate( 123 );
        when( transaction.hasTxStateWithChanges() ).thenReturn( true );

        // WHEN
        operations.nodeDelete( 123 );

        //THEN
        verify( locks, never() ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 123 );
        verify( txState ).nodeDoDelete( 123 );
    }

    @Test
    void shouldAcquireSchemaReadLockBeforeGettingConstraintsByLabelAndProperty()
    {
        // WHEN
        allStoreHolder.constraintsGetForSchema( descriptor );

        // THEN
        order.verify( locks ).acquireShared( LockTracer.NONE, ResourceTypes.LABEL, descriptor.getLabelId() );
        order.verify( storageReader ).constraintsGetForSchema( descriptor );
    }

    @Test
    void shouldAcquireSchemaReadLockBeforeGettingConstraintsByLabel()
    {
        // WHEN
        allStoreHolder.constraintsGetForLabel( 42 );

        // THEN
        order.verify( locks ).acquireShared( LockTracer.NONE, ResourceTypes.LABEL, 42 );
        order.verify( storageReader ).constraintsGetForLabel( 42 );
    }

    @Test
    void shouldAcquireSchemaReadLockBeforeCheckingExistenceConstraints()
    {
        // WHEN
        allStoreHolder.constraintExists( ConstraintDescriptorFactory.uniqueForSchema( descriptor ) );

        // THEN
        order.verify( locks ).acquireShared( LockTracer.NONE, ResourceTypes.LABEL, 123 );
        order.verify( storageReader ).constraintExists( any() );
    }

    @Test
    void shouldAcquireSchemaReadLockLazilyBeforeGettingAllConstraints()
    {
        // given
        int labelId = 1;
        int relTypeId = 2;
        UniquenessConstraintDescriptor uniquenessConstraint = uniqueForLabel( labelId, 2, 3, 3 );
        RelExistenceConstraintDescriptor existenceConstraint = existsForRelType( relTypeId, 3, 4, 5 );
        when( storageReader.constraintsGetAll() )
                .thenReturn( Iterators.iterator( uniquenessConstraint, existenceConstraint ) );

        // when
        Iterator<ConstraintDescriptor> result = allStoreHolder.constraintsGetAll( );
        Iterators.count( result );

        // then
        assertThat( asList( result ), empty() );
        order.verify( storageReader ).constraintsGetAll();
        order.verify( locks ).acquireShared( LockTracer.NONE, ResourceTypes.LABEL, labelId );
        order.verify( locks ).acquireShared( LockTracer.NONE, ResourceTypes.RELATIONSHIP_TYPE, relTypeId );
    }

    @Test
    void shouldAcquireSchemaWriteLockBeforeRemovingIndexRule() throws Exception
    {
        // given
        CapableIndexDescriptor index = TestIndexDescriptorFactory.forLabel( 0, 0 ).withId( 0 ).withoutCapabilities();
        IndexProxy indexProxy = mock( IndexProxy.class );
        when( indexProxy.getDescriptor() ).thenReturn( index );
        when( indexingService.getIndexProxy( index.schema() ) ).thenReturn( indexProxy );
        when( storageReader.indexGetForSchema( index.schema() ) ).thenReturn( index );

        // when
        operations.indexDrop( index );

        // then
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.LABEL, 0 );
        order.verify( txState ).indexDoDrop( index );
    }

    @Test
    void shouldAcquireSchemaWriteLockBeforeCreatingUniquenessConstraint() throws Exception
    {
        // given
        String defaultProvider = Config.defaults().get( default_schema_provider );
        when( constraintIndexCreator.createUniquenessConstraintIndex( transaction, descriptor, defaultProvider ) ).thenReturn( 42L );
        when( storageReader.constraintsGetForSchema(  descriptor.schema() ) ).thenReturn( Collections.emptyIterator() );

        // when
        operations.uniquePropertyConstraintCreate( descriptor );

        // then
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.LABEL, descriptor.getLabelId() );
        order.verify( txState ).constraintDoAdd( ConstraintDescriptorFactory.uniqueForSchema( descriptor ), 42L );
    }

    @Test
    void shouldAcquireSchemaWriteLockBeforeDroppingConstraint() throws Exception
    {
        // given
        UniquenessConstraintDescriptor constraint = uniqueForSchema( descriptor );
        when( storageReader.constraintExists( constraint ) ).thenReturn( true );

        // when
        operations.constraintDrop( constraint );

        // then
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.LABEL, descriptor.getLabelId() );
        order.verify( txState ).constraintDoDrop( constraint );
    }

    @Test
    void detachDeleteNodeWithoutRelationshipsExclusivelyLockNode() throws KernelException
    {
        long nodeId = 1L;
        returnRelationships( transaction, false, new TestRelationshipChain( nodeId ) );
        when( transaction.ambientNodeCursor() ).thenReturn( new StubNodeCursor( false ).withNode( nodeId ) );
        when( nodeCursor.next() ).thenReturn( true );
        LabelSet labels = mock( LabelSet.class );
        when( labels.all() ).thenReturn( EMPTY_LONG_ARRAY );
        when( nodeCursor.labels() ).thenReturn( labels );

        operations.nodeDetachDelete( nodeId );

        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, nodeId );
        order.verify( locks, never() ).releaseExclusive( ResourceTypes.NODE, nodeId );
        order.verify( txState ).nodeDoDelete( nodeId );
    }

    @Test
    void detachDeleteNodeExclusivelyLockNodes() throws KernelException
    {
        long nodeId = 1L;
        returnRelationships( transaction, false,
                new TestRelationshipChain( nodeId ).outgoing( 1, 2L, 42 ) );
        when( transaction.ambientNodeCursor() ).thenReturn( new StubNodeCursor( false ).withNode( nodeId ) );
        LabelSet labels = mock( LabelSet.class );
        when( labels.all() ).thenReturn( EMPTY_LONG_ARRAY );
        when( nodeCursor.labels() ).thenReturn( labels );
        when( nodeCursor.next() ).thenReturn( true );

        operations.nodeDetachDelete( nodeId );

        order.verify( locks ).acquireExclusive(
                LockTracer.NONE, ResourceTypes.NODE, nodeId, 2L );
        order.verify( locks, never() ).releaseExclusive( ResourceTypes.NODE, nodeId );
        order.verify( locks, never() ).releaseExclusive( ResourceTypes.NODE, 2L );
        order.verify( txState ).nodeDoDelete( nodeId );
    }

    @Test
    void shouldAcquiredSharedLabelLocksWhenDeletingNode()
    {
        // given
        long nodeId = 1L;
        long labelId1 = 1;
        long labelId2 = 2;
        when( nodeCursor.next() ).thenReturn( true );
        LabelSet labels = mock( LabelSet.class );
        when( labels.all() ).thenReturn( new long[]{labelId1, labelId2} );
        when( nodeCursor.labels() ).thenReturn( labels );

        // when
        operations.nodeDelete( nodeId );

        // then
        InOrder order = inOrder( locks );
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, nodeId );
        order.verify( locks ).acquireShared( LockTracer.NONE, ResourceTypes.LABEL, labelId1, labelId2 );
        order.verifyNoMoreInteractions();
    }

    @Test
    void shouldAcquiredSharedLabelLocksWhenDetachDeletingNode() throws KernelException
    {
        // given
        long nodeId = 1L;
        long labelId1 = 1;
        long labelId2 = 2;

        returnRelationships( transaction, false, new TestRelationshipChain( nodeId ) );
        when( transaction.ambientNodeCursor() ).thenReturn( new StubNodeCursor( false ).withNode( nodeId ) );
        when( nodeCursor.next() ).thenReturn( true );
        LabelSet labels = mock( LabelSet.class );
        when( labels.all() ).thenReturn( new long[]{labelId1, labelId2} );
        when( nodeCursor.labels() ).thenReturn( labels );

        // when
        operations.nodeDetachDelete( nodeId );

        // then
        InOrder order = inOrder( locks );
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, nodeId );
        order.verify( locks ).acquireShared( LockTracer.NONE, ResourceTypes.LABEL, labelId1, labelId2 );
        order.verifyNoMoreInteractions();
    }

    @Test
    void shouldAcquiredSharedLabelLocksWhenRemovingNodeLabel() throws EntityNotFoundException
    {
        // given
        long nodeId = 1L;
        int labelId = 1;
        when( nodeCursor.next() ).thenReturn( true );
        when( nodeCursor.hasLabel( labelId ) ).thenReturn( true );

        // when
        operations.nodeRemoveLabel( nodeId, labelId );

        // then
        InOrder order = inOrder( locks );
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, nodeId );
        order.verify( locks ).acquireShared( LockTracer.NONE, ResourceTypes.LABEL, labelId );
        order.verifyNoMoreInteractions();
    }

    @Test
    void shouldAcquiredSharedLabelLocksWhenRemovingNodeProperty() throws EntityNotFoundException
    {
        // given
        long nodeId = 1L;
        long labelId1 = 1;
        long labelId2 = 1;
        int propertyKeyId = 5;
        when( nodeCursor.next() ).thenReturn( true );
        LabelSet labels = mock( LabelSet.class );
        when( labels.all() ).thenReturn( new long[]{labelId1, labelId2} );
        when( nodeCursor.labels() ).thenReturn( labels );
        when( propertyCursor.next() ).thenReturn( true );
        when( propertyCursor.propertyKey() ).thenReturn( propertyKeyId );
        when( propertyCursor.propertyValue() ).thenReturn( Values.of( "abc" ) );

        // when
        operations.nodeRemoveProperty( nodeId, propertyKeyId );

        // then
        InOrder order = inOrder( locks );
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, nodeId );
        order.verify( locks ).acquireShared( LockTracer.NONE, ResourceTypes.LABEL, labelId1, labelId2 );
        order.verifyNoMoreInteractions();
    }

    private void setStoreRelationship( long relationshipId, long sourceNode, long targetNode, int relationshipLabel )
    {
        when( relationshipCursor.next() ).thenReturn( true );
        when( relationshipCursor.relationshipReference() ).thenReturn( relationshipId );
        when( relationshipCursor.sourceNodeReference() ).thenReturn( sourceNode );
        when( relationshipCursor.targetNodeReference() ).thenReturn( targetNode );
        when( relationshipCursor.type() ).thenReturn( relationshipLabel );
    }
}
