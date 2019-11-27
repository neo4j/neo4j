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

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;

import org.neo4j.collection.Dependencies;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.configuration.Config;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.index.label.LabelScanStore;
import org.neo4j.internal.kernel.api.LabelSet;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.kernel.api.helpers.StubNodeCursor;
import org.neo4j.internal.kernel.api.helpers.TestRelationshipChain;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.RelationTypeSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.internal.schema.constraints.IndexBackedConstraintDescriptor;
import org.neo4j.internal.schema.constraints.NodeExistenceConstraintDescriptor;
import org.neo4j.internal.schema.constraints.NodeKeyConstraintDescriptor;
import org.neo4j.internal.schema.constraints.RelExistenceConstraintDescriptor;
import org.neo4j.internal.schema.constraints.UniquenessConstraintDescriptor;
import org.neo4j.kernel.api.exceptions.schema.AlreadyConstrainedException;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexingProvidersService;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.index.schema.FulltextIndexProviderFactory;
import org.neo4j.kernel.impl.index.schema.GenericNativeIndexProvider;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.ResourceIds;
import org.neo4j.kernel.impl.locking.SimpleStatementLocks;
import org.neo4j.kernel.impl.locking.StatementLocks;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.ResourceTypes;
import org.neo4j.storageengine.api.CommandCreationContext;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.StorageSchemaReader;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.NamedToken;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.collection.PrimitiveLongCollections.EMPTY_LONG_ARRAY;
import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.internal.helpers.collection.Iterators.asList;
import static org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory.existsForRelType;
import static org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory.existsForSchema;
import static org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory.nodeKeyForSchema;
import static org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory.uniqueForLabel;
import static org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory.uniqueForSchema;
import static org.neo4j.kernel.impl.newapi.TwoPhaseNodeForRelationshipLockingTest.returnRelationships;
import static org.neo4j.test.rule.DatabaseRule.mockedTokenHolders;
import static org.neo4j.values.storable.Values.NO_VALUE;

class OperationsTest
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
    private final LabelSchemaDescriptor schema = SchemaDescriptor.forLabel( 123, 456 );
    private StorageReader storageReader;
    private StorageSchemaReader storageReaderSnapshot;
    private ConstraintIndexCreator constraintIndexCreator;
    private IndexingService indexingService;
    private TokenHolders tokenHolders;
    private CommandCreationContext creationContext;

    @BeforeEach
    void setUp() throws Exception
    {
        TxState realTxState = new TxState();
        txState = Mockito.spy( realTxState );
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
        storageReaderSnapshot = mock( StorageSchemaReader.class );
        when( storageReader.nodeExists( anyLong() ) ).thenReturn( true );
        when( storageReader.constraintsGetForLabel( anyInt() )).thenReturn( Collections.emptyIterator() );
        when( storageReader.constraintsGetAll() ).thenReturn( Collections.emptyIterator() );
        when( storageReader.schemaSnapshot() ).thenReturn( storageReaderSnapshot );
        when( engine.newReader() ).thenReturn( storageReader );
        indexingService = mock( IndexingService.class );
        Dependencies dependencies = new Dependencies();
        var facade = mock( GraphDatabaseFacade.class );
        dependencies.satisfyDependency( facade );
        allStoreHolder = new AllStoreHolder( storageReader, transaction, cursors, mock( GlobalProcedures.class ), mock( SchemaState.class ), indexingService,
                mock( LabelScanStore.class ), mock( IndexStatisticsStore.class ), dependencies );
        constraintIndexCreator = mock( ConstraintIndexCreator.class );
        tokenHolders = mockedTokenHolders();
        creationContext = mock( CommandCreationContext.class );
        IndexingProvidersService indexingProvidersService = mock( IndexingProvidersService.class );
        when( indexingProvidersService.indexProviderByName( "native-btree-1.0" ) ).thenReturn( GenericNativeIndexProvider.DESCRIPTOR );
        when( indexingProvidersService.getDefaultProvider() ).thenReturn( GenericNativeIndexProvider.DESCRIPTOR );
        when( indexingProvidersService.indexProviderByName( "fulltext-1.0" ) ).thenReturn( FulltextIndexProviderFactory.DESCRIPTOR );
        when( indexingProvidersService.getFulltextProvider() ).thenReturn( FulltextIndexProviderFactory.DESCRIPTOR );
        when( indexingProvidersService.indexProviderByName( "provider-1.0" ) ).thenReturn( new IndexProviderDescriptor( "provider", "1.0" ) );
        when( indexingProvidersService.completeConfiguration( any() ) ).thenAnswer( inv -> inv.getArgument( 0 ) );
        operations = new Operations( allStoreHolder, storageReader, mock( IndexTxStateUpdater.class ), creationContext,
                 transaction, new KernelToken( storageReader, creationContext, transaction, tokenHolders ), cursors,
                constraintIndexCreator, mock( ConstraintSemantics.class ), indexingProvidersService, Config.defaults() );
        operations.initialize();

        this.order = inOrder( locks, txState, storageReader, storageReaderSnapshot );
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
        allStoreHolder.constraintsGetForSchema( schema );

        // THEN
        order.verify( locks ).acquireShared( LockTracer.NONE, ResourceTypes.LABEL, schema.getLabelId() );
        order.verify( storageReader ).constraintsGetForSchema( schema );
    }

    @Test
    void shouldNotAcquireSchemaReadLockBeforeGettingIndexesByLabelAndProperty()
    {
        // WHEN
        allStoreHolder.index( schema );

        // THEN
        verifyNoMoreInteractions( locks );
        verify( storageReader ).indexGetForSchema( schema );
    }

    @Test
    void shouldNotAcquireSchemaReadLockWhenGettingIndexesByLabelAndPropertyFromSnapshot()
    {
        // WHEN
        allStoreHolder.snapshot().index( schema );

        // THEN
        verifyNoMoreInteractions( locks );
        verify( storageReaderSnapshot ).indexGetForSchema( schema );
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
    void shouldAcquireSchemaReadLockBeforeGettingConstraintsByRelationshipType()
    {
        // WHEN
        allStoreHolder.constraintsGetForRelationshipType( 42 );

        // THEN
        order.verify( locks ).acquireShared( LockTracer.NONE, ResourceTypes.RELATIONSHIP_TYPE, 42 );
        order.verify( storageReader ).constraintsGetForRelationshipType( 42 );
    }

    @Test
    void shouldNotAcquireSchemaReadLockBeforeGettingConstraintsByLabel()
    {
        // WHEN
        allStoreHolder.snapshot().constraintsGetForLabel( 42 );

        // THEN
        verifyNoMoreInteractions( locks );
        verify( storageReaderSnapshot ).constraintsGetForLabel( 42 );
    }

    @Test
    void shouldNotAcquireSchemaReadLockBeforeGettingConstraintsByRelationshipType()
    {
        // WHEN
        allStoreHolder.snapshot().constraintsGetForRelationshipType( 42 );

        // THEN
        verifyNoMoreInteractions( locks );
        verify( storageReaderSnapshot ).constraintsGetForRelationshipType( 42 );
    }

    @Test
    void shouldAcquireSchemaReadLockBeforeCheckingExistenceConstraints()
    {
        // WHEN
        allStoreHolder.constraintExists( ConstraintDescriptorFactory.uniqueForSchema( schema ) );

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
        when( storageReader.constraintExists( uniquenessConstraint ) ).thenReturn( true );
        when( storageReader.constraintExists( existenceConstraint ) ).thenReturn( true );

        // when
        Iterator<ConstraintDescriptor> result = allStoreHolder.constraintsGetAll( );

        // then
        assertThat( Iterators.count( result ) ).isEqualTo( 2L );
        assertThat( asList( result ) ).isEmpty();
        order.verify( storageReader ).constraintsGetAll();
        order.verify( locks, atLeastOnce() ).acquireShared( LockTracer.NONE, ResourceTypes.LABEL, labelId );
        order.verify( locks, atLeastOnce() ).acquireShared( LockTracer.NONE, ResourceTypes.RELATIONSHIP_TYPE, relTypeId );
    }

    @Test
    void shouldNotAcquireSchemaReadLockLazilyBeforeGettingAllConstraintsFromSnapshot()
    {
        // given
        int labelId = 1;
        int relTypeId = 2;
        UniquenessConstraintDescriptor uniquenessConstraint = uniqueForLabel( labelId, 2, 3, 3 );
        RelExistenceConstraintDescriptor existenceConstraint = existsForRelType( relTypeId, 3, 4, 5 );
        when( storageReaderSnapshot.constraintsGetAll() )
                .thenReturn( Iterators.iterator( uniquenessConstraint, existenceConstraint ) );

        // when
        Iterator<ConstraintDescriptor> result = allStoreHolder.snapshot().constraintsGetAll( );

        // then
        assertThat( Iterators.count( result ) ).isEqualTo( 2L );
        assertThat( asList( result ) ).isEmpty();
        verify( storageReaderSnapshot ).constraintsGetAll();
        verifyNoMoreInteractions( locks );
    }

    @Test
    void shouldAcquireSchemaWriteLockBeforeRemovingIndexRule() throws Exception
    {
        // given
        IndexDescriptor index = IndexPrototype.forSchema( SchemaDescriptor.forLabel( 0, 0 ) ).withName( "index" ).materialise( 0 );
        IndexProxy indexProxy = mock( IndexProxy.class );
        when( indexProxy.getDescriptor() ).thenReturn( index );
        when( indexingService.getIndexProxy( index ) ).thenReturn( indexProxy );
        when( storageReader.indexExists( index ) ).thenReturn( true );

        // when
        operations.indexDrop( index );

        // then
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.LABEL, 0 );
        order.verify( txState ).indexDoDrop( index );
    }

    @Test
    void shouldAcquireSchemaWriteLockBeforeRemovingIndexRuleBySchema() throws Exception
    {
        // given
        IndexDescriptor index = IndexPrototype.forSchema( SchemaDescriptor.forLabel( 0, 0 ) ).withName( "index" ).materialise( 0 );
        IndexProxy indexProxy = mock( IndexProxy.class );
        when( indexProxy.getDescriptor() ).thenReturn( index );
        when( indexingService.getIndexProxy( index ) ).thenReturn( indexProxy );
        when( storageReader.indexGetForSchema( index.schema() ) ).thenReturn( Iterators.iterator( index ) );
        when( storageReader.indexExists( index ) ).thenReturn( true );

        // when
        operations.indexDrop( index.schema() );

        // then
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.LABEL, 0 );
        order.verify( txState ).indexDoDrop( index );
    }

    @Test
    void shouldDropAllGeneralIndexesMatchingSchema() throws Exception
    {
        SchemaDescriptor schema = SchemaDescriptor.forLabel( 0, 0 );
        SchemaDescriptor ftsSchema = SchemaDescriptor.fulltext( NODE, new int[] {0}, new int[] {0} );
        IndexDescriptor indexA = IndexPrototype.forSchema( schema ).withName( "a" ).materialise( 0 );
        IndexDescriptor indexB = IndexPrototype.forSchema( ftsSchema ).withName( "b" ).withIndexType( IndexType.FULLTEXT ).materialise( 1 );
        IndexDescriptor indexC = IndexPrototype.forSchema( schema ).withName( "c" ).materialise( 2 );

        // The full-text index would not actually ever match the given schema,
        // but let's pretend in order to verify that we have safe-guards in place.
        when( storageReader.indexGetForSchema( schema ) ).thenReturn( Iterators.iterator( indexA, indexB, indexC ) );
        when( storageReader.indexExists( any( IndexDescriptor.class ) ) ).thenReturn( true );

        // when
        operations.indexDrop( schema );

        // then
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.LABEL, 0 );
        order.verify( txState ).indexDoDrop( indexA );
        order.verify( txState ).indexDoDrop( indexC );
        verify( txState, never() ).indexDoDrop( indexB );
    }

    @Test
    void shouldAcquireSchemaNameWriteLockBeforeRemovingIndexByName() throws Exception
    {
        // given
        String indexName = "My fancy index";
        IndexDescriptor index = IndexPrototype.forSchema( SchemaDescriptor.forLabel( 0, 0 ) ).withName( indexName ).materialise( 0 );
        IndexProxy indexProxy = mock( IndexProxy.class );
        when( indexProxy.getDescriptor() ).thenReturn( index );
        when( indexingService.getIndexProxy( index ) ).thenReturn( indexProxy );
        when( storageReader.indexGetForName( indexName ) ).thenReturn( index );
        when( storageReader.indexExists( index ) ).thenReturn( true );

        // when
        operations.indexDrop( indexName );

        // then
        long indexNameLock = ResourceIds.schemaNameResourceId( indexName );
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.SCHEMA_NAME, indexNameLock );
        order.verify( txState ).indexDoDrop( index );
    }

    @Test
    void shouldAcquireSchemaWriteLockBeforeCreatingUniquenessConstraint() throws Exception
    {
        // given
        IndexPrototype prototype = IndexPrototype.uniqueForSchema( schema )
                .withName( "constraint name" )
                .withIndexProvider( GenericNativeIndexProvider.DESCRIPTOR );
        IndexDescriptor constraintIndex = prototype.materialise( 42 );
        when( constraintIndexCreator.createUniquenessConstraintIndex( any(), any(), eq( prototype ) ) ).thenReturn( constraintIndex );
        IndexProxy indexProxy = mock( IndexProxy.class );
        when( indexProxy.getDescriptor() ).thenReturn( constraintIndex );
        when( indexingService.getIndexProxy( constraintIndex ) ).thenReturn( indexProxy );
        when( storageReader.constraintsGetForSchema( schema.schema() ) ).thenReturn( Collections.emptyIterator() );
        when( storageReader.indexGetForSchema( schema.schema() ) ).thenReturn( Collections.emptyIterator() );

        // when
        operations.uniquePropertyConstraintCreate( prototype );

        // then
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.LABEL, schema.getLabelId() );
        order.verify( txState ).constraintDoAdd( ConstraintDescriptorFactory.uniqueForSchema( schema ), constraintIndex );
    }

    @Test
    void shouldReleaseAcquiredSchemaWriteLockIfConstraintCreationFails() throws Exception
    {
        // given
        UniquenessConstraintDescriptor constraint = uniqueForSchema( schema );
        storageReaderWithConstraints( constraint );
        int labelId = schema.getLabelId();
        int propertyId = schema.getPropertyId();
        when( tokenHolders.labelTokens().getTokenById( labelId ) ).thenReturn( new NamedToken( "Label", labelId ) );
        when( tokenHolders.propertyKeyTokens().getTokenById( propertyId ) ).thenReturn( new NamedToken( "prop", labelId ) );

        // when
        try
        {
            operations.uniquePropertyConstraintCreate( IndexPrototype.uniqueForSchema( schema ).withName( "constraint name" ) );
            fail( "Expected an exception because this schema should already be constrained." );
        }
        catch ( AlreadyConstrainedException ignore )
        {
            // Good.
        }

        // then
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.LABEL, labelId );
        order.verify( storageReader ).constraintsGetForSchema( schema );
        order.verify( locks ).releaseExclusive( ResourceTypes.LABEL, labelId );
    }

    @Test
    void shouldReleaseAcquiredSchemaWriteLockIfNodeKeyConstraintCreationFails() throws Exception
    {
        // given
        NodeKeyConstraintDescriptor constraint = nodeKeyForSchema( schema );
        storageReaderWithConstraints( constraint );
        int labelId = schema.getLabelId();
        int propertyId = schema.getPropertyId();
        when( tokenHolders.labelTokens().getTokenById( labelId ) ).thenReturn( new NamedToken( "Label", labelId ) );
        when( tokenHolders.propertyKeyTokens().getTokenById( propertyId ) ).thenReturn( new NamedToken( "prop", labelId ) );

        // when
        try
        {
            operations.nodeKeyConstraintCreate( IndexPrototype.uniqueForSchema( schema ).withName( "constraint name" ) );
            fail( "Expected an exception because this schema should already be constrained." );
        }
        catch ( AlreadyConstrainedException ignore )
        {
            // Good.
        }

        // then
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.LABEL, labelId );
        order.verify( storageReader ).constraintsGetForSchema( schema );
        order.verify( locks ).releaseExclusive( ResourceTypes.LABEL, labelId );
    }

    @Test
    void shouldReleaseAcquiredSchemaWriteLockIfNodePropertyExistenceConstraintCreationFails() throws Exception
    {
        // given
        NodeExistenceConstraintDescriptor constraint = existsForSchema( schema );
        storageReaderWithConstraints( constraint );
        int labelId = schema.getLabelId();
        int propertyId = schema.getPropertyId();
        when( tokenHolders.labelTokens().getTokenById( labelId ) ).thenReturn( new NamedToken( "Label", labelId ) );
        when( tokenHolders.propertyKeyTokens().getTokenById( propertyId ) ).thenReturn( new NamedToken( "prop", labelId ) );

        // when
        try
        {
            operations.nodePropertyExistenceConstraintCreate( schema, "constraint name" );
            fail( "Expected an exception because this schema should already be constrained." );
        }
        catch ( AlreadyConstrainedException ignore )
        {
            // Good.
        }

        // then
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.LABEL, labelId );
        order.verify( storageReader ).constraintsGetForSchema( schema );
        order.verify( locks ).releaseExclusive( ResourceTypes.LABEL, labelId );
    }

    @Test
    void shouldReleaseAcquiredSchemaWriteLockIfRelationshipPropertyExistenceConstraintCreationFails() throws Exception
    {
        // given
        RelationTypeSchemaDescriptor descriptor = SchemaDescriptor.forRelType( 11, 13 );
        RelExistenceConstraintDescriptor constraint = existsForSchema( descriptor );
        storageReaderWithConstraints( constraint );
        int relTypeId = descriptor.getRelTypeId();
        int propertyId = descriptor.getPropertyId();
        when( tokenHolders.relationshipTypeTokens().getTokenById( relTypeId ) ).thenReturn( new NamedToken( "Label", relTypeId ) );
        when( tokenHolders.propertyKeyTokens().getTokenById( propertyId ) ).thenReturn( new NamedToken( "prop", relTypeId ) );

        // when
        try
        {
            operations.relationshipPropertyExistenceConstraintCreate( descriptor, "constraint name" );
            fail( "Expected an exception because this schema should already be constrained." );
        }
        catch ( AlreadyConstrainedException ignore )
        {
            // Good.
        }

        // then
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.RELATIONSHIP_TYPE, relTypeId );
        order.verify( storageReader ).constraintsGetForSchema( descriptor );
        order.verify( locks ).releaseExclusive( ResourceTypes.RELATIONSHIP_TYPE, relTypeId );
    }

    @Test
    void shouldAcquireSchemaWriteLockBeforeDroppingConstraint() throws Exception
    {
        // given
        UniquenessConstraintDescriptor constraint = uniqueForSchema( schema ).withName( "constraint" );
        IndexDescriptor index = IndexPrototype.uniqueForSchema( schema ).withName( "constraint" ).materialise( 13 );
        storageReaderWithConstraints( constraint );
        when( storageReader.indexExists( index ) ).thenReturn( true );
        when( storageReader.indexGetForName( "constraint" ) ).thenReturn( index );

        // when
        operations.constraintDrop( constraint );

        // then
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.LABEL, schema.getLabelId() );
        order.verify( txState ).constraintDoDrop( constraint );
        order.verify( txState ).indexDoDrop( index );
    }

    @Test
    void shouldAcquireSchemaNameWriteLockBeforeDroppingConstraintByName() throws Exception
    {
        // given
        UniquenessConstraintDescriptor constraint = uniqueForSchema( schema ).withName( "constraint" );
        IndexDescriptor index = IndexPrototype.uniqueForSchema( schema ).withName( "constraint" ).materialise( 13 );
        storageReaderWithConstraints( constraint );
        when( storageReader.indexExists( index ) ).thenReturn( true );
        when( storageReader.indexGetForName( "constraint" ) ).thenReturn( index );
        when( storageReader.constraintGetForName( "constraint" ) ).thenReturn( constraint );

        // when
        operations.constraintDrop( "constraint" );

        // then
        long nameLock = ResourceIds.schemaNameResourceId( "constraint" );
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.SCHEMA_NAME, nameLock );
        order.verify( txState ).constraintDoDrop( constraint );
        order.verify( txState ).indexDoDrop( index );
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

    @Test
    void mustAssignNameToIndexesThatDoNotHaveUserSuppliedName() throws Exception
    {
        when( creationContext.reserveSchema() ).thenReturn( 1L, 2L, 3L );
        when( tokenHolders.labelTokens().getTokenById( 1 ) ).thenReturn( new NamedToken( "LabelA", 1 ) );
        when( tokenHolders.labelTokens().getTokenById( 2 ) ).thenReturn( new NamedToken( "LabelB", 1 ) );
        when( tokenHolders.labelTokens().getTokenById( 3 ) ).thenReturn( new NamedToken( "LabelC", 1 ) );
        when( tokenHolders.propertyKeyTokens().getTokenById( 1 ) ).thenReturn( new NamedToken( "PropA", 1 ) );
        when( tokenHolders.propertyKeyTokens().getTokenById( 2 ) ).thenReturn( new NamedToken( "PropB", 2 ) );
        storageReaderWithoutConstraints();
        when( storageReader.indexGetForSchema( any() ) ).thenReturn( Collections.emptyIterator() );
        operations.indexCreate( SchemaDescriptor.forLabel( 1, 1 ), null );
        operations.indexCreate( IndexPrototype.forSchema(
                SchemaDescriptor.fulltext( NODE, new int[] {2, 3}, new int[] {1, 2} ) )
                .withIndexType( IndexType.FULLTEXT ) );
        operations.indexCreate( SchemaDescriptor.forLabel( 3, 1 ), "provider-1.0", null );
        IndexDescriptor[] indexDescriptors = txState.indexChanges().getAdded()
                .stream()
                .sorted( Comparator.comparing( d -> d.schema().getEntityTokenIds()[0] ) )
                .toArray( IndexDescriptor[]::new );
        assertThat( indexDescriptors.length ).as( Arrays.toString( indexDescriptors ) ).isEqualTo( 3 );
        assertThat( indexDescriptors[0].getId() ).as( indexDescriptors[0].toString() ).isEqualTo( 1L );
        assertThat( indexDescriptors[1].getId() ).as( indexDescriptors[1].toString() ).isEqualTo( 2L );
        assertThat( indexDescriptors[2].getId() ).as( indexDescriptors[2].toString() ).isEqualTo( 3L );
        assertThat( indexDescriptors[0].getName() ).as( indexDescriptors[0].toString() ).isEqualTo( "index_5c81a58e" );
        assertThat( indexDescriptors[1].getName() ).as( indexDescriptors[1].toString() ).isEqualTo( "index_2813986a" );
        assertThat( indexDescriptors[2].getName() ).as( indexDescriptors[2].toString() ).isEqualTo( "index_edb2dfd3" );
    }

    @Test
    void uniqueIndexesMustBeNamedAfterTheirConstraints() throws KernelException
    {
        when( creationContext.reserveSchema() ).thenReturn( 1L, 2L, 3L );
        when( storageReader.constraintsGetForSchema( any() ) ).thenReturn( Iterators.emptyResourceIterator() );
        when( storageReader.indexGetForSchema( any() ) ).thenReturn( Iterators.emptyResourceIterator() );
        String constraintName = "my_constraint";
        when( constraintIndexCreator.createUniquenessConstraintIndex( any(), any(), any() ) ).thenAnswer( i ->
                {
                    IndexPrototype prototype = i.getArgument( 2 );
                    Optional<String> name = prototype.getName();
                    assertTrue( name.isPresent() );
                    assertThat( name.get() ).isEqualTo( constraintName );
                    return prototype.materialise( 2 );
                } );
        IndexPrototype prototype = IndexPrototype.uniqueForSchema( schema ).withName( constraintName );
        IndexBackedConstraintDescriptor constraint = operations.uniquePropertyConstraintCreate( prototype ).asIndexBackedConstraint();
        assertThat( constraint.ownedIndexId() ).isEqualTo( 2L );
    }

    @Test
    void shouldAcquireTxStateBeforeAllocatingNodeIdInBareCreateMethod()
    {
        // given
        KernelTransactionImplementation ktx = mock( KernelTransactionImplementation.class );
        when( ktx.txState() ).thenReturn( mock( TransactionState.class ) );
        CommandCreationContext commandCreationContext = mock( CommandCreationContext.class );
        Operations operations = new Operations( mock( AllStoreHolder.class ), mock( StorageReader.class ), mock( IndexTxStateUpdater.class ),
                commandCreationContext, ktx, mock( KernelToken.class ), mock( DefaultPooledCursors.class ), mock( ConstraintIndexCreator.class ),
                mock( ConstraintSemantics.class ), mock( IndexingProvidersService.class ), mock( Config.class ) );

        // when
        operations.nodeCreate();

        // then
        InOrder inOrder = inOrder( ktx, commandCreationContext );
        inOrder.verify( ktx ).txState();
        inOrder.verify( commandCreationContext ).reserveNode();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void shouldAcquireTxStateBeforeAllocatingNodeIdInCreateWithLabelsMethod() throws ConstraintValidationException
    {
        // given
        KernelTransactionImplementation ktx = mock( KernelTransactionImplementation.class );
        when( ktx.txState() ).thenReturn( mock( TransactionState.class ) );
        StatementLocks statementLocks = mock( StatementLocks.class );
        when( ktx.statementLocks() ).thenReturn( statementLocks );
        when( statementLocks.optimistic() ).thenReturn( mock( Locks.Client.class ) );
        CommandCreationContext commandCreationContext = mock( CommandCreationContext.class );
        DefaultPooledCursors cursors = mock( DefaultPooledCursors.class );
        when( cursors.allocateFullAccessNodeCursor() ).thenReturn( mock( FullAccessNodeCursor.class ) );
        when( cursors.allocateFullAccessPropertyCursor() ).thenReturn( mock( FullAccessPropertyCursor.class ) );
        Operations operations = new Operations( mock( AllStoreHolder.class ), mock( StorageReader.class ), mock( IndexTxStateUpdater.class ),
                commandCreationContext, ktx, mock( KernelToken.class ), cursors, mock( ConstraintIndexCreator.class ),
                mock( ConstraintSemantics.class ), mock( IndexingProvidersService.class ), mock( Config.class ) );
        operations.initialize();

        // when
        operations.nodeCreateWithLabels( new int[]{1} );

        // then
        InOrder inOrder = inOrder( ktx, commandCreationContext );
        inOrder.verify( ktx ).txState();
        inOrder.verify( commandCreationContext ).reserveNode();
        inOrder.verify( ktx ).txState(); // for the constraints check for the label
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void shouldAcquireTxStateBeforeAllocatingRelationshipId() throws EntityNotFoundException
    {
        // given
        KernelTransactionImplementation ktx = mock( KernelTransactionImplementation.class );
        when( ktx.txState() ).thenReturn( mock( TransactionState.class ) );
        StatementLocks statementLocks = mock( StatementLocks.class );
        when( ktx.statementLocks() ).thenReturn( statementLocks );
        when( statementLocks.optimistic() ).thenReturn( mock( Locks.Client.class ) );
        when( statementLocks.pessimistic() ).thenReturn( mock( Locks.Client.class ) );
        CommandCreationContext commandCreationContext = mock( CommandCreationContext.class );
        AllStoreHolder allStoreHolder = mock( AllStoreHolder.class );
        when( allStoreHolder.nodeExists( anyLong() ) ).thenReturn( true );
        Operations operations = new Operations( allStoreHolder, mock( StorageReader.class ), mock( IndexTxStateUpdater.class ),
                commandCreationContext, ktx, mock( KernelToken.class ), mock( DefaultPooledCursors.class ), mock( ConstraintIndexCreator.class ),
                mock( ConstraintSemantics.class ), mock( IndexingProvidersService.class ), mock( Config.class ) );

        // when
        operations.relationshipCreate( 0, 1, 2 );

        // then
        InOrder inOrder = inOrder( ktx, commandCreationContext );
        inOrder.verify( ktx ).txState();
        inOrder.verify( commandCreationContext ).reserveRelationship();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void shouldAcquireTxStateBeforeAllocatingSchemaId() throws KernelException
    {
        // given
        KernelTransactionImplementation ktx = mock( KernelTransactionImplementation.class );
        when( ktx.txState() ).thenReturn( mock( TransactionState.class ) );
        StatementLocks statementLocks = mock( StatementLocks.class );
        when( ktx.statementLocks() ).thenReturn( statementLocks );
        when( statementLocks.optimistic() ).thenReturn( mock( Locks.Client.class ) );
        when( statementLocks.pessimistic() ).thenReturn( mock( Locks.Client.class ) );
        CommandCreationContext commandCreationContext = mock( CommandCreationContext.class );
        IndexingProvidersService indexingProvidersService = mock( IndexingProvidersService.class );
        when( indexingProvidersService.getDefaultProvider() ).thenReturn( mock( IndexProviderDescriptor.class ) );
        AllStoreHolder allStoreHolder = mock( AllStoreHolder.class );
        when( allStoreHolder.index( any() ) ).thenReturn( Iterators.emptyResourceIterator() );
        when( allStoreHolder.indexGetForName( any() ) ).thenReturn( IndexDescriptor.NO_INDEX );
        when( allStoreHolder.constraintsGetForSchema( any() ) ).thenReturn( Iterators.emptyResourceIterator() );
        Operations operations = new Operations( allStoreHolder, mock( StorageReader.class ), mock( IndexTxStateUpdater.class ),
                commandCreationContext, ktx, mock( KernelToken.class ), mock( DefaultPooledCursors.class ), mock( ConstraintIndexCreator.class ),
                mock( ConstraintSemantics.class ), indexingProvidersService, Config.defaults() );

        // when
        operations.indexCreate( IndexPrototype.forSchema( schema ).withName( "name" ) );

        // then
        InOrder inOrder = inOrder( ktx, commandCreationContext );
        inOrder.verify( ktx ).txState();
        inOrder.verify( commandCreationContext ).reserveSchema();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void indexedBackedConstraintCreateMustThrowOnIndexTypeFullText() throws Exception
    {
        // given
        IndexPrototype prototype = IndexPrototype.uniqueForSchema( schema )
                .withName( "constraint name" )
                .withIndexProvider( GenericNativeIndexProvider.DESCRIPTOR )
                .withIndexType( IndexType.FULLTEXT );
        IndexDescriptor constraintIndex = prototype.materialise( 42 );
        when( constraintIndexCreator.createUniquenessConstraintIndex( any(), any(), eq( prototype ) ) ).thenReturn( constraintIndex );
        IndexProxy indexProxy = mock( IndexProxy.class );
        when( indexProxy.getDescriptor() ).thenReturn( constraintIndex );
        when( indexingService.getIndexProxy( constraintIndex ) ).thenReturn( indexProxy );
        when( storageReader.constraintsGetForSchema( schema ) ).thenReturn( Collections.emptyIterator() );
        when( storageReader.indexGetForSchema( schema ) ).thenReturn( Collections.emptyIterator() );

        // when
        var e = assertThrows( KernelException.class, () -> operations.uniquePropertyConstraintCreate( prototype ) );
        assertThat( e.getUserMessage( TokenNameLookup.idTokenNameLookup ) ).contains( "FULLTEXT" );
    }

    @Test
    void indexedBackedConstraintCreateMustThrowOnFulltextSchemas() throws Exception
    {
        // given
        when( tokenHolders.labelTokens().getTokenById( anyInt() ) ).thenReturn( new NamedToken( "Label", 123 ) );
        when( tokenHolders.propertyKeyTokens().getTokenById( anyInt() ) ).thenReturn( new NamedToken( "prop", 456 ) );
        SchemaDescriptor schema = SchemaDescriptor.fulltext( NODE, this.schema.getEntityTokenIds(), this.schema.getPropertyIds() );
        IndexPrototype prototype = IndexPrototype.uniqueForSchema( schema )
                .withName( "constraint name" )
                .withIndexProvider( GenericNativeIndexProvider.DESCRIPTOR );
        IndexDescriptor constraintIndex = prototype.materialise( 42 );
        when( constraintIndexCreator.createUniquenessConstraintIndex( any(), any(), eq( prototype ) ) ).thenReturn( constraintIndex );
        IndexProxy indexProxy = mock( IndexProxy.class );
        when( indexProxy.getDescriptor() ).thenReturn( constraintIndex );
        when( indexingService.getIndexProxy( constraintIndex ) ).thenReturn( indexProxy );
        when( storageReader.constraintsGetForSchema( schema ) ).thenReturn( Collections.emptyIterator() );
        when( storageReader.indexGetForSchema( schema ) ).thenReturn( Collections.emptyIterator() );

        // when
        var e = assertThrows( KernelException.class, () -> operations.uniquePropertyConstraintCreate( prototype ) );
        assertThat( e.getUserMessage( TokenNameLookup.idTokenNameLookup ) ).contains( "full-text schema" );
    }

    @Test
    void indexedBackedConstraintCreateMustThrowOnRelationshipSchemas() throws Exception
    {
        // given
        when( tokenHolders.relationshipTypeTokens().getTokenById( anyInt() ) ).thenReturn( new NamedToken( "RelType", 123 ) );
        when( tokenHolders.propertyKeyTokens().getTokenById( anyInt() ) ).thenReturn( new NamedToken( "prop", 456 ) );
        SchemaDescriptor schema = SchemaDescriptor.forRelType( this.schema.getEntityTokenIds()[0], this.schema.getPropertyIds() );
        IndexPrototype prototype = IndexPrototype.uniqueForSchema( schema )
                .withName( "constraint name" )
                .withIndexProvider( GenericNativeIndexProvider.DESCRIPTOR );
        IndexDescriptor constraintIndex = prototype.materialise( 42 );
        when( constraintIndexCreator.createUniquenessConstraintIndex( any(), any(), eq( prototype ) ) ).thenReturn( constraintIndex );
        IndexProxy indexProxy = mock( IndexProxy.class );
        when( indexProxy.getDescriptor() ).thenReturn( constraintIndex );
        when( indexingService.getIndexProxy( constraintIndex ) ).thenReturn( indexProxy );
        when( storageReader.constraintsGetForSchema( schema ) ).thenReturn( Collections.emptyIterator() );
        when( storageReader.indexGetForSchema( schema ) ).thenReturn( Collections.emptyIterator() );

        // when
        var e = assertThrows( KernelException.class, () -> operations.uniquePropertyConstraintCreate( prototype ) );
        assertThat( e.getUserMessage( TokenNameLookup.idTokenNameLookup ) ).contains( "relationship type schema" );
    }

    @Test
    void indexedBackedConstraintCreateMustThrowOnNonUniqueIndexPrototypes() throws Exception
    {
        // given
        when( tokenHolders.labelTokens().getTokenById( anyInt() ) ).thenReturn( new NamedToken( "Label", 123 ) );
        when( tokenHolders.propertyKeyTokens().getTokenById( anyInt() ) ).thenReturn( new NamedToken( "prop", 456 ) );
        IndexPrototype prototype = IndexPrototype.forSchema( schema )
                .withName( "constraint name" )
                .withIndexProvider( GenericNativeIndexProvider.DESCRIPTOR );
        IndexDescriptor constraintIndex = prototype.materialise( 42 );
        when( constraintIndexCreator.createUniquenessConstraintIndex( any(), any(), eq( prototype ) ) ).thenReturn( constraintIndex );
        IndexProxy indexProxy = mock( IndexProxy.class );
        when( indexProxy.getDescriptor() ).thenReturn( constraintIndex );
        when( indexingService.getIndexProxy( constraintIndex ) ).thenReturn( indexProxy );
        when( storageReader.constraintsGetForSchema( schema ) ).thenReturn( Collections.emptyIterator() );
        when( storageReader.indexGetForSchema( schema ) ).thenReturn( Collections.emptyIterator() );

        // when
        var e = assertThrows( KernelException.class, () -> operations.uniquePropertyConstraintCreate( prototype ) );
        assertThat( e.getUserMessage( TokenNameLookup.idTokenNameLookup ) ).containsIgnoringCase( "index prototype" ).containsIgnoringCase( "not unique" );
    }

    private static Iterator<ConstraintDescriptor> asIterator( ConstraintDescriptor constraint )
    {
        return singletonList( constraint ).iterator();
    }

    private void storageReaderWithConstraints( ConstraintDescriptor constraint )
    {
        when( storageReader.constraintsGetForSchema( constraint.schema() ) ).thenReturn( asIterator( constraint ) );
        when( storageReader.constraintExists( constraint ) ).thenReturn( true );
    }

    private void storageReaderWithoutConstraints()
    {
        when( storageReader.constraintsGetForSchema( any() ) ).thenReturn( Iterables.<ConstraintDescriptor>empty().iterator() );
        when( storageReader.constraintExists( any() ) ).thenReturn( false );
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
