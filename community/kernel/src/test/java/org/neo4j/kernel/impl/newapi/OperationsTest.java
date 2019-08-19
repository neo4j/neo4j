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

import org.hamcrest.Matchers;
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
import org.neo4j.common.EntityType;
import org.neo4j.configuration.Config;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.index.label.LabelScanStore;
import org.neo4j.internal.kernel.api.LabelSet;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.internal.kernel.api.helpers.StubNodeCursor;
import org.neo4j.internal.kernel.api.helpers.TestRelationshipChain;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.RelationTypeSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
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
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.SimpleStatementLocks;
import org.neo4j.kernel.impl.util.DefaultValueMapper;
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.collection.PrimitiveLongCollections.EMPTY_LONG_ARRAY;
import static org.neo4j.configuration.GraphDatabaseSettings.default_schema_provider;
import static org.neo4j.internal.helpers.collection.Iterables.single;
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
    private final LabelSchemaDescriptor descriptor = SchemaDescriptor.forLabel( 123, 456 );
    private StorageReader storageReader;
    private StorageSchemaReader storageReaderSnapshot;
    private ConstraintIndexCreator constraintIndexCreator;
    private IndexingService indexingService;
    private TokenHolders tokenHolders;
    private CommandCreationContext creationContext;
    private IndexingProvidersService indexingProvidersService;
    private TxState realTxState;

    @BeforeEach
    void setUp() throws Exception
    {
        realTxState = new TxState();
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
        DefaultValueMapper mapper = mock( DefaultValueMapper.class );
        dependencies.satisfyDependency( mapper );
        allStoreHolder = new AllStoreHolder( storageReader, transaction, cursors, mock( GlobalProcedures.class ), mock( SchemaState.class ), indexingService,
                mock( LabelScanStore.class ), mock( IndexStatisticsStore.class ), dependencies );
        constraintIndexCreator = mock( ConstraintIndexCreator.class );
        tokenHolders = mockedTokenHolders();
        creationContext = mock( CommandCreationContext.class );
        indexingProvidersService = mock( IndexingProvidersService.class );
        when( indexingProvidersService.indexProviderByName( "native-btree-1.0" ) ).thenReturn( new IndexProviderDescriptor( "native-btree", "1.0" ) );
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
        allStoreHolder.constraintsGetForSchema( descriptor );

        // THEN
        order.verify( locks ).acquireShared( LockTracer.NONE, ResourceTypes.LABEL, descriptor.getLabelId() );
        order.verify( storageReader ).constraintsGetForSchema( descriptor );
    }

    @Test
    void shouldNotAcquireSchemaReadLockBeforeGettingIndexesByLabelAndProperty()
    {
        // WHEN
        allStoreHolder.index( descriptor );

        // THEN
        verifyNoMoreInteractions( locks );
        verify( storageReader ).indexGetForSchema( descriptor );
    }

    @Test
    void shouldNotAcquireSchemaReadLockWhenGettingIndexesByLabelAndPropertyFromSnapshot()
    {
        // WHEN
        allStoreHolder.snapshot().index( descriptor );

        // THEN
        verifyNoMoreInteractions( locks );
        verify( storageReaderSnapshot ).indexGetForSchema( descriptor );
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

        // then
        assertThat( Iterators.count( result ), Matchers.is( 2L ) );
        assertThat( asList( result ), empty() );
        order.verify( storageReader ).constraintsGetAll();
        order.verify( locks ).acquireShared( LockTracer.NONE, ResourceTypes.LABEL, labelId );
        order.verify( locks ).acquireShared( LockTracer.NONE, ResourceTypes.RELATIONSHIP_TYPE, relTypeId );
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
        assertThat( Iterators.count( result ), Matchers.is( 2L ) );
        assertThat( asList( result ), empty() );
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
        when( indexingService.getIndexProxy( index.schema() ) ).thenReturn( indexProxy );
        when( storageReader.indexGetForSchema( index.schema() ) ).thenReturn( index );

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
        when( indexingService.getIndexProxy( index.schema() ) ).thenReturn( indexProxy );
        when( storageReader.indexGetForSchema( index.schema() ) ).thenReturn( index );

        // when
        operations.indexDrop( index.schema() );

        // then
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.LABEL, 0 );
        order.verify( txState ).indexDoDrop( index );
    }

    @Test
    void shouldAcquireSchemaWriteLockBeforeCreatingUniquenessConstraint() throws Exception
    {
        // given
        String defaultProvider = Config.defaults().get( default_schema_provider );
        IndexDescriptor constraintIndex = IndexPrototype.uniqueForSchema( descriptor ).withName( "index_42" ).materialise( 42 );
        UniquenessConstraintDescriptor constraint = ConstraintDescriptorFactory.uniqueForSchema( descriptor );
        when( constraintIndexCreator.createUniquenessConstraintIndex( transaction, constraint, defaultProvider ) ).thenReturn( constraintIndex );
        IndexProxy indexProxy = mock( IndexProxy.class );
        when( indexProxy.getDescriptor() ).thenReturn( constraintIndex );
        when( indexingService.getIndexProxy( constraintIndex.schema() ) ).thenReturn( indexProxy );
        when( storageReader.constraintsGetForSchema(  descriptor.schema() ) ).thenReturn( Collections.emptyIterator() );

        // when
        operations.uniquePropertyConstraintCreate( descriptor, "constraint name" );

        // then
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.LABEL, descriptor.getLabelId() );
        order.verify( txState ).constraintDoAdd( ConstraintDescriptorFactory.uniqueForSchema( descriptor ), constraintIndex );
    }

    @Test
    void shouldReleaseAcquiredSchemaWriteLockIfConstraintCreationFails() throws Exception
    {
        // given
        UniquenessConstraintDescriptor constraint = uniqueForSchema( descriptor );
        when( storageReader.constraintExists( constraint ) ).thenReturn( true );
        int labelId = descriptor.getLabelId();
        int propertyId = descriptor.getPropertyId();
        when( tokenHolders.labelTokens().getTokenById( labelId ) ).thenReturn( new NamedToken( "Label", labelId ) );
        when( tokenHolders.propertyKeyTokens().getTokenById( propertyId ) ).thenReturn( new NamedToken( "prop", labelId ) );

        // when
        try
        {
            operations.uniquePropertyConstraintCreate( descriptor, "constraint name" );
            fail( "Expected an exception because this schema should already be constrained." );
        }
        catch ( AlreadyConstrainedException ignore )
        {
            // Good.
        }

        // then
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.LABEL, labelId );
        order.verify( storageReader ).constraintExists( constraint );
        order.verify( locks ).releaseExclusive( ResourceTypes.LABEL, labelId );
    }

    @Test
    void shouldReleaseAcquiredSchemaWriteLockIfConstraintWithIndexProviderCreationFails() throws Exception
    {
        // given
        String indexProvider = Config.defaults().get( default_schema_provider );
        UniquenessConstraintDescriptor constraint = uniqueForSchema( descriptor );
        when( storageReader.constraintExists( constraint ) ).thenReturn( true );
        int labelId = descriptor.getLabelId();
        int propertyId = descriptor.getPropertyId();
        when( tokenHolders.labelTokens().getTokenById( labelId ) ).thenReturn( new NamedToken( "Label", labelId ) );
        when( tokenHolders.propertyKeyTokens().getTokenById( propertyId ) ).thenReturn( new NamedToken( "prop", labelId ) );

        // when
        try
        {
            operations.uniquePropertyConstraintCreate( descriptor, indexProvider, "constraint name" );
            fail( "Expected an exception because this schema should already be constrained." );
        }
        catch ( AlreadyConstrainedException ignore )
        {
            // Good.
        }

        // then
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.LABEL, labelId );
        order.verify( storageReader ).constraintExists( constraint );
        order.verify( locks ).releaseExclusive( ResourceTypes.LABEL, labelId );
    }

    @Test
    void shouldReleaseAcquiredSchemaWriteLockIfNodeKeyConstraintCreationFails() throws Exception
    {
        // given
        NodeKeyConstraintDescriptor constraint = nodeKeyForSchema( descriptor );
        when( storageReader.constraintExists( constraint ) ).thenReturn( true );
        int labelId = descriptor.getLabelId();
        int propertyId = descriptor.getPropertyId();
        when( tokenHolders.labelTokens().getTokenById( labelId ) ).thenReturn( new NamedToken( "Label", labelId ) );
        when( tokenHolders.propertyKeyTokens().getTokenById( propertyId ) ).thenReturn( new NamedToken( "prop", labelId ) );

        // when
        try
        {
            operations.nodeKeyConstraintCreate( descriptor, "constraint name" );
            fail( "Expected an exception because this schema should already be constrained." );
        }
        catch ( AlreadyConstrainedException ignore )
        {
            // Good.
        }

        // then
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.LABEL, labelId );
        order.verify( storageReader ).constraintExists( constraint );
        order.verify( locks ).releaseExclusive( ResourceTypes.LABEL, labelId );
    }

    @Test
    void shouldReleaseAcquiredSchemaWriteLockIfNodeKeyConstraintWithIndexProviderCreationFails() throws Exception
    {
        // given
        String indexProvider = Config.defaults().get( default_schema_provider );
        NodeKeyConstraintDescriptor constraint = nodeKeyForSchema( descriptor );
        when( storageReader.constraintExists( constraint ) ).thenReturn( true );
        int labelId = descriptor.getLabelId();
        int propertyId = descriptor.getPropertyId();
        when( tokenHolders.labelTokens().getTokenById( labelId ) ).thenReturn( new NamedToken( "Label", labelId ) );
        when( tokenHolders.propertyKeyTokens().getTokenById( propertyId ) ).thenReturn( new NamedToken( "prop", labelId ) );

        // when
        try
        {
            operations.nodeKeyConstraintCreate( descriptor, indexProvider, "constraint name" );
            fail( "Expected an exception because this schema should already be constrained." );
        }
        catch ( AlreadyConstrainedException ignore )
        {
            // Good.
        }

        // then
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.LABEL, labelId );
        order.verify( storageReader ).constraintExists( constraint );
        order.verify( locks ).releaseExclusive( ResourceTypes.LABEL, labelId );
    }

    @Test
    void shouldReleaseAcquiredSchemaWriteLockIfNodePropertyExistenceConstraintCreationFails() throws Exception
    {
        // given
        NodeExistenceConstraintDescriptor constraint = existsForSchema( descriptor );
        when( storageReader.constraintExists( constraint ) ).thenReturn( true );
        int labelId = descriptor.getLabelId();
        int propertyId = descriptor.getPropertyId();
        when( tokenHolders.labelTokens().getTokenById( labelId ) ).thenReturn( new NamedToken( "Label", labelId ) );
        when( tokenHolders.propertyKeyTokens().getTokenById( propertyId ) ).thenReturn( new NamedToken( "prop", labelId ) );

        // when
        try
        {
            operations.nodePropertyExistenceConstraintCreate( descriptor, "constraint name" );
            fail( "Expected an exception because this schema should already be constrained." );
        }
        catch ( AlreadyConstrainedException ignore )
        {
            // Good.
        }

        // then
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.LABEL, labelId );
        order.verify( storageReader ).constraintExists( constraint );
        order.verify( locks ).releaseExclusive( ResourceTypes.LABEL, labelId );
    }

    @Test
    void shouldReleaseAcquiredSchemaWriteLockIfRelationshipPropertyExistenceConstraintCreationFails() throws Exception
    {
        // given
        RelationTypeSchemaDescriptor descriptor = SchemaDescriptor.forRelType( 11, 13 );
        RelExistenceConstraintDescriptor constraint = existsForSchema( descriptor );
        when( storageReader.constraintExists( constraint ) ).thenReturn( true );
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
        order.verify( storageReader ).constraintExists( constraint );
        order.verify( locks ).releaseExclusive( ResourceTypes.RELATIONSHIP_TYPE, relTypeId );
    }

    @Test
    void shouldAcquireSchemaWriteLockBeforeDroppingConstraint() throws Exception
    {
        // given
        UniquenessConstraintDescriptor constraint = uniqueForSchema( descriptor ).withName( "constraint" );
        IndexDescriptor index = IndexPrototype.uniqueForSchema( descriptor ).withName( "constraint" ).materialise( 13 );
        when( storageReader.constraintExists( constraint ) ).thenReturn( true );
        when( storageReader.indexGetForSchema( descriptor ) ).thenReturn( index );

        // when
        operations.constraintDrop( constraint );

        // then
        order.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.LABEL, descriptor.getLabelId() );
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
        operations.indexCreate( SchemaDescriptor.forLabel( 1, 1 ) );
        operations.indexCreate( SchemaDescriptor.fulltext( EntityType.NODE, IndexConfig.empty(), new int[] {2, 3}, new int[] {1, 2} ), null );
        operations.indexCreate( SchemaDescriptor.forLabel( 3, 1 ), "provider-1.0", null );
        IndexDescriptor[] indexDescriptors = txState.indexChanges().getAdded()
                .stream()
                .sorted( Comparator.comparing( d -> d.schema().getEntityTokenIds()[0] ) )
                .toArray( IndexDescriptor[]::new );
        assertThat( Arrays.toString( indexDescriptors ), indexDescriptors.length, is( 3 ) );
        assertThat( indexDescriptors[0].toString(), indexDescriptors[0].getId(), is( 1L ) );
        assertThat( indexDescriptors[1].toString(), indexDescriptors[1].getId(), is( 2L ) );
        assertThat( indexDescriptors[2].toString(), indexDescriptors[2].getId(), is( 3L ) );
        assertThat( indexDescriptors[0].toString(), indexDescriptors[0].getName(), is( "Index on :LabelA (PropA)" ) );
        assertThat( indexDescriptors[1].toString(), indexDescriptors[1].getName(), is( "Full-Text Index on :LabelB,:LabelC (PropA,PropB)" ) );
        assertThat( indexDescriptors[2].toString(), indexDescriptors[2].getName(), is( "Index on :LabelC (PropA)" ) );
    }

    @Test
    void uniqueIndexesMustBeNamedAfterTheirConstraints() throws Exception
    {
        when( creationContext.reserveSchema() ).thenReturn( 1L, 2L, 3L );
        operations.indexUniqueCreate( ConstraintDescriptorFactory.uniqueForLabel( 1, 1 ).withName( "My bla bla constraint" ), "provider-1.0" );
        IndexDescriptor indexDescriptor = single( txState.indexChanges().getAdded() );
        assertThat( indexDescriptor.toString(), indexDescriptor.getName(), is( "My bla bla constraint" ) );
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
