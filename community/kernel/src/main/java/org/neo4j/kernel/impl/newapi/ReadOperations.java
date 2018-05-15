/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.neo4j.collection.RawIterator;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.CapableIndexReference;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeExplicitIndexCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipExplicitIndexCursor;
import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.Scan;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.exceptions.explicitindex.ExplicitIndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.kernel.api.procs.ProcedureHandle;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.procs.UserAggregator;
import org.neo4j.internal.kernel.api.procs.UserFunctionHandle;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.SchemaUtil;
import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.index.IndexNotApplicableKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.IndexBrokenKernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.proc.BasicContext;
import org.neo4j.kernel.api.proc.Context;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.api.txstate.ExplicitIndexTransactionState;
import org.neo4j.kernel.api.txstate.TransactionCountingStateVisitor;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.kernel.impl.api.ClockContext;
import org.neo4j.kernel.impl.api.CountsRecordState;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.SchemaState;
import org.neo4j.kernel.impl.api.security.OverriddenAccessMode;
import org.neo4j.kernel.impl.api.security.RestrictedAccessMode;
import org.neo4j.kernel.impl.api.store.DefaultCapableIndexReference;
import org.neo4j.kernel.impl.api.store.DefaultIndexReference;
import org.neo4j.kernel.impl.index.ExplicitIndexStore;
import org.neo4j.kernel.impl.index.IndexEntityType;
import org.neo4j.kernel.impl.locking.LockTracer;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.register.Register;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.TransactionalDependencies;
import org.neo4j.storageengine.api.lock.ResourceType;
import org.neo4j.storageengine.api.schema.PopulationProgress;
import org.neo4j.storageengine.api.txstate.ReadableDiffSets;
import org.neo4j.values.AnyValue;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.storable.Value;

import static java.lang.String.format;
import static org.neo4j.helpers.collection.Iterators.emptyResourceIterator;
import static org.neo4j.helpers.collection.Iterators.filter;
import static org.neo4j.kernel.impl.api.store.DefaultIndexReference.toDescriptor;
import static org.neo4j.kernel.impl.locking.ResourceTypes.INDEX_ENTRY;
import static org.neo4j.kernel.impl.locking.ResourceTypes.indexEntryResourceId;
import static org.neo4j.kernel.impl.store.record.AbstractBaseRecord.NO_ID;
import static org.neo4j.register.Registers.newDoubleLongRegister;
import static org.neo4j.storageengine.api.txstate.TxStateVisitor.EMPTY;

/**
 * Implements all storage reading, with transaction state.
 * This class is designed to be generic, even for different {@link StorageEngine}.
 * It collects all parts related to reading into one surface. Some parts come from {@link StorageReader},
 * some from {@link Procedures} etc.
 *
 * Regarding transaction state: methods that simply positions cursors will pass responsibility for
 * reading through transaction state onto the cursors. Other methods that return an actual result or do actual
 * work inside the method will read through transaction state inside the method itself.
 * Therefore the current design is that the {@link StorageReader} will have to implement reading through transaction state too.
 *
 * @see TransactionalDependencies
 */
public class ReadOperations implements TxStateHolder,
        org.neo4j.internal.kernel.api.Read,
        org.neo4j.internal.kernel.api.ExplicitIndexRead,
        org.neo4j.internal.kernel.api.SchemaRead,
        org.neo4j.internal.kernel.api.Procedures,
        org.neo4j.internal.kernel.api.Locks,
        CursorFactory
{
    private final StorageReader storageReader;
    private final KernelTransactionImplementation ktx;
    private final ExplicitIndexStore explicitIndexStore;
    private final Procedures procedures;
    private final SchemaState schemaState;

    public ReadOperations(
            StorageReader read,
            KernelTransactionImplementation ktx,
            ExplicitIndexStore explicitIndexStore,
            Procedures procedures,
            SchemaState schemaState )
    {
        this.storageReader = read;
        this.ktx = ktx;
        this.explicitIndexStore = explicitIndexStore;
        this.procedures = procedures;
        this.schemaState = schemaState;
    }

    @Override
    public boolean nodeExists( long reference )
    {
        ktx.assertOpen();

        if ( hasTxStateWithChanges() )
        {
            TransactionState txState = txState();
            if ( txState.nodeIsDeletedInThisTx( reference ) )
            {
                return false;
            }
            else if ( txState.nodeIsAddedInThisTx( reference ) )
            {
                return true;
            }
        }
        return storageReader.nodeExists( reference );
    }

    @Override
    public long countsForNode( int labelId )
    {
        long count = countsForNodeWithoutTxState( labelId );
        if ( ktx.hasTxStateWithChanges() )
        {
            CountsRecordState counts = new CountsRecordState();
            try
            {
                TransactionState txState = ktx.txState();
                txState.accept( new TransactionCountingStateVisitor( EMPTY, storageReader,
                        txState, counts ) );
                if ( counts.hasChanges() )
                {
                    count += counts.nodeCount( labelId, newDoubleLongRegister() ).readSecond();
                }
            }
            catch ( ConstraintValidationException | CreateConstraintFailureException e )
            {
                throw new IllegalArgumentException( "Unexpected error: " + e.getMessage() );
            }
        }
        return count;
    }

    @Override
    public long countsForNodeWithoutTxState( int labelId )
    {
        return storageReader.countsForNode( labelId );
    }

    @Override
    public long countsForRelationship( int startLabelId, int typeId, int endLabelId )
    {
        long count = countsForRelationshipWithoutTxState( startLabelId, typeId, endLabelId );
        if ( ktx.hasTxStateWithChanges() )
        {
            CountsRecordState counts = new CountsRecordState();
            try
            {
                TransactionState txState = ktx.txState();
                txState.accept( new TransactionCountingStateVisitor( EMPTY, storageReader,
                        txState, counts ) );
                if ( counts.hasChanges() )
                {
                    count += counts.relationshipCount( startLabelId, typeId, endLabelId, newDoubleLongRegister() )
                            .readSecond();
                }
            }
            catch ( ConstraintValidationException | CreateConstraintFailureException e )
            {
                throw new IllegalArgumentException( "Unexpected error: " + e.getMessage() );
            }
        }
        return count;
    }

    @Override
    public long countsForRelationshipWithoutTxState( int startLabelId, int typeId, int endLabelId )
    {
        return storageReader.countsForRelationship( startLabelId, typeId, endLabelId );
    }

    @Override
    public boolean relationshipExists( long reference )
    {
        ktx.assertOpen();

        if ( hasTxStateWithChanges() )
        {
            TransactionState txState = txState();
            if ( txState.relationshipIsDeletedInThisTx( reference ) )
            {
                return false;
            }
            else if ( txState.relationshipIsAddedInThisTx( reference ) )
            {
                return true;
            }
        }
        return storageReader.relationshipExists( reference );
    }

    @Override
    public void graphProperties( PropertyCursor cursor )
    {
        ktx.assertOpen();
        storageReader.graphProperties( cursor );
    }

    @Override
    public void nodeExplicitIndexLookup( NodeExplicitIndexCursor cursor, String index, String key, Object value ) throws ExplicitIndexNotFoundKernelException
    {
        ktx.assertOpen();
        storageReader.nodeExplicitIndexLookup( cursor, index, key, value );
    }

    @Override
    public void nodeExplicitIndexQuery( NodeExplicitIndexCursor cursor, String index, Object query ) throws ExplicitIndexNotFoundKernelException
    {
        ktx.assertOpen();
        storageReader.nodeExplicitIndexQuery( cursor, index, query );
    }

    @Override
    public void nodeExplicitIndexQuery( NodeExplicitIndexCursor cursor, String index, String key, Object query ) throws ExplicitIndexNotFoundKernelException
    {
        ktx.assertOpen();
        storageReader.nodeExplicitIndexQuery( cursor, index, key, query );
    }

    @Override
    public boolean nodeExplicitIndexExists( String indexName, Map<String,String> customConfiguration )
    {
        ktx.assertOpen();
        return explicitIndexTxState().checkIndexExistence( IndexEntityType.Node, indexName, customConfiguration  );
    }

    @Override
    public Map<String,String> nodeExplicitIndexGetConfiguration( String indexName ) throws ExplicitIndexNotFoundKernelException
    {
        ktx.assertOpen();
        return explicitIndexStore.getNodeIndexConfiguration( indexName );
    }

    @Override
    public void relationshipExplicitIndexLookup( RelationshipExplicitIndexCursor cursor, String index, String key, Object value, long source, long target )
            throws ExplicitIndexNotFoundKernelException
    {
        ktx.assertOpen();
        storageReader.relationshipExplicitIndexLookup( cursor, index, key, value, source, target );
    }

    @Override
    public void relationshipExplicitIndexQuery( RelationshipExplicitIndexCursor cursor, String index, Object query, long source, long target )
            throws ExplicitIndexNotFoundKernelException
    {
        ktx.assertOpen();
        storageReader.relationshipExplicitIndexQuery( cursor, index, query, source, target );
    }

    @Override
    public void relationshipExplicitIndexQuery( RelationshipExplicitIndexCursor cursor, String index, String key, Object query, long source, long target )
            throws ExplicitIndexNotFoundKernelException
    {
        ktx.assertOpen();
        storageReader.relationshipExplicitIndexQuery( cursor, index, key, query, source, target );
    }

    @Override
    public boolean relationshipExplicitIndexExists( String indexName, Map<String,String> customConfiguration )
    {
        ktx.assertOpen();
        return explicitIndexTxState().checkIndexExistence( IndexEntityType.Relationship, indexName, customConfiguration  );
    }

    @Override
    public String[] nodeExplicitIndexesGetAll()
    {
        return explicitIndexStore.getAllNodeIndexNames();
    }

    @Override
    public String[] relationshipExplicitIndexesGetAll()
    {
        return explicitIndexStore.getAllRelationshipIndexNames();
    }

    @Override
    public Map<String,String> relationshipExplicitIndexGetConfiguration( String indexName ) throws ExplicitIndexNotFoundKernelException
    {
        return explicitIndexStore.getRelationshipIndexConfiguration( indexName );
    }

    @Override
    public void nodeIndexSeek( IndexReference index, NodeValueIndexCursor cursor, IndexOrder indexOrder, IndexQuery... query ) throws KernelException
    {
        ktx.assertOpen();
        storageReader.nodeIndexSeek( index, cursor, indexOrder, query );
    }

    @Override
    public long lockingNodeUniqueIndexSeek( IndexReference index, IndexQuery.ExactPredicate... predicates ) throws KernelException
    {
        ktx.assertOpen();
        assertIndexOnline( index );
        assertPredicatesMatchSchema( index, predicates );
        int labelId = index.label();

        Locks.Client locks = ktx.statementLocks().optimistic();
        LockTracer lockTracer = ktx.lockTracer();
        long indexEntryId = indexEntryResourceId( labelId, predicates );

        //First try to find node under a shared lock
        //if not found upgrade to exclusive and try again
        locks.acquireShared( lockTracer, INDEX_ENTRY, indexEntryId );
        try ( NodeValueIndexCursor cursor = storageReader.allocateNodeValueIndexCursor() )
        {
            long existingNode = storageReader.lockingNodeUniqueIndexSeek( index, predicates );
            if ( existingNode == NO_ID )
            {
                locks.releaseShared( INDEX_ENTRY, indexEntryId );
                locks.acquireExclusive( lockTracer, INDEX_ENTRY, indexEntryId );
                existingNode = storageReader.lockingNodeUniqueIndexSeek( index, predicates );
                if ( existingNode != NO_ID ) // we found it under the exclusive lock
                {
                    // downgrade to a shared lock
                    locks.acquireShared( lockTracer, INDEX_ENTRY, indexEntryId );
                    locks.releaseExclusive( INDEX_ENTRY, indexEntryId );
                }
            }

            return existingNode;
        }
    }

    @Override
    public void nodeIndexScan( IndexReference index, NodeValueIndexCursor cursor, IndexOrder indexOrder ) throws KernelException
    {
        ktx.assertOpen();
        storageReader.nodeIndexScan( index, cursor, indexOrder );
    }

    @Override
    public void nodeLabelScan( int label, NodeLabelIndexCursor cursor )
    {
        ktx.assertOpen();
        storageReader.nodeLabelScan( label, cursor );
    }

    @Override
    public void nodeLabelUnionScan( NodeLabelIndexCursor cursor, int... labels )
    {
        ktx.assertOpen();
        storageReader.nodeLabelUnionScan( cursor, labels );
    }

    @Override
    public void nodeLabelIntersectionScan( NodeLabelIndexCursor cursor, int... labels )
    {
        ktx.assertOpen();
        storageReader.nodeLabelIntersectionScan( cursor, labels );
    }

    @Override
    public Scan<NodeLabelIndexCursor> nodeLabelScan( int label )
    {
        ktx.assertOpen();
        return storageReader.nodeLabelScan( label );
    }

    @Override
    public void allNodesScan( NodeCursor cursor )
    {
        ktx.assertOpen();
        storageReader.allNodesScan( cursor );
    }

    @Override
    public Scan<NodeCursor> allNodesScan()
    {
        ktx.assertOpen();
        return storageReader.allNodesScan();
    }

    @Override
    public void singleNode( long reference, NodeCursor cursor )
    {
        ktx.assertOpen();
        storageReader.singleNode( reference, cursor );
    }

    @Override
    public long nodesGetCount()
    {
        ktx.assertOpen();
        long base = storageReader.nodesGetCount();
        return ktx.hasTxStateWithChanges() ? base + ktx.txState().addedAndRemovedNodes().delta() : base;
    }

    @Override
    public long relationshipsGetCount()
    {
        ktx.assertOpen();
        long base = storageReader.relationshipsGetCount();
        return ktx.hasTxStateWithChanges() ? base + ktx.txState().addedAndRemovedRelationships().delta() : base;
    }

    @Override
    public void singleRelationship( long reference, RelationshipScanCursor cursor )
    {
        ktx.assertOpen();
        storageReader.singleRelationship( reference, cursor );
    }

    @Override
    public void allRelationshipsScan( RelationshipScanCursor cursor )
    {
        ktx.assertOpen();
        storageReader.allRelationshipsScan( cursor );
    }

    @Override
    public Scan<RelationshipScanCursor> allRelationshipsScan()
    {
        ktx.assertOpen();
        return storageReader.allRelationshipsScan();
    }

    @Override
    public void relationshipTypeScan( int type, RelationshipScanCursor cursor )
    {
        ktx.assertOpen();
        storageReader.relationshipTypeScan( type, cursor );
    }

    @Override
    public Scan<RelationshipScanCursor> relationshipTypeScan( int type )
    {
        ktx.assertOpen();
        return storageReader.relationshipTypeScan( type );
    }

    @Override
    public void relationshipGroups( long nodeReference, long reference, RelationshipGroupCursor cursor )
    {
        ktx.assertOpen();
        storageReader.relationshipGroups( nodeReference, reference, cursor );
    }

    @Override
    public void relationships( long nodeReference, long reference, RelationshipTraversalCursor cursor )
    {
        ktx.assertOpen();
        storageReader.relationships( nodeReference, reference, cursor );
    }

    @Override
    public void nodeProperties( long nodeReference, long reference, PropertyCursor cursor )
    {
        ktx.assertOpen();
        storageReader.nodeProperties( nodeReference, reference, cursor );
    }

    @Override
    public void relationshipProperties( long relationshipReference, long reference, PropertyCursor cursor )
    {
        ktx.assertOpen();
        storageReader.relationshipProperties( relationshipReference, reference, cursor );
    }

    @Override
    public void futureNodeReferenceRead( long reference )
    {
        ktx.assertOpen();
        storageReader.futureNodeReferenceRead( reference );
    }

    @Override
    public void futureRelationshipsReferenceRead( long reference )
    {
        ktx.assertOpen();
        storageReader.futureRelationshipsReferenceRead( reference );
    }

    @Override
    public void futureNodePropertyReferenceRead( long reference )
    {
        ktx.assertOpen();
        storageReader.futureNodePropertyReferenceRead( reference );
    }

    @Override
    public void futureRelationshipPropertyReferenceRead( long reference )
    {
        ktx.assertOpen();
        storageReader.futureRelationshipPropertyReferenceRead( reference );
    }

    @Override
    public CapableIndexReference index( int label, int... properties )
    {
        ktx.assertOpen();

        LabelSchemaDescriptor descriptor;
        try
        {
            descriptor = SchemaDescriptorFactory.forLabel( label, properties );
        }
        catch ( IllegalArgumentException ignore )
        {
            // This means we have invalid label or property ids.
            return CapableIndexReference.NO_INDEX;
        }
        SchemaIndexDescriptor indexDescriptor = storageReader.indexGetForSchema( descriptor );
        if ( ktx.hasTxStateWithChanges() )
        {
            ReadableDiffSets<SchemaIndexDescriptor> diffSets =
                    ktx.txState().indexDiffSetsByLabel( label );
            if ( indexDescriptor != null )
            {
                if ( diffSets.isRemoved( indexDescriptor ) )
                {
                    return CapableIndexReference.NO_INDEX;
                }
                else
                {
                    return indexGetCapability( indexDescriptor );
                }
            }
            else
            {
                Iterator<SchemaIndexDescriptor> fromTxState = filter(
                        SchemaDescriptor.equalTo( descriptor ),
                        diffSets.apply( emptyResourceIterator() ) );
                if ( fromTxState.hasNext() )
                {
                    return DefaultCapableIndexReference.fromDescriptor( fromTxState.next() );
                }
                else
                {
                    return CapableIndexReference.NO_INDEX;
                }
            }
        }

        return indexDescriptor != null ? indexGetCapability( indexDescriptor ) : CapableIndexReference.NO_INDEX;
    }

    private CapableIndexReference indexGetCapability( SchemaIndexDescriptor schemaIndexDescriptor )
    {
        try
        {
            return storageReader.indexReference( schemaIndexDescriptor );
        }
        catch ( IndexNotFoundKernelException e )
        {
            throw new IllegalStateException( "Could not find capability for index " + schemaIndexDescriptor, e );
        }
    }

    private boolean checkIndexState( SchemaIndexDescriptor index, ReadableDiffSets<SchemaIndexDescriptor> diffSet )
            throws IndexNotFoundKernelException
    {
        if ( diffSet.isAdded( index ) )
        {
            return true;
        }
        if ( diffSet.isRemoved( index ) )
        {
            throw new IndexNotFoundKernelException( format( "Index on %s has been dropped in this transaction.",
                    index.userDescription( SchemaUtil.idTokenNameLookup ) ) );
        }
        return false;
    }

    @Override
    public Iterator<IndexReference> indexesGetForLabel( int labelId )
    {
        acquireSharedOptimisticLock( ResourceTypes.LABEL, labelId );
        ktx.assertOpen();

        Iterator<IndexReference> iterator = storageReader.indexesGetForLabel( labelId );
        if ( ktx.hasTxStateWithChanges() )
        {
            Iterator<SchemaIndexDescriptor> descriptors = Iterators.map( DefaultIndexReference::toDescriptor, iterator );
            descriptors = ktx.txState().indexDiffSetsByLabel( labelId ).apply( descriptors );
            iterator = Iterators.map( DefaultIndexReference::fromDescriptor, descriptors );
        }
        return optimisticLabelLock( iterator );
    }

    @Override
    public Iterator<IndexReference> indexesGetAll()
    {
        ktx.assertOpen();

        Iterator<IndexReference> iterator = storageReader.indexesGetAll();
        if ( ktx.hasTxStateWithChanges() )
        {
            Iterator<SchemaIndexDescriptor> descriptors = Iterators.map( DefaultIndexReference::toDescriptor, iterator );
            descriptors = ktx.txState().indexChanges().apply( descriptors );
            iterator = Iterators.map( DefaultIndexReference::fromDescriptor, descriptors );
        }
        return optimisticLabelLock( iterator );
    }

    private Iterator<IndexReference> optimisticLabelLock( Iterator<IndexReference> iterator )
    {
        return Iterators.map( reference ->
        {
            acquireSharedOptimisticLock( ResourceTypes.LABEL, reference.properties()[0] );
            return reference;
        }, iterator );
    }

    @Override
    public InternalIndexState indexGetState( IndexReference index ) throws IndexNotFoundKernelException
    {
        assertValidIndex( index );
        acquireSharedOptimisticLock( ResourceTypes.LABEL, index.label() );
        ktx.assertOpen();
        return indexGetState( toDescriptor( index ) );
    }

    private InternalIndexState indexGetState( SchemaIndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        // If index is in our state, then return populating
        if ( ktx.hasTxStateWithChanges() )
        {
            if ( checkIndexState( descriptor,
                    ktx.txState().indexDiffSetsByLabel( descriptor.schema().keyId() ) ) )
            {
                return InternalIndexState.POPULATING;
            }
        }

        return storageReader.indexGetState( descriptor );
    }

    private void assertValidIndex( IndexReference index ) throws IndexNotFoundKernelException
    {
        if ( index == CapableIndexReference.NO_INDEX )
        {
            throw new IndexNotFoundKernelException( "No index was found" );
        }
    }

    @Override
    public PopulationProgress indexGetPopulationProgress( IndexReference index )
            throws IndexNotFoundKernelException
    {
        acquireSharedOptimisticLock( ResourceTypes.LABEL, index.label() );
        ktx.assertOpen();
        SchemaIndexDescriptor descriptor = toDescriptor( index );

        if ( ktx.hasTxStateWithChanges() )
        {
            if ( checkIndexState( descriptor,
                    ktx.txState().indexDiffSetsByLabel( index.label() ) ) )
            {
                return PopulationProgress.NONE;
            }
        }

        return storageReader.indexGetPopulationProgress( descriptor.schema() );
    }

    @Override
    public Long indexGetOwningUniquenessConstraintId( IndexReference index )
    {
        acquireSharedOptimisticLock( ResourceTypes.LABEL, index.label() );
        ktx.assertOpen();
        return storageReader.indexGetOwningUniquenessConstraintId( toDescriptor( index ) );
    }

    @Override
    public long indexGetCommittedId( IndexReference index ) throws SchemaRuleNotFoundException
    {
        acquireSharedOptimisticLock( ResourceTypes.LABEL, index.label() );
        ktx.assertOpen();
        return storageReader.indexGetCommittedId( toDescriptor( index ) );
    }

    @Override
    public String indexGetFailure( IndexReference index ) throws IndexNotFoundKernelException
    {
        return storageReader.indexGetFailure( index );
    }

    @Override
    public double indexUniqueValuesSelectivity( IndexReference index ) throws IndexNotFoundKernelException
    {
        acquireSharedLabelLock( index.label() );
        ktx.assertOpen();
        return storageReader.indexUniqueValuesSelectivity( index );
    }

    @Override
    public long indexSize( IndexReference index ) throws IndexNotFoundKernelException
    {
        acquireSharedLabelLock( index.label() );
        ktx.assertOpen();
        return storageReader.indexSize( index );
    }

    @Override
    public long nodesCountIndexed( IndexReference index, long nodeId, Value value ) throws KernelException
    {
        ktx.assertOpen();
        return storageReader.nodesCountIndexed( index, nodeId, value );
    }

    @Override
    public Register.DoubleLongRegister indexUpdatesAndSize( IndexReference index, Register.DoubleLongRegister target )
            throws IndexNotFoundKernelException
    {
        ktx.assertOpen();
        return storageReader.indexUpdatesAndSize(
                SchemaDescriptorFactory.forLabel( index.label(), index.properties() ), target );
    }

    @Override
    public Register.DoubleLongRegister indexSample( IndexReference index, Register.DoubleLongRegister target )
            throws IndexNotFoundKernelException
    {
        ktx.assertOpen();
        return storageReader.indexSample(
                SchemaDescriptorFactory.forLabel( index.label(), index.properties() ), target );
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForSchema( SchemaDescriptor descriptor )
    {
        acquireSharedOptimisticLock( descriptor.keyType(), descriptor.keyId() );
        ktx.assertOpen();
        Iterator<ConstraintDescriptor> constraints = storageReader.constraintsGetForSchema( descriptor );
        if ( ktx.hasTxStateWithChanges() )
        {
            return ktx.txState().constraintsChangesForSchema( descriptor ).apply( constraints );
        }
        return constraints;
    }

    @Override
    public boolean constraintExists( ConstraintDescriptor descriptor )
    {
        SchemaDescriptor schema = descriptor.schema();
        acquireSharedOptimisticLock( schema.keyType(), schema.keyId() );
        ktx.assertOpen();
        boolean inStore = storageReader.constraintExists( descriptor );
        if ( ktx.hasTxStateWithChanges() )
        {
            ReadableDiffSets<ConstraintDescriptor> diffSet =
                    ktx.txState().constraintsChangesForSchema( descriptor.schema() );
            return diffSet.isAdded( descriptor ) || (inStore && !diffSet.isRemoved( descriptor ));
        }

        return inStore;
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForLabel( int labelId )
    {
        acquireSharedOptimisticLock( ResourceTypes.LABEL, labelId );
        ktx.assertOpen();
        Iterator<ConstraintDescriptor> constraints = storageReader.constraintsGetForLabel( labelId );
        if ( ktx.hasTxStateWithChanges() )
        {
            return ktx.txState().constraintsChangesForLabel( labelId ).apply( constraints );
        }
        return constraints;
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetAll()
    {
        ktx.assertOpen();
        Iterator<ConstraintDescriptor> constraints = storageReader.constraintsGetAll();
        if ( ktx.hasTxStateWithChanges() )
        {
            constraints = ktx.txState().constraintsChanges().apply( constraints );
        }
        return Iterators.map( constraintDescriptor ->
        {
            SchemaDescriptor schema = constraintDescriptor.schema();
            ktx.statementLocks().pessimistic().acquireShared( ktx.lockTracer(), schema.keyType(), schema.keyId() );
            return constraintDescriptor;
        }, constraints );
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForRelationshipType( int typeId )
    {
        acquireSharedOptimisticLock( ResourceTypes.RELATIONSHIP_TYPE, typeId );
        ktx.assertOpen();
        Iterator<ConstraintDescriptor> constraints = storageReader.constraintsGetForRelationshipType( typeId );
        if ( ktx.hasTxStateWithChanges() )
        {
            return ktx.txState().constraintsChangesForRelationshipType( typeId ).apply( constraints );
        }
        return constraints;
    }

    @Override
    public <K, V> V schemaStateGetOrCreate( K key, Function<K,V> creator )
    {
        return schemaState.getOrCreate( key, creator );
    }

    @Override
    public <K, V> V schemaStateGet( K key )
    {
        return schemaState.get( key );
    }

    @Override
    public void schemaStateFlush()
    {
        schemaState.clear();
    }

    @Override
    public TransactionState txState()
    {
        return ktx.txState();
    }

    @Override
    public ExplicitIndexTransactionState explicitIndexTxState()
    {
        return ktx.explicitIndexTxState();
    }

    @Override
    public boolean hasTxStateWithChanges()
    {
        return ktx.hasTxStateWithChanges();
    }

    @Override
    public void acquireExclusiveNodeLock( long... ids )
    {
        acquireExclusiveLock( ResourceTypes.NODE, ids );
        ktx.assertOpen();
    }

    @Override
    public void acquireExclusiveRelationshipLock( long... ids )
    {
        acquireExclusiveLock( ResourceTypes.RELATIONSHIP, ids );
        ktx.assertOpen();
    }

    @Override
    public void acquireExclusiveExplicitIndexLock( long... ids )
    {
        acquireExclusiveLock( ResourceTypes.EXPLICIT_INDEX, ids );
        ktx.assertOpen();
    }

    @Override
    public void acquireExclusiveLabelLock( long... ids )
    {
        acquireExclusiveLock( ResourceTypes.LABEL, ids );
        ktx.assertOpen();
    }

    @Override
    public void releaseExclusiveNodeLock( long... ids )
    {
        releaseExclusiveLock( ResourceTypes.NODE, ids );
        ktx.assertOpen();
    }

    @Override
    public void releaseExclusiveRelationshipLock( long... ids )
    {
        releaseExclusiveLock( ResourceTypes.RELATIONSHIP, ids );
        ktx.assertOpen();
    }

    @Override
    public void releaseExclusiveExplicitIndexLock( long... ids )
    {
        releaseExclusiveLock( ResourceTypes.EXPLICIT_INDEX, ids );
        ktx.assertOpen();
    }

    @Override
    public void releaseExclusiveLabelLock( long... ids )
    {
        releaseExclusiveLock( ResourceTypes.LABEL, ids );
        ktx.assertOpen();
    }

    @Override
    public void acquireSharedNodeLock( long... ids )
    {
        acquireSharedLock( ResourceTypes.NODE, ids );
        ktx.assertOpen();
    }

    @Override
    public void acquireSharedRelationshipLock( long... ids )
    {
        acquireSharedLock( ResourceTypes.RELATIONSHIP, ids );
        ktx.assertOpen();
    }

    @Override
    public void acquireSharedExplicitIndexLock( long... ids )
    {
        acquireSharedLock( ResourceTypes.EXPLICIT_INDEX, ids );
        ktx.assertOpen();
    }

    @Override
    public void acquireSharedLabelLock( long... ids )
    {
        acquireSharedLock( ResourceTypes.LABEL, ids );
        ktx.assertOpen();
    }

    @Override
    public void releaseSharedNodeLock( long... ids )
    {
        releaseSharedLock( ResourceTypes.NODE, ids );
        ktx.assertOpen();
    }

    @Override
    public void releaseSharedRelationshipLock( long... ids )
    {
        releaseSharedLock( ResourceTypes.RELATIONSHIP, ids );
        ktx.assertOpen();
    }

    @Override
    public void releaseSharedExplicitIndexLock( long... ids )
    {
        releaseSharedLock( ResourceTypes.EXPLICIT_INDEX, ids );
        ktx.assertOpen();
    }

    @Override
    public void releaseSharedLabelLock( long... ids )
    {
        releaseSharedLock( ResourceTypes.LABEL, ids );
        ktx.assertOpen();
    }

    private void acquireSharedOptimisticLock( ResourceType resource, long resourceId )
    {
        ktx.statementLocks().optimistic().acquireShared( ktx.lockTracer(), resource, resourceId );
    }

    private void acquireExclusiveLock( ResourceTypes types, long... ids )
    {
        ktx.statementLocks().pessimistic().acquireExclusive( ktx.lockTracer(), types, ids );
    }

    private void releaseExclusiveLock( ResourceTypes types, long... ids )
    {
        ktx.statementLocks().pessimistic().releaseExclusive( types, ids );
    }

    private void acquireSharedLock( ResourceTypes types, long... ids )
    {
        ktx.statementLocks().pessimistic().acquireShared( ktx.lockTracer(), types, ids );
    }

    private void releaseSharedLock( ResourceTypes types, long... ids )
    {
        ktx.statementLocks().pessimistic().releaseShared( types, ids );
    }

    @Override
    public UserFunctionHandle functionGet( QualifiedName name )
    {
        ktx.assertOpen();
        return procedures.function( name );
    }

    @Override
    public ProcedureHandle procedureGet( QualifiedName name ) throws ProcedureException
    {
        ktx.assertOpen();
        return procedures.procedure( name );
    }

    @Override
    public Set<ProcedureSignature> proceduresGetAll( )
    {
        ktx.assertOpen();
        return procedures.getAllProcedures();
    }

    @Override
    public UserFunctionHandle aggregationFunctionGet( QualifiedName name )
    {
        ktx.assertOpen();
        return procedures.aggregationFunction( name );
    }

    private RawIterator<Object[],ProcedureException> callProcedure(
            int id, Object[] input, final AccessMode override )
            throws ProcedureException
    {
        ktx.assertOpen();

        final SecurityContext procedureSecurityContext = ktx.securityContext().withMode( override );
        final RawIterator<Object[],ProcedureException> procedureCall;
        try ( KernelTransaction.Revertable ignore = ktx.overrideWith( procedureSecurityContext );
                Statement statement = ktx.acquireStatement() )
        {
            procedureCall = procedures
                    .callProcedure( populateProcedureContext( procedureSecurityContext ), id, input, statement );
        }
        return createIterator( procedureSecurityContext, procedureCall );
    }

    private RawIterator<Object[],ProcedureException> callProcedure(
            QualifiedName name, Object[] input, final AccessMode override )
            throws ProcedureException
    {
        ktx.assertOpen();

        final SecurityContext procedureSecurityContext = ktx.securityContext().withMode( override );
        final RawIterator<Object[],ProcedureException> procedureCall;
        try ( KernelTransaction.Revertable ignore = ktx.overrideWith( procedureSecurityContext );
                Statement statement = ktx.acquireStatement() )
        {
            procedureCall = procedures
                    .callProcedure( populateProcedureContext( procedureSecurityContext ), name, input, statement );
        }
        return createIterator( procedureSecurityContext, procedureCall );
    }

    private RawIterator<Object[],ProcedureException> createIterator( SecurityContext procedureSecurityContext,
            RawIterator<Object[],ProcedureException> procedureCall )
    {
        return new RawIterator<Object[],ProcedureException>()
        {
            @Override
            public boolean hasNext() throws ProcedureException
            {
                try ( KernelTransaction.Revertable ignore = ktx.overrideWith( procedureSecurityContext ) )
                {
                    return procedureCall.hasNext();
                }
            }

            @Override
            public Object[] next() throws ProcedureException
            {
                try ( KernelTransaction.Revertable ignore = ktx.overrideWith( procedureSecurityContext ) )
                {
                    return procedureCall.next();
                }
            }
        };
    }

    private AnyValue callFunction( int id, AnyValue[] input, final AccessMode mode ) throws ProcedureException
    {
        ktx.assertOpen();

        try ( KernelTransaction.Revertable ignore = ktx.overrideWith( ktx.securityContext().withMode( mode ) ) )
        {
            return procedures.callFunction( populateFunctionContext(), id, input );
        }
    }

    private AnyValue callFunction( QualifiedName name, AnyValue[] input, final AccessMode mode )
            throws ProcedureException
    {
        ktx.assertOpen();

        try ( KernelTransaction.Revertable ignore = ktx.overrideWith( ktx.securityContext().withMode( mode ) ) )
        {
            return procedures.callFunction( populateFunctionContext(), name, input );
        }
    }

    private UserAggregator aggregationFunction( int id, final AccessMode mode )
            throws ProcedureException
    {
        ktx.assertOpen();

        try ( KernelTransaction.Revertable ignore = ktx.overrideWith( ktx.securityContext().withMode( mode ) ) )
        {
            return procedures.createAggregationFunction( populateAggregationContext(), id );
        }
    }

    private UserAggregator aggregationFunction( QualifiedName name, final AccessMode mode )
            throws ProcedureException
    {
        ktx.assertOpen();

        try ( KernelTransaction.Revertable ignore = ktx.overrideWith( ktx.securityContext().withMode( mode ) ) )
        {
            return procedures.createAggregationFunction( populateAggregationContext(), name );
        }
    }

    private BasicContext populateFunctionContext()
    {
        BasicContext ctx = new BasicContext();
        ctx.put( Context.KERNEL_TRANSACTION, ktx );
        ctx.put( Context.THREAD, Thread.currentThread() );
        ClockContext clocks = ktx.clocks();
        ctx.put( Context.SYSTEM_CLOCK, clocks.systemClock() );
        ctx.put( Context.STATEMENT_CLOCK, clocks.statementClock() );
        ctx.put( Context.TRANSACTION_CLOCK, clocks.transactionClock() );
        return ctx;
    }

    private BasicContext populateAggregationContext()
    {
        BasicContext ctx = new BasicContext();
        ctx.put( Context.KERNEL_TRANSACTION, ktx );
        ctx.put( Context.THREAD, Thread.currentThread() );
        return ctx;
    }

    private BasicContext populateProcedureContext( SecurityContext procedureSecurityContext )
    {
        BasicContext ctx = new BasicContext();
        ctx.put( Context.KERNEL_TRANSACTION, ktx );
        ctx.put( Context.THREAD, Thread.currentThread() );
        ctx.put( Context.SECURITY_CONTEXT, procedureSecurityContext );
        return ctx;
    }

    @Override
    public RawIterator<Object[],ProcedureException> procedureCallRead( int id, Object[] arguments )
            throws ProcedureException
    {
        AccessMode accessMode = ktx.securityContext().mode();
        if ( !accessMode.allowsReads() )
        {
            throw accessMode.onViolation( format( "Read operations are not allowed for %s.",
                    ktx.securityContext().description() ) );
        }
        return callProcedure( id, arguments, new RestrictedAccessMode( ktx.securityContext().mode(), AccessMode.Static
                .READ ) );
    }

    @Override
    public RawIterator<Object[],ProcedureException> procedureCallReadOverride( int id, Object[] arguments )
            throws ProcedureException
    {
        return callProcedure( id, arguments,
                new OverriddenAccessMode( ktx.securityContext().mode(), AccessMode.Static.READ ) );
    }

    @Override
    public RawIterator<Object[],ProcedureException> procedureCallWrite( int id, Object[] arguments )
            throws ProcedureException
    {
        AccessMode accessMode = ktx.securityContext().mode();
        if ( !accessMode.allowsWrites() )
        {
            throw accessMode.onViolation( format( "Write operations are not allowed for %s.",
                    ktx.securityContext().description() ) );
        }
        return callProcedure( id, arguments,
                new RestrictedAccessMode( ktx.securityContext().mode(), AccessMode.Static.TOKEN_WRITE ) );
    }

    @Override
    public RawIterator<Object[],ProcedureException> procedureCallWriteOverride( int id, Object[] arguments )
            throws ProcedureException
    {
        return callProcedure( id, arguments,
                new OverriddenAccessMode( ktx.securityContext().mode(), AccessMode.Static.TOKEN_WRITE ) );

    }

    @Override
    public RawIterator<Object[],ProcedureException> procedureCallSchema( int id, Object[] arguments )
            throws ProcedureException
    {
        AccessMode accessMode = ktx.securityContext().mode();
        if ( !accessMode.allowsSchemaWrites() )
        {
            throw accessMode.onViolation( format( "Schema operations are not allowed for %s.",
                    ktx.securityContext().description() ) );
        }
        return callProcedure( id, arguments,
                new RestrictedAccessMode( ktx.securityContext().mode(), AccessMode.Static.FULL ) );
    }

    @Override
    public RawIterator<Object[],ProcedureException> procedureCallSchemaOverride( int id, Object[] arguments )
            throws ProcedureException
    {
        return callProcedure( id, arguments,
                new OverriddenAccessMode( ktx.securityContext().mode(), AccessMode.Static.FULL ) );
    }

    @Override
    public RawIterator<Object[],ProcedureException> procedureCallRead( QualifiedName name, Object[] arguments )
            throws ProcedureException
    {
        AccessMode accessMode = ktx.securityContext().mode();
        if ( !accessMode.allowsReads() )
        {
            throw accessMode.onViolation( format( "Read operations are not allowed for %s.",
                    ktx.securityContext().description() ) );
        }
        return callProcedure( name, arguments, new RestrictedAccessMode( ktx.securityContext().mode(), AccessMode.Static
                .READ ) );
    }

    @Override
    public RawIterator<Object[],ProcedureException> procedureCallReadOverride( QualifiedName name, Object[] arguments )
            throws ProcedureException
    {
        return callProcedure( name, arguments,
                new OverriddenAccessMode( ktx.securityContext().mode(), AccessMode.Static.READ ) );
    }

    @Override
    public RawIterator<Object[],ProcedureException> procedureCallWrite( QualifiedName name, Object[] arguments )
            throws ProcedureException
    {
        AccessMode accessMode = ktx.securityContext().mode();
        if ( !accessMode.allowsWrites() )
        {
            throw accessMode.onViolation( format( "Write operations are not allowed for %s.",
                    ktx.securityContext().description() ) );
        }
        return callProcedure( name, arguments,
                new RestrictedAccessMode( ktx.securityContext().mode(), AccessMode.Static.TOKEN_WRITE ) );
    }

    @Override
    public RawIterator<Object[],ProcedureException> procedureCallWriteOverride( QualifiedName name, Object[] arguments )
            throws ProcedureException
    {
        return callProcedure( name, arguments,
                new OverriddenAccessMode( ktx.securityContext().mode(), AccessMode.Static.TOKEN_WRITE ) );

    }

    @Override
    public RawIterator<Object[],ProcedureException> procedureCallSchema( QualifiedName name, Object[] arguments )
            throws ProcedureException
    {
        AccessMode accessMode = ktx.securityContext().mode();
        if ( !accessMode.allowsSchemaWrites() )
        {
            throw accessMode.onViolation( format( "Schema operations are not allowed for %s.",
                    ktx.securityContext().description() ) );
        }
        return callProcedure( name, arguments,
                new RestrictedAccessMode( ktx.securityContext().mode(), AccessMode.Static.FULL ) );
    }

    @Override
    public RawIterator<Object[],ProcedureException> procedureCallSchemaOverride( QualifiedName name,
            Object[] arguments )
            throws ProcedureException
    {
        return callProcedure( name, arguments,
                new OverriddenAccessMode( ktx.securityContext().mode(), AccessMode.Static.FULL ) );
    }

    @Override
    public AnyValue functionCall( int id, AnyValue[] arguments ) throws ProcedureException
    {
        if ( !ktx.securityContext().mode().allowsReads() )
        {
            throw ktx.securityContext().mode().onViolation(
                    format( "Read operations are not allowed for %s.", ktx.securityContext().description() ) );
        }
        return callFunction( id, arguments,
                new RestrictedAccessMode( ktx.securityContext().mode(), AccessMode.Static.READ ) );
    }

    @Override
    public AnyValue functionCall( QualifiedName name, AnyValue[] arguments ) throws ProcedureException
    {
        if ( !ktx.securityContext().mode().allowsReads() )
        {
            throw ktx.securityContext().mode().onViolation(
                    format( "Read operations are not allowed for %s.", ktx.securityContext().description() ) );
        }
        return callFunction( name, arguments,
                new RestrictedAccessMode( ktx.securityContext().mode(), AccessMode.Static.READ ) );
    }

    @Override
    public AnyValue functionCallOverride( int id, AnyValue[] arguments ) throws ProcedureException
    {
        return callFunction( id, arguments,
                new OverriddenAccessMode( ktx.securityContext().mode(), AccessMode.Static.READ ) );
    }

    @Override
    public AnyValue functionCallOverride( QualifiedName name, AnyValue[] arguments ) throws ProcedureException
    {
        return callFunction( name, arguments,
                new OverriddenAccessMode( ktx.securityContext().mode(), AccessMode.Static.READ ) );
    }

    @Override
    public UserAggregator aggregationFunction( int id ) throws ProcedureException
    {
        if ( !ktx.securityContext().mode().allowsReads() )
        {
            throw ktx.securityContext().mode().onViolation(
                    format( "Read operations are not allowed for %s.", ktx.securityContext().description() ) );
        }
        return aggregationFunction( id,
                new RestrictedAccessMode( ktx.securityContext().mode(), AccessMode.Static.READ ) );
    }

    @Override
    public UserAggregator aggregationFunction( QualifiedName name ) throws ProcedureException
    {
        if ( !ktx.securityContext().mode().allowsReads() )
        {
            throw ktx.securityContext().mode().onViolation(
                    format( "Read operations are not allowed for %s.", ktx.securityContext().description() ) );
        }
        return aggregationFunction( name,
                new RestrictedAccessMode( ktx.securityContext().mode(), AccessMode.Static.READ ) );
    }

    @Override
    public UserAggregator aggregationFunctionOverride( int id ) throws ProcedureException
    {
        return aggregationFunction( id,
                new OverriddenAccessMode( ktx.securityContext().mode(), AccessMode.Static.READ ) );
    }

    @Override
    public UserAggregator aggregationFunctionOverride( QualifiedName name ) throws ProcedureException
    {
        return aggregationFunction( name,
                new OverriddenAccessMode( ktx.securityContext().mode(), AccessMode.Static.READ ) );
    }

    @Override
    public ValueMapper<Object> valueMapper()
    {
        return procedures.valueMapper();
    }

    private void assertIndexOnline( IndexReference index )
            throws IndexNotFoundKernelException, IndexBrokenKernelException
    {
        switch ( indexGetState( index ) )
        {
        case ONLINE:
            return;
        default:
            throw new IndexBrokenKernelException( indexGetFailure( index ) );
        }
    }

    private void assertPredicatesMatchSchema( IndexReference index, IndexQuery.ExactPredicate[] predicates )
            throws IndexNotApplicableKernelException
    {
        int[] propertyIds = index.properties();
        if ( propertyIds.length != predicates.length )
        {
            throw new IndexNotApplicableKernelException(
                    format( "The index specifies %d properties, but only %d lookup predicates were given.",
                            propertyIds.length, predicates.length ) );
        }
        for ( int i = 0; i < predicates.length; i++ )
        {
            if ( predicates[i].propertyKeyId() != propertyIds[i] )
            {
                throw new IndexNotApplicableKernelException(
                        format( "The index has the property id %d in position %d, but the lookup property id was %d.",
                                propertyIds[i], i, predicates[i].propertyKeyId() ) );
            }
        }
    }

    @Override
    public NodeCursor allocateNodeCursor()
    {
        return storageReader.allocateNodeCursor();
    }

    @Override
    public RelationshipScanCursor allocateRelationshipScanCursor()
    {
        return storageReader.allocateRelationshipScanCursor();
    }

    @Override
    public RelationshipTraversalCursor allocateRelationshipTraversalCursor()
    {
        return storageReader.allocateRelationshipTraversalCursor();
    }

    @Override
    public PropertyCursor allocatePropertyCursor()
    {
        return storageReader.allocatePropertyCursor();
    }

    @Override
    public RelationshipGroupCursor allocateRelationshipGroupCursor()
    {
        return storageReader.allocateRelationshipGroupCursor();
    }

    @Override
    public NodeValueIndexCursor allocateNodeValueIndexCursor()
    {
        return storageReader.allocateNodeValueIndexCursor();
    }

    @Override
    public NodeLabelIndexCursor allocateNodeLabelIndexCursor()
    {
        return storageReader.allocateNodeLabelIndexCursor();
    }

    @Override
    public NodeExplicitIndexCursor allocateNodeExplicitIndexCursor()
    {
        return storageReader.allocateNodeExplicitIndexCursor();
    }

    @Override
    public RelationshipExplicitIndexCursor allocateRelationshipExplicitIndexCursor()
    {
        return storageReader.allocateRelationshipExplicitIndexCursor();
    }
}
