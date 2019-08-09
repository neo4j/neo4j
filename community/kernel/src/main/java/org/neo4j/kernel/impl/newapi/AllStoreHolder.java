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

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import org.neo4j.collection.Dependencies;
import org.neo4j.collection.RawIterator;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.index.label.LabelScanReader;
import org.neo4j.internal.index.label.LabelScanStore;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.PopulationProgress;
import org.neo4j.internal.kernel.api.SchemaReadCore;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.internal.kernel.api.procs.ProcedureHandle;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.procs.UserAggregator;
import org.neo4j.internal.kernel.api.procs.UserFunctionHandle;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.IndexReaderCache;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.api.security.OverriddenAccessMode;
import org.neo4j.kernel.impl.api.security.RestrictedAccessMode;
import org.neo4j.kernel.impl.util.DefaultValueMapper;
import org.neo4j.lock.ResourceTypes;
import org.neo4j.register.Register.DoubleLongRegister;
import org.neo4j.register.Registers;
import org.neo4j.storageengine.api.CountsDelta;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.StorageSchemaReader;
import org.neo4j.storageengine.api.txstate.DiffSets;
import org.neo4j.storageengine.api.txstate.TransactionCountingStateVisitor;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Value;

import static java.lang.String.format;
import static org.neo4j.common.TokenNameLookup.idTokenNameLookup;
import static org.neo4j.internal.helpers.collection.Iterators.filter;
import static org.neo4j.internal.helpers.collection.Iterators.singleOrNull;
import static org.neo4j.kernel.api.procedure.BasicContext.buildContext;
import static org.neo4j.register.Registers.newDoubleLongRegister;
import static org.neo4j.storageengine.api.txstate.TxStateVisitor.EMPTY;

public class AllStoreHolder extends Read
{
    private final StorageReader storageReader;
    private final GlobalProcedures globalProcedures;
    private final SchemaState schemaState;
    private final IndexingService indexingService;
    private final LabelScanStore labelScanStore;
    private final IndexStatisticsStore indexStatisticsStore;
    private final Dependencies databaseDependencies;
    private final IndexReaderCache indexReaderCache;
    private final DefaultValueMapper valueMapper;
    private LabelScanReader labelScanReader;

    public AllStoreHolder( StorageReader storageReader,
                           KernelTransactionImplementation ktx,
                           DefaultPooledCursors cursors,
                           GlobalProcedures globalProcedures,
                           SchemaState schemaState,
                           IndexingService indexingService,
                           LabelScanStore labelScanStore,
                           IndexStatisticsStore indexStatisticsStore,
                           Dependencies databaseDependencies )
    {
        super( storageReader, cursors, ktx );
        this.storageReader = storageReader;
        this.globalProcedures = globalProcedures;
        this.schemaState = schemaState;
        this.indexReaderCache = new IndexReaderCache( indexingService );
        this.indexingService = indexingService;
        this.labelScanStore = labelScanStore;
        this.indexStatisticsStore = indexStatisticsStore;
        this.databaseDependencies = databaseDependencies;
        this.valueMapper = databaseDependencies.resolveDependency( DefaultValueMapper.class );
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
    public boolean nodeDeletedInTransaction( long node )
    {
        ktx.assertOpen();
        return hasTxStateWithChanges() && txState().nodeIsDeletedInThisTx( node );
    }

    @Override
    public boolean relationshipDeletedInTransaction( long relationship )
    {
        ktx.assertOpen();
        return hasTxStateWithChanges() && txState().relationshipIsDeletedInThisTx( relationship );
    }

    @Override
    public Value nodePropertyChangeInTransactionOrNull( long node, int propertyKeyId )
    {
        ktx.assertOpen();
        return hasTxStateWithChanges() ? txState().getNodeState( node ).propertyValue( propertyKeyId ) : null;
    }

    @Override
    public Value relationshipPropertyChangeInTransactionOrNull( long relationship, int propertyKeyId )
    {
        ktx.assertOpen();
        return hasTxStateWithChanges() ? txState().getRelationshipState( relationship).propertyValue( propertyKeyId ) : null;
    }

    @Override
    public long countsForNode( int labelId )
    {
        AccessMode mode = ktx.securityContext().mode();
        if ( mode.allowsTraverseAllLabels() )
        {
            return countsForNodeWithoutTxState( labelId ) + countsForNodeInTxState( labelId );
        }
        else
        {
            return countsByAllNodeScan( labelId );
        }
    }

    @Override
    public long countsForNodeWithoutTxState( int labelId )
    {
        AccessMode mode = ktx.securityContext().mode();
        if ( mode.allowsTraverseAllLabels() )
        {
            return storageReader.countsForNode( labelId );
        }
        else
        {
            return countsByAllNodeScan( labelId ) - countsForNodeInTxState( labelId );
        }
    }

    private long countsByAllNodeScan( int labelId )
    {
        // We have a restriction on what part of the graph can be traversed. This disables the count store entirely.
        // We need to calculate the counts through expensive operations. We cannot use a NodeLabelScan because the
        // label requested might not be allowed for that node, and yet the node might be visible due to Traverse rules.
        long count = 0;
        try ( DefaultNodeCursor nodes = cursors.allocateNodeCursor() ) // DefaultNodeCursor already contains traversal checks within next()
        {
            this.allNodesScan( nodes );
            while ( nodes.next() )
            {
                if ( labelId == TokenRead.ANY_LABEL || nodes.labels().contains( labelId ) )
                {
                    count++;
                }
            }
            return count;
        }
    }

    private long countsByAllRelationshipScan( int typeId )
    {
        // We have a restriction on what part of the graph can be traversed. This disables the count store entirely.
        // We need to calculate the counts through expensive operations.
        long count = 0;
        try ( DefaultRelationshipScanCursor rels = cursors.allocateRelationshipScanCursor() )
        // DefaultRelationshipScanCursor already contains traversal checks within next()
        {
            this.allRelationshipsScan( rels );
            while ( rels.next() )
            {
                if ( typeId == TokenRead.ANY_RELATIONSHIP_TYPE || rels.type() == typeId )
                {
                    count++;
                }
            }
            return count;
        }
    }

    private long countsForNodeInTxState( int labelId )
    {
        long count = 0;
        if ( ktx.hasTxStateWithChanges() )
        {
            CountsDelta counts = new CountsDelta();
            try
            {
                TransactionState txState = ktx.txState();
                txState.accept( new TransactionCountingStateVisitor( EMPTY, storageReader, txState, counts ) );
                if ( counts.hasChanges() )
                {
                    count += counts.nodeCount( labelId, newDoubleLongRegister() ).readSecond();
                }
            }
            catch ( KernelException e )
            {
                throw new IllegalArgumentException( "Unexpected error: " + e.getMessage() );
            }
        }
        return count;
    }

    @Override
    public long countsForRelationship( int startLabelId, int typeId, int endLabelId )
    {
        AccessMode mode = ktx.securityContext().mode();
        if ( !mode.allowsTraverseAllRelTypes() )
        {
            // expensive path
            return countsByAllRelationshipScan( typeId );
        }

        long count = countsForRelationshipWithoutTxState( startLabelId, typeId, endLabelId );
        if ( ktx.hasTxStateWithChanges() )
        {
            CountsDelta counts = new CountsDelta();
            try
            {
                TransactionState txState = ktx.txState();
                txState.accept( new TransactionCountingStateVisitor( EMPTY, storageReader, txState, counts ) );
                if ( counts.hasChanges() )
                {
                    count += counts.relationshipCount( startLabelId, typeId, endLabelId, newDoubleLongRegister() ).readSecond();
                }
            }
            catch ( KernelException e )
            {
                throw new IllegalArgumentException( "Unexpected error: " + e.getMessage() );
            }
        }
        return count;
    }

    @Override
    public long countsForRelationshipWithoutTxState( int startLabelId, int typeId, int endLabelId )
    {
        AccessMode mode = ktx.securityContext().mode();
        if ( (typeId == TokenRead.ANY_RELATIONSHIP_TYPE && mode.allowsTraverseAllRelTypes() || mode.allowsTraverseRelType( typeId )) &&
                (startLabelId == TokenRead.ANY_LABEL && mode.allowsTraverseAllLabels() || mode.allowsTraverseLabel( startLabelId )) &&
                (endLabelId == TokenRead.ANY_LABEL && mode.allowsTraverseAllLabels() || mode.allowsTraverseLabel( endLabelId )) )
        {
            return storageReader.countsForRelationship( startLabelId, typeId, endLabelId );
        }
        else
        {
            long count = 0;
            try ( DefaultRelationshipScanCursor rels = cursors.allocateRelationshipScanCursor();
                    DefaultNodeCursor sourceNode = cursors.allocateNodeCursor();
                    DefaultNodeCursor targetNode = cursors.allocateNodeCursor() )
            {
                this.relationshipTypeScan( typeId, rels );
                while ( rels.next() )
                {
                    rels.source( sourceNode );
                    rels.target( targetNode );
                    if ( sourceNode.next() && (startLabelId == TokenRead.ANY_LABEL || sourceNode.labels().contains( startLabelId )) &&
                           targetNode.next() && (endLabelId == TokenRead.ANY_LABEL || targetNode.labels().contains( endLabelId )) )
                    {
                        count++;
                    }
                }
            }
            return count;
        }
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
    public IndexReader indexReader( IndexDescriptor index, boolean fresh ) throws IndexNotFoundKernelException
    {
        assertValidIndex( index );
        return fresh ? indexReaderCache.newUnCachedReader( index )
                     : indexReaderCache.getOrCreate( index );
    }

    @Override
    public IndexReadSession indexReadSession( IndexDescriptor index ) throws IndexNotFoundKernelException
    {
        assertValidIndex( index );
        return new DefaultIndexReadSession( indexReaderCache.getOrCreate( index ), index );
    }

    @Override
    public void prepareForLabelScans()
    {
        labelScanReader();
    }

    @Override
    LabelScanReader labelScanReader()
    {
        if ( labelScanReader == null )
        {
            labelScanReader = labelScanStore.newReader();
        }
        return labelScanReader;
    }

    @Override
    public IndexDescriptor index( int label, int... properties )
    {
        ktx.assertOpen();

        LabelSchemaDescriptor descriptor;
        try
        {
            descriptor = SchemaDescriptor.forLabel( label, properties );
        }
        catch ( IllegalArgumentException ignore )
        {
            // This means we have invalid label or property ids.
            return IndexDescriptor.NO_INDEX;
        }
        return index( descriptor );
    }

    @Override
    public IndexDescriptor indexForSchemaNonTransactional( SchemaDescriptor schema )
    {
        IndexDescriptor index = storageReader.indexGetForSchema( schema );
        if ( index == null )
        {
            return IndexDescriptor.NO_INDEX;
        }
        return index;
    }

    /**
     *
     * @param index committed, transaction-added or even null.
     * @return The validated index descriptor, which is not necessarily the same as the one given as an argument.
     */
    private IndexDescriptor lockedIndex( IndexDescriptor index )
    {
        if ( index == null )
        {
            return IndexDescriptor.NO_INDEX;
        }
        return acquireSharedSchemaLock( index );
    }

    /**
     * Maps all index descriptors according to {@link #lockedIndex(IndexDescriptor)}.
     */
    private Iterator<IndexDescriptor> lockedIndexes( Iterator<IndexDescriptor> indexes )
    {
        return Iterators.map( this::lockedIndex, indexes );
    }

    @Override
    public IndexDescriptor index( SchemaDescriptor schema )
    {
        ktx.assertOpen();
        return lockedIndex( indexGetForSchema( storageReader, schema ) );
    }

    IndexDescriptor indexGetForSchema( StorageSchemaReader reader, SchemaDescriptor schema )
    {
        IndexDescriptor index = reader.indexGetForSchema( schema );
        if ( ktx.hasTxStateWithChanges() )
        {
            DiffSets<IndexDescriptor> diffSets = ktx.txState().indexDiffSetsBySchema( schema );
            if ( index != null )
            {
                if ( diffSets.isRemoved( index ) )
                {
                    index = null;
                }
            }
            else
            {
                Iterator<IndexDescriptor> fromTxState =
                        filter( SchemaDescriptor.equalTo( schema ), diffSets.getAdded().iterator() );
                if ( fromTxState.hasNext() )
                {
                    index = fromTxState.next();
                }
            }
        }

        return index;
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetForLabel( int labelId )
    {
        acquireSharedLock( ResourceTypes.LABEL, labelId );
        ktx.assertOpen();
        return lockedIndexes( indexesGetForLabel( storageReader, labelId ) );
    }

    Iterator<IndexDescriptor> indexesGetForLabel( StorageSchemaReader reader, int labelId )
    {
        if ( ktx.securityContext().mode().allowsTraverseNodeLabels( labelId ) )
        {
            Iterator<IndexDescriptor> iterator = reader.indexesGetForLabel( labelId );

            if ( ktx.hasTxStateWithChanges() )
            {
                iterator = ktx.txState().indexDiffSetsByLabel( labelId ).apply( iterator );
            }

            return iterator;
        }
        else
        {
            return Collections.emptyIterator();
        }
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetForRelationshipType( int relationshipType )
    {
        acquireSharedLock( ResourceTypes.RELATIONSHIP_TYPE, relationshipType );
        ktx.assertOpen();
        return lockedIndexes( indexesGetForRelationshipType( storageReader, relationshipType ) );
    }

    Iterator<IndexDescriptor> indexesGetForRelationshipType( StorageSchemaReader reader, int relationshipType )
    {
        Iterator<IndexDescriptor> iterator = reader.indexesGetForRelationshipType( relationshipType );
        if ( ktx.hasTxStateWithChanges() )
        {
            iterator = ktx.txState().indexDiffSetsByRelationshipType( relationshipType ).apply( iterator );
        }
        return iterator;
    }

    @Override
    public IndexDescriptor indexGetForName( String name )
    {
        ktx.assertOpen();

        IndexDescriptor index = storageReader.indexGetForName( name );
        if ( ktx.hasTxStateWithChanges() )
        {
            Predicate<IndexDescriptor> namePredicate = indexDescriptor -> indexDescriptor.getName().equals( name );
            Iterator<IndexDescriptor> indexes = ktx.txState().indexChanges().filterAdded( namePredicate ).apply( Iterators.iterator( index ) );
            index = singleOrNull( indexes );
        }
        return lockedIndex( index );
    }

    @Override
    public ConstraintDescriptor constraintGetForName( String name )
    {
        ktx.assertOpen();

        ConstraintDescriptor constraint = storageReader.constraintGetForName( name );
        if ( ktx.hasTxStateWithChanges() )
        {
            Predicate<ConstraintDescriptor> namePredicate = constraintDescriptor -> constraintDescriptor.getName().equals( name );
            Iterator<ConstraintDescriptor> constraints =
                    ktx.txState().constraintsChanges().filterAdded( namePredicate ).apply( Iterators.iterator( constraint ) );
            constraint = singleOrNull( constraints );
        }
        if ( constraint == null )
        {
            return null;
        }
        return acquireSharedSchemaLock( constraint );
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetAll()
    {
        ktx.assertOpen();
        Iterator<IndexDescriptor> iterator = indexesGetAll( storageReader );
        return lockedIndexes( iterator );
    }

    Iterator<IndexDescriptor> indexesGetAll( StorageSchemaReader reader )
    {
        Iterator<IndexDescriptor> iterator = reader.indexesGetAll();
        if ( ktx.hasTxStateWithChanges() )
        {
            iterator = ktx.txState().indexChanges().apply( iterator );
        }
        return iterator;
    }

    @Override
    public InternalIndexState indexGetState( IndexDescriptor index ) throws IndexNotFoundKernelException
    {
        assertValidIndex( index );
        SchemaDescriptor schema = index.schema();
        acquireSharedSchemaLock( schema );
        ktx.assertOpen();

        return indexGetStateLocked( index );
    }

    InternalIndexState indexGetStateLocked( IndexDescriptor index ) throws IndexNotFoundKernelException
    {
        SchemaDescriptor schema = index.schema();
        // If index is in our state, then return populating
        if ( ktx.hasTxStateWithChanges() )
        {
            if ( checkIndexState( index, ktx.txState().indexDiffSetsBySchema( schema ) ) )
            {
                return InternalIndexState.POPULATING;
            }
        }

        return indexingService.getIndexProxy( schema ).getState();
    }

    @Override
    public PopulationProgress indexGetPopulationProgress( IndexDescriptor index )
            throws IndexNotFoundKernelException
    {
        assertValidIndex( index );
        acquireSharedSchemaLock( index.schema() );
        ktx.assertOpen();
        return indexGetPopulationProgressLocked( index );
    }

    PopulationProgress indexGetPopulationProgressLocked( IndexDescriptor index ) throws IndexNotFoundKernelException
    {
        if ( ktx.hasTxStateWithChanges() )
        {
            if ( checkIndexState( index, ktx.txState().indexDiffSetsBySchema( index.schema() ) ) )
            {
                return PopulationProgress.NONE;
            }
        }

        return indexingService.getIndexProxy( index.schema() ).getIndexPopulationProgress();
    }

    @Override
    public Long indexGetOwningUniquenessConstraintId( IndexDescriptor index )
    {
        acquireSharedSchemaLock( index.schema() );
        ktx.assertOpen();
        return storageReader.indexGetOwningUniquenessConstraintId( storageReader.indexGetForSchema( index.schema() ) );
    }

    @Override
    public String indexGetFailure( IndexDescriptor index ) throws IndexNotFoundKernelException
    {
        assertValidIndex( index );
        return indexingService.getIndexProxy( index.schema() ).getPopulationFailure().asString();
    }

    @Override
    public double indexUniqueValuesSelectivity( IndexDescriptor index ) throws IndexNotFoundKernelException
    {
        assertValidIndex( index );
        SchemaDescriptor schema = index.schema();
        acquireSharedSchemaLock( schema );
        ktx.assertOpen();
        IndexDescriptor storageIndex = storageReader.indexGetForSchema( index.schema() );
        if ( storageIndex == null )
        {
            throw new IndexNotFoundKernelException( "No index found for " + index.schema() );
        }
        DoubleLongRegister output = indexStatisticsStore.indexSample( storageIndex.getId(), Registers.newDoubleLongRegister() );
        long unique = output.readFirst();
        long size = output.readSecond();
        return size == 0 ? 1.0d : ((double) unique) / ((double) size);
    }

    @Override
    public long indexSize( IndexDescriptor index ) throws IndexNotFoundKernelException
    {
        assertValidIndex( index );
        SchemaDescriptor schema = index.schema();
        acquireSharedSchemaLock( schema );
        ktx.assertOpen();
        IndexDescriptor storageIndex = storageReader.indexGetForSchema( index.schema() );
        if ( storageIndex == null )
        {
            throw new IndexNotFoundKernelException( "No index found for " + index.schema() );
        }
        return indexStatisticsStore.indexUpdatesAndSize( storageIndex.getId(), Registers.newDoubleLongRegister() ).readSecond();
    }

    @Override
    public long nodesCountIndexed( IndexDescriptor index, long nodeId, int propertyKeyId, Value value ) throws KernelException
    {
        ktx.assertOpen();
        assertValidIndex( index );
        IndexReader reader = indexReaderCache.getOrCreate( index );
        return reader.countIndexedNodes( nodeId, new int[] {propertyKeyId}, value );
    }

    @Override
    public long nodesGetCount( )
    {
        ktx.assertOpen();
        long base = storageReader.nodesGetCount();
        return ktx.hasTxStateWithChanges() ? base + ktx.txState().addedAndRemovedNodes().delta() : base;
    }

    @Override
    public long relationshipsGetCount( )
    {
        ktx.assertOpen();
        long base = storageReader.relationshipsGetCount();
        return ktx.hasTxStateWithChanges() ? base + ktx.txState().addedAndRemovedRelationships().delta() : base;
    }

    @Override
    public DoubleLongRegister indexUpdatesAndSize( IndexDescriptor index, DoubleLongRegister target )
            throws IndexNotFoundKernelException
    {
        ktx.assertOpen();
        assertValidIndex( index );
        IndexDescriptor storageIndex = storageReader.indexGetForSchema( index.schema() );
        if ( storageIndex == null )
        {
            throw new IndexNotFoundKernelException( "No index found for " + index.schema() );
        }
        return indexStatisticsStore.indexUpdatesAndSize( storageIndex.getId(), target );

    }

    @Override
    public DoubleLongRegister indexSample( IndexDescriptor index, DoubleLongRegister target )
            throws IndexNotFoundKernelException
    {
        ktx.assertOpen();
        assertValidIndex( index );
        IndexDescriptor storageIndex = storageReader.indexGetForSchema( index.schema() );
        if ( storageIndex == null )
        {
            throw new IndexNotFoundKernelException( "No index found for " + index.schema() );
        }

        return indexStatisticsStore.indexSample( storageIndex.getId(), target );
    }

    private boolean checkIndexState( IndexDescriptor index, DiffSets<IndexDescriptor> diffSet )
            throws IndexNotFoundKernelException
    {
        if ( diffSet.isAdded( index ) )
        {
            return true;
        }
        if ( diffSet.isRemoved( index ) )
        {
            throw new IndexNotFoundKernelException( format( "Index on %s has been dropped in this transaction.",
                    index.userDescription( idTokenNameLookup ) ) );
        }
        return false;
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForSchema( SchemaDescriptor descriptor )
    {
        acquireSharedSchemaLock( descriptor );
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
        acquireSharedSchemaLock( schema );
        ktx.assertOpen();
        boolean inStore = storageReader.constraintExists( descriptor );
        if ( ktx.hasTxStateWithChanges() )
        {
            DiffSets<ConstraintDescriptor> diffSet =
                    ktx.txState().constraintsChangesForSchema( descriptor.schema() );
            return diffSet.isAdded( descriptor ) || (inStore && !diffSet.isRemoved( descriptor ));
        }

        return inStore;
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForLabel( int labelId )
    {
        acquireSharedLock( ResourceTypes.LABEL, labelId );
        ktx.assertOpen();
        return constraintsGetForLabel( storageReader, labelId );
    }

    Iterator<ConstraintDescriptor> constraintsGetForLabel( StorageSchemaReader reader, int labelId )
    {
        Iterator<ConstraintDescriptor> constraints = reader.constraintsGetForLabel( labelId );
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
        Iterator<ConstraintDescriptor> constraints = constraintsGetAll( storageReader );
        return Iterators.map( this::lockConstraint, constraints );
    }

    Iterator<ConstraintDescriptor> constraintsGetAll( StorageSchemaReader reader )
    {
        Iterator<ConstraintDescriptor> constraints = reader.constraintsGetAll();
        if ( ktx.hasTxStateWithChanges() )
        {
            constraints = ktx.txState().constraintsChanges().apply( constraints );
        }
        return constraints;
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForRelationshipType( int typeId )
    {
        acquireSharedLock( ResourceTypes.RELATIONSHIP_TYPE, typeId );
        ktx.assertOpen();
        return constraintsGetForRelationshipType( storageReader, typeId );
    }

    Iterator<ConstraintDescriptor> constraintsGetForRelationshipType( StorageSchemaReader reader, int typeId )
    {
        Iterator<ConstraintDescriptor> constraints = reader.constraintsGetForRelationshipType( typeId );
        if ( ktx.hasTxStateWithChanges() )
        {
            return ktx.txState().constraintsChangesForRelationshipType( typeId ).apply( constraints );
        }
        return constraints;
    }

    @Override
    public SchemaReadCore snapshot()
    {
        ktx.assertOpen();
        StorageSchemaReader snapshot = storageReader.schemaSnapshot();
        return new SchemaReadCoreSnapshot( snapshot, ktx, this );
    }

    @Override
    public UserFunctionHandle functionGet( QualifiedName name )
    {
        ktx.assertOpen();
        return globalProcedures.function( name );
    }

    @Override
    public ProcedureHandle procedureGet( QualifiedName name ) throws ProcedureException
    {
        ktx.assertOpen();
        return globalProcedures.procedure( name );
    }

    @Override
    public Set<ProcedureSignature> proceduresGetAll( )
    {
        ktx.assertOpen();
        return globalProcedures.getAllProcedures();
    }

    @Override
    public UserFunctionHandle aggregationFunctionGet( QualifiedName name )
    {
        ktx.assertOpen();
        return globalProcedures.aggregationFunction( name );
    }

    @Override
    public RawIterator<AnyValue[],ProcedureException> procedureCallRead( int id, AnyValue[] arguments, ProcedureCallContext context )
            throws ProcedureException
    {
        AccessMode accessMode = ktx.securityContext().mode();
        if ( !accessMode.allowsReads() )
        {
            throw accessMode.onViolation( format( "Read operations are not allowed for %s.",
                    ktx.securityContext().description() ) );
        }
        return callProcedure( id, arguments, new RestrictedAccessMode( ktx.securityContext().mode(), AccessMode.Static.READ ), context );
    }

    @Override
    public RawIterator<AnyValue[],ProcedureException> procedureCallReadOverride( int id, AnyValue[] arguments, ProcedureCallContext context )
            throws ProcedureException
    {
        return callProcedure( id, arguments,
                new OverriddenAccessMode( ktx.securityContext().mode(), AccessMode.Static.READ ), context );
    }

    @Override
    public RawIterator<AnyValue[],ProcedureException> procedureCallWrite( int id, AnyValue[] arguments, ProcedureCallContext context )
            throws ProcedureException
    {
        AccessMode accessMode = ktx.securityContext().mode();
        if ( !accessMode.allowsWrites() )
        {
            throw accessMode.onViolation( format( "Write operations are not allowed for %s.",
                    ktx.securityContext().description() ) );
        }
        return callProcedure( id, arguments,
                new RestrictedAccessMode( ktx.securityContext().mode(), AccessMode.Static.TOKEN_WRITE ), context );
    }

    @Override
    public RawIterator<AnyValue[],ProcedureException> procedureCallWriteOverride( int id, AnyValue[] arguments, ProcedureCallContext context )
            throws ProcedureException
    {
        return callProcedure( id, arguments,
                new OverriddenAccessMode( ktx.securityContext().mode(), AccessMode.Static.TOKEN_WRITE ), context );
    }

    @Override
    public RawIterator<AnyValue[],ProcedureException> procedureCallSchema( int id, AnyValue[] arguments, ProcedureCallContext context )
            throws ProcedureException
    {
        AccessMode accessMode = ktx.securityContext().mode();
        if ( !accessMode.allowsSchemaWrites() )
        {
            throw accessMode.onViolation( format( "Schema operations are not allowed for %s.",
                    ktx.securityContext().description() ) );
        }
        return callProcedure( id, arguments,
                new RestrictedAccessMode( ktx.securityContext().mode(), AccessMode.Static.FULL ), context );
    }

    @Override
    public RawIterator<AnyValue[],ProcedureException> procedureCallSchemaOverride( int id, AnyValue[] arguments, ProcedureCallContext context )
            throws ProcedureException
    {
        return callProcedure( id, arguments,
                new OverriddenAccessMode( ktx.securityContext().mode(), AccessMode.Static.FULL ), context );
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
    public AnyValue functionCallOverride( int id, AnyValue[] arguments ) throws ProcedureException
    {
        return callFunction( id, arguments,
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
    public UserAggregator aggregationFunctionOverride( int id ) throws ProcedureException
    {
        return aggregationFunction( id,
                new OverriddenAccessMode( ktx.securityContext().mode(), AccessMode.Static.READ ) );
    }

    @Override
    public <K, V> V schemaStateGetOrCreate( K key, Function<K,V> creator )
    {
        return schemaState.getOrCreate( key, creator );
    }

    @Override
    public void schemaStateFlush()
    {
        schemaState.clear();
    }

    private RawIterator<AnyValue[],ProcedureException> callProcedure(
            int id, AnyValue[] input, final AccessMode override, ProcedureCallContext procedureCallContext )
            throws ProcedureException
    {
        ktx.assertOpen();

        final SecurityContext procedureSecurityContext = ktx.securityContext().withMode( override );
        final RawIterator<AnyValue[],ProcedureException> procedureCall;
        try ( KernelTransaction.Revertable ignore = ktx.overrideWith( procedureSecurityContext );
              Statement statement = ktx.acquireStatement() )
        {
            procedureCall = globalProcedures
                    .callProcedure( prepareContext( procedureSecurityContext, procedureCallContext ), id, input, statement );
        }
        return createIterator( procedureSecurityContext, procedureCall );
    }

    private RawIterator<AnyValue[],ProcedureException> createIterator( SecurityContext procedureSecurityContext,
            RawIterator<AnyValue[],ProcedureException> procedureCall )
    {
        return new RawIterator<>()
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
            public AnyValue[] next() throws ProcedureException
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

        SecurityContext securityContext = ktx.securityContext().withMode( mode );
        try ( KernelTransaction.Revertable ignore = ktx.overrideWith( securityContext ) )
        {
            return globalProcedures.callFunction( prepareContext( securityContext, ProcedureCallContext.EMPTY ), id, input );
        }
    }

    private UserAggregator aggregationFunction( int id, final AccessMode mode )
            throws ProcedureException
    {
        ktx.assertOpen();

        SecurityContext securityContext = ktx.securityContext().withMode( mode );
        try ( KernelTransaction.Revertable ignore = ktx.overrideWith( securityContext ) )
        {
            return globalProcedures.createAggregationFunction( prepareContext( securityContext, ProcedureCallContext.EMPTY ), id );
        }
    }

    private Context prepareContext( SecurityContext securityContext, ProcedureCallContext procedureContext )
    {
        return buildContext( databaseDependencies, valueMapper )
                .withKernelTransaction( ktx )
                .withSecurityContext( securityContext )
                .withProcedureCallContext( procedureContext )
                .context();
    }

    static void assertValidIndex( IndexDescriptor index ) throws IndexNotFoundKernelException
    {
        if ( index == IndexDescriptor.NO_INDEX )
        {
            throw new IndexNotFoundKernelException( "No index was found" );
        }
    }

    public void release()
    {
        indexReaderCache.close();
    }

    private ConstraintDescriptor lockConstraint( ConstraintDescriptor constraint )
    {
        SchemaDescriptor schema = constraint.schema();
        ktx.statementLocks().pessimistic().acquireShared( ktx.lockTracer(), schema.keyType(), schema.lockingKeys() );
        return constraint;
    }
}
