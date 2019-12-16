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
import java.util.Objects;
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
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.IndexReaderCache;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.api.security.OverriddenAccessMode;
import org.neo4j.kernel.impl.api.security.RestrictedAccessMode;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.util.DefaultValueMapper;
import org.neo4j.lock.ResourceTypes;
import org.neo4j.storageengine.api.CountsDelta;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.StorageSchemaReader;
import org.neo4j.storageengine.api.txstate.DiffSets;
import org.neo4j.storageengine.api.txstate.TransactionCountingStateVisitor;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Value;

import static java.lang.String.format;
import static org.neo4j.internal.helpers.collection.Iterators.singleOrNull;
import static org.neo4j.kernel.api.procedure.BasicContext.buildContext;
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

        AccessMode mode = ktx.securityContext().mode();
        boolean existsInNodeStore = storageReader.nodeExists( reference );

        if ( mode.allowsTraverseAllLabels() )
        {
            return existsInNodeStore;
        }
        else if ( !existsInNodeStore )
        {
            return false;
        }
        else
        {
            try ( DefaultNodeCursor node = cursors.allocateNodeCursor() ) // DefaultNodeCursor already contains traversal checks within next()
            {
                ktx.dataRead().singleNode( reference, node );
                return node.next();
            }
        }
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
        return countsForNodeWithoutTxState( labelId ) + countsForNodeInTxState( labelId );
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
            long result;
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
                result = count;
            }
            return result - countsForNodeInTxState( labelId );
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
                    count += counts.nodeCount( labelId );
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
        return countsForRelationshipWithoutTxState( startLabelId, typeId, endLabelId ) + countsForRelationshipInTxState( startLabelId, typeId, endLabelId );
    }

    @Override
    public long countsForRelationshipWithoutTxState( int startLabelId, int typeId, int endLabelId )
    {
        AccessMode mode = ktx.securityContext().mode();
        if ( mode.allowsTraverseRelType( typeId ) &&
             mode.allowsTraverseNode( startLabelId ) &&
             mode.allowsTraverseNode( endLabelId ) )
        {
            return storageReader.countsForRelationship( startLabelId, typeId, endLabelId );
        }
        else
        {
            long count = 0;
            try ( DefaultRelationshipScanCursor rels = cursors.allocateRelationshipScanCursor();
                    DefaultNodeCursor sourceNode = cursors.allocateFullAccessNodeCursor();
                    DefaultNodeCursor targetNode = cursors.allocateFullAccessNodeCursor() )
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
            return count - countsForRelationshipInTxState( startLabelId, typeId, endLabelId );
        }
    }

    private long countsForRelationshipInTxState( int startLabelId, int typeId, int endLabelId )
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
                    count += counts.relationshipCount( startLabelId, typeId, endLabelId );
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
    public Iterator<IndexDescriptor> indexForSchemaNonTransactional( SchemaDescriptor schema )
    {
        return storageReader.indexGetForSchema( schema );
    }

    /**
     * Lock the given index if it is valid and exists.
     *
     * If the given index descriptor does not reference an index that exists, then {@link IndexDescriptor#NO_INDEX} is returned.
     *
     * @param index committed, transaction-added or even null.
     * @return The validated index descriptor, which is not necessarily the same as the one given as an argument.
     */
    private IndexDescriptor lockIndex( IndexDescriptor index )
    {
        if ( index == null )
        {
            return IndexDescriptor.NO_INDEX;
        }
        index = acquireSharedSchemaLock( index );
        // Since the schema cache gives us snapshots views of the schema, the indexes could be dropped in-between us
        // getting the snapshot, and taking the shared schema locks.
        // Thus, after we take the lock, we need to filter out indexes that no longer exists.
        if ( !indexExists( index ) )
        {
            releaseSharedSchemaLock( index );
            index = IndexDescriptor.NO_INDEX;
        }
        return index;
    }

    /**
     * Maps all index descriptors according to {@link #lockIndex(IndexDescriptor)}.
     */
    private Iterator<IndexDescriptor> lockIndexes( Iterator<IndexDescriptor> indexes )
    {
        Predicate<IndexDescriptor> exists = index -> index != IndexDescriptor.NO_INDEX;
        return Iterators.filter( exists, Iterators.map( this::lockIndex, indexes ) );
    }

    private boolean indexExists( IndexDescriptor index )
    {
        if ( ktx.hasTxStateWithChanges() )
        {
            DiffSets<IndexDescriptor> changes = ktx.txState().indexChanges();
            return changes.isAdded( index ) || (storageReader.indexExists( index ) && !changes.isRemoved( index ) );
        }
        return storageReader.indexExists( index );
    }

    public void assertIndexExists( IndexDescriptor index ) throws IndexNotFoundKernelException
    {
        if ( !indexExists( index ) )
        {
            throw new IndexNotFoundKernelException( "Index does not exist: ", index );
        }
    }

    private ConstraintDescriptor lockConstraint( ConstraintDescriptor constraint )
    {
        if ( constraint == null )
        {
            return null;
        }
        constraint = acquireSharedSchemaLock( constraint );
        if ( !constraintExists( constraint ) )
        {
            releaseSharedSchemaLock( constraint );
            constraint = null;
        }
        return constraint;
    }

    private Iterator<ConstraintDescriptor> lockConstraints( Iterator<ConstraintDescriptor> constraints )
    {
        return Iterators.filter( Objects::nonNull, Iterators.map( this::lockConstraint, constraints ) );
    }

    @Override
    public boolean constraintExists( ConstraintDescriptor constraint )
    {
        acquireSharedSchemaLock( constraint );
        ktx.assertOpen();

        if ( ktx.hasTxStateWithChanges() )
        {
            DiffSets<ConstraintDescriptor> changes = ktx.txState().constraintsChanges();
            return changes.isAdded( constraint ) || (storageReader.constraintExists( constraint ) && !changes.isRemoved( constraint ) );
        }
        return storageReader.constraintExists( constraint );
    }

    @Override
    public Iterator<IndexDescriptor> index( SchemaDescriptor schema )
    {
        ktx.assertOpen();
        return lockIndexes( indexGetForSchema( storageReader, schema ) );
    }

    Iterator<IndexDescriptor> indexGetForSchema( StorageSchemaReader reader, SchemaDescriptor schema )
    {
        Iterator<IndexDescriptor> indexes = reader.indexGetForSchema( schema );
        if ( ktx.hasTxStateWithChanges() )
        {
            DiffSets<IndexDescriptor> diffSets = ktx.txState().indexDiffSetsBySchema( schema );
            indexes = diffSets.apply( indexes );
        }

        return indexes;
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetForLabel( int labelId )
    {
        acquireSharedLock( ResourceTypes.LABEL, labelId );
        ktx.assertOpen();
        return lockIndexes( indexesGetForLabel( storageReader, labelId ) );
    }

    Iterator<IndexDescriptor> indexesGetForLabel( StorageSchemaReader reader, int labelId )
    {
        if ( ktx.securityContext().mode().allowsTraverseNode( labelId ) )
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
        return lockIndexes( indexesGetForRelationshipType( storageReader, relationshipType ) );
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
        return indexGetForName( storageReader, name );
    }

    IndexDescriptor indexGetForName( StorageSchemaReader reader, String name )
    {
        ktx.assertOpen();

        IndexDescriptor index = reader.indexGetForName( name );
        if ( ktx.hasTxStateWithChanges() )
        {
            Predicate<IndexDescriptor> namePredicate = indexDescriptor -> indexDescriptor.getName().equals( name );
            Iterator<IndexDescriptor> indexes = ktx.txState().indexChanges().filterAdded( namePredicate ).apply( Iterators.iterator( index ) );
            index = singleOrNull( indexes );
        }
        return lockIndex( index );
    }

    @Override
    public ConstraintDescriptor constraintGetForName( String name )
    {
        return constraintGetForName( storageReader, name );
    }

    ConstraintDescriptor constraintGetForName( StorageSchemaReader reader, String name )
    {
        ktx.assertOpen();

        ConstraintDescriptor constraint = reader.constraintGetForName( name );
        if ( ktx.hasTxStateWithChanges() )
        {
            Predicate<ConstraintDescriptor> namePredicate = constraintDescriptor -> constraintDescriptor.getName().equals( name );
            Iterator<ConstraintDescriptor> constraints =
                    ktx.txState().constraintsChanges().filterAdded( namePredicate ).apply( Iterators.iterator( constraint ) );
            constraint = singleOrNull( constraints );
        }
        return lockConstraint( constraint );
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetAll()
    {
        ktx.assertOpen();
        Iterator<IndexDescriptor> iterator = indexesGetAll( storageReader );
        return lockIndexes( iterator );
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
        acquireSharedSchemaLock( index );
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

        return indexingService.getIndexProxy( index ).getState();
    }

    @Override
    public PopulationProgress indexGetPopulationProgress( IndexDescriptor index )
            throws IndexNotFoundKernelException
    {
        assertValidIndex( index );
        acquireSharedSchemaLock( index );
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

        return indexingService.getIndexProxy( index ).getIndexPopulationProgress();
    }

    @Override
    public Long indexGetOwningUniquenessConstraintId( IndexDescriptor index )
    {
        acquireSharedSchemaLock( index );
        ktx.assertOpen();
        return storageReader.indexGetOwningUniquenessConstraintId( storageReader.indexGetForName( index.getName() ) );
    }

    @Override
    public String indexGetFailure( IndexDescriptor index ) throws IndexNotFoundKernelException
    {
        assertValidIndex( index );
        return indexingService.getIndexProxy( index ).getPopulationFailure().asString();
    }

    @Override
    public double indexUniqueValuesSelectivity( IndexDescriptor index ) throws IndexNotFoundKernelException
    {
        assertValidIndex( index );
        acquireSharedSchemaLock( index );
        ktx.assertOpen();
        assertIndexExists( index ); // Throws if the index has been dropped.
        final IndexSample indexSample = indexStatisticsStore.indexSample( index.getId() );
        long unique = indexSample.uniqueValues();
        long size = indexSample.sampleSize();
        return size == 0 ? 1.0d : ((double) unique) / ((double) size);
    }

    @Override
    public long indexSize( IndexDescriptor index ) throws IndexNotFoundKernelException
    {
        assertValidIndex( index );
        acquireSharedSchemaLock( index );
        ktx.assertOpen();
        return indexStatisticsStore.indexSample( index.getId() ).indexSize();
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
    public IndexSample indexSample( IndexDescriptor index ) throws IndexNotFoundKernelException
    {
        ktx.assertOpen();
        assertValidIndex( index );
        return indexStatisticsStore.indexSample( index.getId() );
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
            throw new IndexNotFoundKernelException( "Index has been dropped in this transaction: ", index );
        }
        return false;
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForSchema( SchemaDescriptor schema )
    {
        acquireSharedSchemaLock( schema );
        ktx.assertOpen();
        Iterator<ConstraintDescriptor> constraints = storageReader.constraintsGetForSchema( schema );
        if ( ktx.hasTxStateWithChanges() )
        {
            return ktx.txState().constraintsChangesForSchema( schema ).apply( constraints );
        }
        return constraints;
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
        return lockConstraints( constraints );
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
        return callProcedure( id, arguments, new RestrictedAccessMode( ktx.securityContext().mode(), AccessMode.Static.READ ), context );
    }

    @Override
    public RawIterator<AnyValue[],ProcedureException> procedureCallReadOverride( int id, AnyValue[] arguments, ProcedureCallContext context )
            throws ProcedureException
    {
        return callProcedure( id, arguments, new OverriddenAccessMode( ktx.securityContext().mode(), AccessMode.Static.READ ), context );
    }

    @Override
    public RawIterator<AnyValue[],ProcedureException> procedureCallWrite( int id, AnyValue[] arguments, ProcedureCallContext context )
            throws ProcedureException
    {
        return callProcedure( id, arguments, new RestrictedAccessMode( ktx.securityContext().mode(), AccessMode.Static.TOKEN_WRITE ), context );
    }

    @Override
    public RawIterator<AnyValue[],ProcedureException> procedureCallWriteOverride( int id, AnyValue[] arguments, ProcedureCallContext context )
            throws ProcedureException
    {
        return callProcedure( id, arguments, new OverriddenAccessMode( ktx.securityContext().mode(), AccessMode.Static.TOKEN_WRITE ), context );
    }

    @Override
    public RawIterator<AnyValue[],ProcedureException> procedureCallSchema( int id, AnyValue[] arguments, ProcedureCallContext context )
            throws ProcedureException
    {
        AccessMode accessMode = ktx.securityContext().mode();
        if ( !accessMode.allowsSchemaWrites() )
        {
            throw accessMode.onViolation( format( "Schema operations are not allowed for %s.", ktx.securityContext().description() ) );
        }
        return callProcedure( id, arguments, new RestrictedAccessMode( ktx.securityContext().mode(), AccessMode.Static.FULL ), context );
    }

    @Override
    public RawIterator<AnyValue[],ProcedureException> procedureCallSchemaOverride( int id, AnyValue[] arguments, ProcedureCallContext context )
            throws ProcedureException
    {
        return callProcedure( id, arguments, new OverriddenAccessMode( ktx.securityContext().mode(), AccessMode.Static.FULL ), context );
    }

    @Override
    public AnyValue functionCall( int id, AnyValue[] arguments ) throws ProcedureException
    {
        return callFunction( id, arguments, new RestrictedAccessMode( ktx.securityContext().mode(), AccessMode.Static.READ ) );
    }

    @Override
    public AnyValue functionCallOverride( int id, AnyValue[] arguments ) throws ProcedureException
    {
        return callFunction( id, arguments, new OverriddenAccessMode( ktx.securityContext().mode(), AccessMode.Static.READ ) );
    }

    @Override
    public UserAggregator aggregationFunction( int id ) throws ProcedureException
    {
        return aggregationFunction( id, new RestrictedAccessMode( ktx.securityContext().mode(), AccessMode.Static.READ ) );
    }

    @Override
    public UserAggregator aggregationFunctionOverride( int id ) throws ProcedureException
    {
        return aggregationFunction( id, new OverriddenAccessMode( ktx.securityContext().mode(), AccessMode.Static.READ ) );
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

    @Override
    public boolean transactionStateHasChanges()
    {
        return txState().hasChanges();
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
        final InternalTransaction internalTransaction = ktx.internalTransaction();
        return buildContext( databaseDependencies, new DefaultValueMapper( internalTransaction ) )
                .withTransaction( internalTransaction )
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
}
