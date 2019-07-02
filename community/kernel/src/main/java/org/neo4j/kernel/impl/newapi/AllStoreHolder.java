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
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.internal.kernel.api.procs.ProcedureHandle;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.procs.UserAggregator;
import org.neo4j.internal.kernel.api.procs.UserFunctionHandle;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor2;
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
import static org.neo4j.internal.helpers.collection.Iterators.iterator;
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
        long count = countsForNodeWithoutTxState( labelId );
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
    public long countsForNodeWithoutTxState( int labelId )
    {
        AccessMode mode = ktx.securityContext().mode();
        if ( labelId == TokenRead.ANY_LABEL && mode.allowsTraverseAllLabels() || mode.allowsTraverseLabels( labelId ) )
        {
            return storageReader.countsForNode( labelId );
        }
        else
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
    }

    @Override
    public long countsForRelationship( int startLabelId, int typeId, int endLabelId )
    {
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
                (startLabelId == TokenRead.ANY_LABEL && mode.allowsTraverseAllLabels() || mode.allowsTraverseLabels( startLabelId )) &&
                (endLabelId == TokenRead.ANY_LABEL && mode.allowsTraverseAllLabels() || mode.allowsTraverseLabels( endLabelId )) )
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
    public IndexReader indexReader( IndexDescriptor2 index, boolean fresh ) throws IndexNotFoundKernelException
    {
        assertValidIndex( index );
        return fresh ? indexReaderCache.newUnCachedReader( index )
                     : indexReaderCache.getOrCreate( index );
    }

    @Override
    public IndexReadSession indexReadSession( IndexDescriptor2 index ) throws IndexNotFoundKernelException
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
    public IndexDescriptor2 index( int label, int... properties )
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
            return IndexDescriptor2.NO_INDEX;
        }
        IndexDescriptor2 index = storageReader.indexGetForSchema( descriptor );
        if ( ktx.hasTxStateWithChanges() )
        {
            DiffSets<IndexDescriptor2> diffSets = ktx.txState().indexDiffSetsByLabel( label );
            if ( index != null )
            {
                if ( diffSets.isRemoved( index ) )
                {
                    index = null;
                }
            }
            else
            {
                Iterator<IndexDescriptor2> fromTxState =
                        filter( SchemaDescriptor.equalTo( descriptor ), diffSets.getAdded().iterator() );
                if ( fromTxState.hasNext() )
                {
                    index = fromTxState.next();
                }
            }
        }

        return indexReference( index );
    }

    /**
     * Validate that the index descriptor matches an existing index, and take its schema locks.
     * @param index committed, transaction-added or even null.
     * @return The validated index descriptor, which is not necessarily the same as the one given as an argument.
     */
    public IndexDescriptor2 indexReference( IndexDescriptor2 index )
    {
        return mapToIndexReference( index, true );
    }

    /**
     * Maps all index descriptors according to {@link #indexReference(IndexDescriptor2)}.
     */
    public Iterator<IndexDescriptor2> indexReferences( Iterator<? extends IndexDescriptor2> indexes )
    {
        return Iterators.map( index -> mapToIndexReference( index, true ), indexes );
    }

    /**
     * The same as {@link #indexReference(IndexDescriptor2)}, except no schema lock is taken.
     */
    public IndexDescriptor2 indexReferenceNoLocking( IndexDescriptor2 index )
    {
        return mapToIndexReference( index, false );
    }

    /**
     * Maps all index descriptors according to {@link #indexReferenceNoLocking(IndexDescriptor2)}.
     */
    public Iterator<IndexDescriptor2> indexReferencesNoLocking( Iterator<? extends IndexDescriptor2> indexes )
    {
        return Iterators.map( index -> mapToIndexReference( index, false ), indexes );
    }

    private IndexDescriptor2 mapToIndexReference( IndexDescriptor2 index, boolean takeSchemaLock )
    {
        if ( index == null )
        {
            // This is OK since storage may not have it and it wasn't added in this tx.
            return IndexDescriptor2.NO_INDEX;
        }
        try
        {
            if ( takeSchemaLock )
            {
                acquireSharedSchemaLock( index.schema() );
            }
            return indexingService.getIndexProxy( index.schema() ).getDescriptor();
        }
        catch ( IndexNotFoundKernelException e )
        {
            // OK we tried lookup in the indexing service, but it wasn't there. Not loaded yet?
        }
        return index;
    }

    /**
     * And then there's this method for mapping from IndexReference --> IndexDescriptor, for those places where
     * we need to go back to storage land when we have an IndexReference.
     * @param index an index reference to get IndexDescriptor for.
     * @return the IndexDescriptor for the IndexReference.
     */
    public IndexDescriptor2 storageIndexDescriptor( IndexDescriptor2 index )
    {
        // TODO we should be able to remove this method entirely, since we no longer map between index descriptor types...
        // Go and look this up by schema from storage.
        return storageReader.indexGetForSchema( index.schema() );
    }

    @Override
    public IndexDescriptor2 index( SchemaDescriptor schema )
    {
        ktx.assertOpen();
        return indexReference( indexGetForSchema( storageReader, schema ) );
    }

    IndexDescriptor2 indexGetForSchema( StorageSchemaReader reader, SchemaDescriptor schema )
    {
        IndexDescriptor2 index = reader.indexGetForSchema( schema );
        if ( ktx.hasTxStateWithChanges() )
        {
            DiffSets<IndexDescriptor2> diffSets = ktx.txState().indexDiffSetsBySchema( schema );
            if ( index != null )
            {
                if ( diffSets.isRemoved( index ) )
                {
                    index = null;
                }
            }
            else
            {
                Iterator<IndexDescriptor2> fromTxState =
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
    public Iterator<IndexDescriptor2> indexesGetForLabel( int labelId )
    {
        acquireSharedLock( ResourceTypes.LABEL, labelId );
        ktx.assertOpen();
        return Iterators.map( this::indexReference, indexesGetForLabel( storageReader, labelId ) );
    }

    Iterator<? extends IndexDescriptor2> indexesGetForLabel( StorageSchemaReader reader, int labelId )
    {
        Iterator<? extends IndexDescriptor2> iterator = reader.indexesGetForLabel( labelId );
        if ( ktx.hasTxStateWithChanges() )
        {
            iterator = ktx.txState().indexDiffSetsByLabel( labelId ).apply( iterator );
        }
        return iterator;
    }

    @Override
    public Iterator<IndexDescriptor2> indexesGetForRelationshipType( int relationshipType )
    {
        acquireSharedLock( ResourceTypes.RELATIONSHIP_TYPE, relationshipType );
        ktx.assertOpen();
        return indexReferences( indexesGetForRelationshipType( storageReader, relationshipType ) );
    }

    Iterator<? extends IndexDescriptor2> indexesGetForRelationshipType( StorageSchemaReader reader, int relationshipType )
    {
        Iterator<? extends IndexDescriptor2> iterator = reader.indexesGetForRelationshipType( relationshipType );
        if ( ktx.hasTxStateWithChanges() )
        {
            iterator = ktx.txState().indexDiffSetsByRelationshipType( relationshipType ).apply( iterator );
        }
        return iterator;
    }

    @Override
    public IndexDescriptor2 indexGetForName( String name )
    {
        ktx.assertOpen();

        IndexDescriptor2 index = storageReader.indexGetForName( name );
        if ( ktx.hasTxStateWithChanges() )
        {
            Predicate<IndexDescriptor2> namePredicate = indexDescriptor -> indexDescriptor.getName().equals( name );
            Iterator<IndexDescriptor2> indexes = ktx.txState().indexChanges().filterAdded( namePredicate ).apply( Iterators.iterator( index ) );
            index = singleOrNull( indexes );
        }
        return indexReference( index );
    }

    @Override
    public Iterator<IndexDescriptor2> indexesGetAll()
    {
        ktx.assertOpen();

        Iterator<IndexDescriptor2> iterator = indexesGetAll( storageReader );

        return Iterators.map( this::indexReference, iterator );
    }

    Iterator<IndexDescriptor2> indexesGetAll( StorageSchemaReader reader )
    {
        Iterator<IndexDescriptor2> iterator = reader.indexesGetAll();
        if ( ktx.hasTxStateWithChanges() )
        {
            iterator = ktx.txState().indexChanges().apply( iterator );
        }
        return iterator;
    }

    @Override
    public InternalIndexState indexGetState( IndexDescriptor2 index ) throws IndexNotFoundKernelException
    {
        assertValidIndex( index );
        SchemaDescriptor schema = index.schema();
        acquireSharedSchemaLock( schema );
        ktx.assertOpen();

        return indexGetStateLocked( index );
    }

    InternalIndexState indexGetStateLocked( IndexDescriptor2 index ) throws IndexNotFoundKernelException
    {
        SchemaDescriptor schema = index.schema();
        // If index is in our state, then return populating
        if ( ktx.hasTxStateWithChanges() )
        {
            if ( checkIndexState( storageIndexDescriptor( index ), ktx.txState().indexDiffSetsBySchema( schema ) ) )
            {
                return InternalIndexState.POPULATING;
            }
        }

        return indexingService.getIndexProxy( schema ).getState();
    }

    @Override
    public PopulationProgress indexGetPopulationProgress( IndexDescriptor2 index )
            throws IndexNotFoundKernelException
    {
        assertValidIndex( index );
        acquireSharedSchemaLock( index.schema() );
        ktx.assertOpen();
        return indexGetPopulationProgressLocked( index );
    }

    PopulationProgress indexGetPopulationProgressLocked( IndexDescriptor2 index ) throws IndexNotFoundKernelException
    {
        if ( ktx.hasTxStateWithChanges() )
        {
            if ( checkIndexState( storageIndexDescriptor( index ), ktx.txState().indexDiffSetsBySchema( index.schema() ) ) )
            {
                return PopulationProgress.NONE;
            }
        }

        return indexingService.getIndexProxy( index.schema() ).getIndexPopulationProgress();
    }

    @Override
    public Long indexGetOwningUniquenessConstraintId( IndexDescriptor2 index )
    {
        acquireSharedSchemaLock( index.schema() );
        ktx.assertOpen();
        return storageReader.indexGetOwningUniquenessConstraintId( storageReader.indexGetForSchema( index.schema() ) );
    }

    @Override
    public long indexGetCommittedId( IndexDescriptor2 index ) throws SchemaRuleNotFoundException
    {
        // todo can we remove this method or something?
        acquireSharedSchemaLock( index.schema() );
        ktx.assertOpen();
        if ( ktx.hasTxStateWithChanges() && ktx.txState().indexChanges().isAdded( index ) )
        {
            throw new SchemaRuleNotFoundException( index );
        }
        return index.getId();
    }

    @Override
    public String indexGetFailure( IndexDescriptor2 index ) throws IndexNotFoundKernelException
    {
        assertValidIndex( index );
        return indexingService.getIndexProxy( index.schema() ).getPopulationFailure().asString();
    }

    @Override
    public double indexUniqueValuesSelectivity( IndexDescriptor2 index ) throws IndexNotFoundKernelException
    {
        assertValidIndex( index );
        SchemaDescriptor schema = index.schema();
        acquireSharedSchemaLock( schema );
        ktx.assertOpen();
        IndexDescriptor2 storageIndex = storageReader.indexGetForSchema( index.schema() );
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
    public long indexSize( IndexDescriptor2 index ) throws IndexNotFoundKernelException
    {
        assertValidIndex( index );
        SchemaDescriptor schema = index.schema();
        acquireSharedSchemaLock( schema );
        ktx.assertOpen();
        IndexDescriptor2 storageIndex = storageReader.indexGetForSchema( index.schema() );
        if ( storageIndex == null )
        {
            throw new IndexNotFoundKernelException( "No index found for " + index.schema() );
        }
        return indexStatisticsStore.indexUpdatesAndSize( storageIndex.getId(), Registers.newDoubleLongRegister() ).readSecond();
    }

    @Override
    public long nodesCountIndexed( IndexDescriptor2 index, long nodeId, int propertyKeyId, Value value ) throws KernelException
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
    public DoubleLongRegister indexUpdatesAndSize( IndexDescriptor2 index, DoubleLongRegister target )
            throws IndexNotFoundKernelException
    {
        ktx.assertOpen();
        assertValidIndex( index );
        IndexDescriptor2 storageIndex = storageReader.indexGetForSchema( index.schema() );
        if ( storageIndex == null )
        {
            throw new IndexNotFoundKernelException( "No index found for " + index.schema() );
        }
        return indexStatisticsStore.indexUpdatesAndSize( storageIndex.getId(), target );

    }

    @Override
    public DoubleLongRegister indexSample( IndexDescriptor2 index, DoubleLongRegister target )
            throws IndexNotFoundKernelException
    {
        ktx.assertOpen();
        assertValidIndex( index );
        IndexDescriptor2 storageIndex = storageReader.indexGetForSchema( index.schema() );
        if ( storageIndex == null )
        {
            throw new IndexNotFoundKernelException( "No index found for " + index.schema() );
        }

        return indexStatisticsStore.indexSample( storageIndex.getId(), target );
    }

    IndexDescriptor2 indexGetForSchema( SchemaDescriptor descriptor )
    {
        IndexDescriptor2 index = storageReader.indexGetForSchema( descriptor );
        Iterator<IndexDescriptor2> indexes = iterator( index );
        if ( ktx.hasTxStateWithChanges() )
        {
            indexes = filter(
                    SchemaDescriptor.equalTo( descriptor ),
                    ktx.txState().indexDiffSetsBySchema( descriptor ).apply( indexes ) );
        }
        return indexReference( singleOrNull( indexes ) );
    }

    private boolean checkIndexState( IndexDescriptor2 index, DiffSets<IndexDescriptor2> diffSet )
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

    boolean nodeExistsInStore( long id )
    {
        return storageReader.nodeExists( id );
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
    public RawIterator<AnyValue[],ProcedureException> procedureCallRead( int id, AnyValue[] arguments )
            throws ProcedureException
    {
        AccessMode accessMode = ktx.securityContext().mode();
        if ( !accessMode.allowsReads() )
        {
            throw accessMode.onViolation( format( "Read operations are not allowed for %s.",
                    ktx.securityContext().description() ) );
        }
        return callProcedure( id, arguments, new RestrictedAccessMode( ktx.securityContext().mode(), AccessMode.Static.READ ) );
    }

    @Override
    public RawIterator<AnyValue[],ProcedureException> procedureCallReadOverride( int id, AnyValue[] arguments )
            throws ProcedureException
    {
        return callProcedure( id, arguments,
                new OverriddenAccessMode( ktx.securityContext().mode(), AccessMode.Static.READ ) );
    }

    @Override
    public RawIterator<AnyValue[],ProcedureException> procedureCallWrite( int id, AnyValue[] arguments )
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
    public RawIterator<AnyValue[],ProcedureException> procedureCallWriteOverride( int id, AnyValue[] arguments )
            throws ProcedureException
    {
        return callProcedure( id, arguments,
                new OverriddenAccessMode( ktx.securityContext().mode(), AccessMode.Static.TOKEN_WRITE ) );
    }

    @Override
    public RawIterator<AnyValue[],ProcedureException> procedureCallSchema( int id, AnyValue[] arguments )
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
    public RawIterator<AnyValue[],ProcedureException> procedureCallSchemaOverride( int id, AnyValue[] arguments )
            throws ProcedureException
    {
        return callProcedure( id, arguments,
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
            int id, AnyValue[] input, final AccessMode override )
            throws ProcedureException
    {
        ktx.assertOpen();

        final SecurityContext procedureSecurityContext = ktx.securityContext().withMode( override );
        final RawIterator<AnyValue[],ProcedureException> procedureCall;
        try ( KernelTransaction.Revertable ignore = ktx.overrideWith( procedureSecurityContext );
              Statement statement = ktx.acquireStatement() )
        {
            procedureCall = globalProcedures
                    .callProcedure( prepareContext( procedureSecurityContext ), id, input, statement );
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
            return globalProcedures.callFunction( prepareContext( securityContext ), id, input );
        }
    }

    private UserAggregator aggregationFunction( int id, final AccessMode mode )
            throws ProcedureException
    {
        ktx.assertOpen();

        SecurityContext securityContext = ktx.securityContext().withMode( mode );
        try ( KernelTransaction.Revertable ignore = ktx.overrideWith( securityContext ) )
        {
            return globalProcedures.createAggregationFunction( prepareContext( securityContext ), id );
        }
    }

    private Context prepareContext( SecurityContext securityContext )
    {
        return buildContext( databaseDependencies, valueMapper )
                .withKernelTransaction( ktx )
                .withSecurityContext( securityContext )
                .context();
    }

    static void assertValidIndex( IndexDescriptor2 index ) throws IndexNotFoundKernelException
    {
        if ( index == IndexDescriptor2.NO_INDEX )
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
