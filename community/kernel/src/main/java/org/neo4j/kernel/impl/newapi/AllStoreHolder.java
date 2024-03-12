/*
 * Copyright (c) "Neo4j"
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
import java.util.stream.Stream;

import org.neo4j.collection.Dependencies;
import org.neo4j.collection.RawIterator;
import org.neo4j.common.EntityType;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.IndexMonitor;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.PopulationProgress;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.SchemaReadCore;
import org.neo4j.internal.kernel.api.TokenPredicate;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.TokenReadSession;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.internal.kernel.api.procs.ProcedureHandle;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.procs.UserAggregator;
import org.neo4j.internal.kernel.api.procs.UserFunctionHandle;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.AdminAccessMode;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.api.index.TokenIndexReader;
import org.neo4j.kernel.api.index.ValueIndexReader;
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
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.CountsDelta;
import org.neo4j.storageengine.api.StorageLocks;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.StorageSchemaReader;
import org.neo4j.storageengine.api.txstate.DiffSets;
import org.neo4j.storageengine.api.txstate.TransactionCountingStateVisitor;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Value;

import static java.lang.String.format;
import static org.neo4j.function.Predicates.alwaysTrue;
import static org.neo4j.internal.helpers.collection.Iterators.singleOrNull;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;
import static org.neo4j.kernel.api.procedure.BasicContext.buildContext;
import static org.neo4j.storageengine.api.txstate.TxStateVisitor.EMPTY;

public class AllStoreHolder extends Read
{
    private final GlobalProcedures globalProcedures;
    private final SchemaState schemaState;
    private final IndexingService indexingService;
    private final IndexStatisticsStore indexStatisticsStore;
    private final Dependencies databaseDependencies;
    private final MemoryTracker memoryTracker;
    private final IndexReaderCache<ValueIndexReader> valueIndexReaderCache;
    private final IndexReaderCache<TokenIndexReader> tokenIndexReaderCache;

    public AllStoreHolder( StorageReader storageReader, KernelTransactionImplementation ktx, StorageLocks storageLocks,
            DefaultPooledCursors cursors, GlobalProcedures globalProcedures, SchemaState schemaState, IndexingService indexingService,
            IndexStatisticsStore indexStatisticsStore, Dependencies databaseDependencies, MemoryTracker memoryTracker )
    {

        super( storageReader, cursors, ktx, storageLocks );
        this.globalProcedures = globalProcedures;
        this.schemaState = schemaState;
        this.valueIndexReaderCache = new IndexReaderCache<>( index -> indexingService.getIndexProxy( index ).newValueReader() );
        this.tokenIndexReaderCache = new IndexReaderCache<>( index -> indexingService.getIndexProxy( index ).newTokenReader() );
        this.indexingService = indexingService;
        this.indexStatisticsStore = indexStatisticsStore;
        this.databaseDependencies = databaseDependencies;
        this.memoryTracker = memoryTracker;
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
        boolean existsInNodeStore = storageReader.nodeExists( reference, ktx.storeCursors() );

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
            // DefaultNodeCursor already contains traversal checks within next()
            try ( DefaultNodeCursor node = cursors.allocateNodeCursor( ktx.cursorContext() ) )
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
        if ( mode.allowsTraverseAllNodesWithLabel( labelId ) )
        {
            // All nodes with the specified label can be traversed, so the count store can be used.
            return storageReader.countsForNode( labelId, ktx.cursorContext() );
        }
        else if ( mode.disallowsTraverseLabel( labelId ) )
        {
            // No nodes with the specified label can be traversed, so the count will be 0.
            return 0;
        }
        else
        {
            // We have a restriction on what part of the graph can be traversed, that can affect nodes with the specified label.
            // This disables the count store entirely.
            // We need to calculate the counts through expensive operations.
            // We cannot use a NodeLabelScan without an expensive post-filtering, since it is not guaranteed that all nodes with the label can be traversed.
            long count = 0;
            // DefaultNodeCursor already contains traversal checks within next()
            try ( DefaultNodeCursor nodes = cursors.allocateNodeCursor( ktx.cursorContext() ) )
            {
                this.allNodesScan( nodes );
                while ( nodes.next() )
                {
                    if ( labelId == TokenRead.ANY_LABEL || nodes.hasLabel( labelId ) )
                    {
                        count++;
                    }
                }
            }
            return count - countsForNodeInTxState( labelId );
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
                CursorContext cursorContext = ktx.cursorContext();
                try ( var countingVisitor = new TransactionCountingStateVisitor( EMPTY, storageReader, txState, counts, cursorContext, ktx.storeCursors() ) )
                {
                    txState.accept( countingVisitor );
                }
                if ( counts.hasChanges() )
                {
                    count += counts.nodeCount( labelId, cursorContext );
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
        CursorContext cursorContext = ktx.cursorContext();
        if ( mode.allowsTraverseRelType( typeId ) &&
             mode.allowsTraverseNode( startLabelId ) &&
             mode.allowsTraverseNode( endLabelId ) )
        {
            return storageReader.countsForRelationship( startLabelId, typeId, endLabelId, cursorContext );
        }
        if ( mode.disallowsTraverseRelType( typeId ) ||
                  mode.disallowsTraverseLabel( startLabelId ) ||
                  mode.disallowsTraverseLabel( endLabelId ) )
        {
            // Not allowed to traverse any relationship with the specified relationship type, start node label and end node label,
            // so the count will be 0.
            return 0;
        }

        // token index scan can only scan for single relationship type
        if ( typeId != TokenRead.ANY_RELATIONSHIP_TYPE )
        {
            try
            {
                var index = findUsableTokenIndex( EntityType.RELATIONSHIP );
                if ( index != IndexDescriptor.NO_INDEX )
                {
                    long count = 0;
                    try ( DefaultRelationshipTypeIndexCursor relationshipsWithType = cursors.allocateRelationshipTypeIndexCursor( cursorContext );
                          DefaultRelationshipScanCursor relationship = cursors.allocateRelationshipScanCursor( cursorContext );
                          DefaultNodeCursor sourceNode = cursors.allocateNodeCursor( cursorContext );
                          DefaultNodeCursor targetNode = cursors.allocateNodeCursor( cursorContext ) )
                    {
                        var session = tokenReadSession( index );
                        this.relationshipTypeScan( session, relationshipsWithType, unconstrained(), new TokenPredicate( typeId ), ktx.cursorContext() );
                        while ( relationshipsWithType.next() )
                        {
                            relationshipsWithType.relationship( relationship );
                            count += countRelationshipsWithEndLabels( relationship, sourceNode, targetNode, startLabelId, endLabelId );
                        }
                    }
                    return count - countsForRelationshipInTxState( startLabelId, typeId, endLabelId );
                }
            }
            catch ( KernelException ignored )
            {
                // ignore, fallback to allRelationshipsScan
            }
        }

        long count;
        try ( DefaultRelationshipScanCursor rels = cursors.allocateRelationshipScanCursor( cursorContext );
                DefaultNodeCursor sourceNode = cursors.allocateFullAccessNodeCursor( cursorContext );
                DefaultNodeCursor targetNode = cursors.allocateFullAccessNodeCursor( cursorContext ) )
        {
            this.allRelationshipsScan( rels );
            Predicate<RelationshipScanCursor> predicate = typeId == TokenRead.ANY_RELATIONSHIP_TYPE ? alwaysTrue() : CursorPredicates.hasType( typeId );
            var filteredCursor = new FilteringRelationshipScanCursorWrapper( rels, predicate );
            count = countRelationshipsWithEndLabels( filteredCursor, sourceNode, targetNode, startLabelId, endLabelId );
        }
        return count - countsForRelationshipInTxState( startLabelId, typeId, endLabelId );
    }

    private static long countRelationshipsWithEndLabels( RelationshipScanCursor relationship, DefaultNodeCursor sourceNode, DefaultNodeCursor targetNode,
            int startLabelId, int endLabelId )
    {
        long internalCount = 0;
        while ( relationship.next() )
        {
            relationship.source( sourceNode );
            relationship.target( targetNode );
            if ( sourceNode.next() && (startLabelId == TokenRead.ANY_LABEL || sourceNode.hasLabel( startLabelId )) &&
                 targetNode.next() && (endLabelId == TokenRead.ANY_LABEL || targetNode.hasLabel( endLabelId )) )
            {
                internalCount++;
            }
        }
        return internalCount;
    }

    private long countsForRelationshipInTxState( int startLabelId, int typeId, int endLabelId )
    {
        long count = 0;
        if ( ktx.hasTxStateWithChanges() )
        {
            CursorContext cursorContext = ktx.cursorContext();
            CountsDelta counts = new CountsDelta();
            try
            {
                TransactionState txState = ktx.txState();
                try ( var countingVisitor = new TransactionCountingStateVisitor( EMPTY, storageReader, txState, counts, cursorContext, ktx.storeCursors() ) )
                {
                    txState.accept( countingVisitor );
                }
                if ( counts.hasChanges() )
                {
                    count += counts.relationshipCount( startLabelId, typeId, endLabelId, cursorContext );
                }
            }
            catch ( KernelException e )
            {
                throw new IllegalArgumentException( "Unexpected error: " + e.getMessage() );
            }
        }
        return count;
    }

    IndexDescriptor findUsableTokenIndex( EntityType entityType ) throws IndexNotFoundKernelException
    {
        var descriptor = SchemaDescriptors.forAnyEntityTokens( entityType );
        var index = index( descriptor, IndexType.LOOKUP );
        if ( index != IndexDescriptor.NO_INDEX && indexGetState( index ) == InternalIndexState.ONLINE )
        {
            return index;
        }
        return IndexDescriptor.NO_INDEX;
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
        AccessMode mode = ktx.securityContext().mode();
        CursorContext cursorContext = ktx.cursorContext();
        boolean existsInRelStore = storageReader.relationshipExists( reference, ktx.storeCursors() );

        if ( mode.allowsTraverseAllRelTypes() )
        {
            return existsInRelStore;
        }
        else if ( !existsInRelStore )
        {
            return false;
        }
        else
        {
            // DefaultNodeCursor already contains traversal checks within next()
            try ( DefaultRelationshipScanCursor rels = cursors.allocateRelationshipScanCursor( cursorContext ) )
            {
                ktx.dataRead().singleRelationship( reference, rels );
                return rels.next();
            }
        }
    }

    @Override
    public ValueIndexReader newValueIndexReader( IndexDescriptor index ) throws IndexNotFoundKernelException
    {
        assertValidIndex( index );
        return indexingService.getIndexProxy( index ).newValueReader();
    }

    @Override
    public IndexReadSession indexReadSession( IndexDescriptor index ) throws IndexNotFoundKernelException
    {
        assertValidIndex( index );
        return new DefaultIndexReadSession( valueIndexReaderCache.getOrCreate( index ), index );
    }

    @Override
    public TokenReadSession tokenReadSession( IndexDescriptor index ) throws IndexNotFoundKernelException
    {
        assertValidIndex( index );
        return new DefaultTokenReadSession( tokenIndexReaderCache.getOrCreate( index ), index );
    }

    @Override
    public Iterator<IndexDescriptor> indexForSchemaNonTransactional( SchemaDescriptor schema )
    {
        return storageReader.indexGetForSchema( schema );
    }

    @Override
    public IndexDescriptor indexForSchemaAndIndexTypeNonTransactional( SchemaDescriptor schema, IndexType indexType )
    {
        var index = storageReader.indexGetForSchemaAndType( schema, indexType );
        return index == null ? IndexDescriptor.NO_INDEX : index;
    }

    @Override
    public Iterator<IndexDescriptor> indexForSchemaNonLocking( SchemaDescriptor schema )
    {
        return indexGetForSchema( storageReader, schema );
    }

    @Override
    public Iterator<IndexDescriptor> getLabelIndexesNonLocking( int labelId )
    {
        return indexesGetForLabel( storageReader, labelId );
    }

    @Override
    public Iterator<IndexDescriptor> getRelTypeIndexesNonLocking( int relTypeId )
    {
        return indexesGetForRelationshipType( storageReader, relTypeId );
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetAllNonLocking()
    {
        return indexesGetAll( storageReader );
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
    public IndexDescriptor index( SchemaDescriptor schema, IndexType type )
    {
        ktx.assertOpen();
        return lockIndex( indexGetForSchemaAndType( storageReader, schema, type ) );
    }

    IndexDescriptor indexGetForSchemaAndType( StorageSchemaReader reader, SchemaDescriptor schema, IndexType type )
    {
        var index = reader.indexGetForSchemaAndType( schema, type );
        if ( ktx.hasTxStateWithChanges() )
        {
            var indexChanges = ktx.txState().indexChanges();
            if ( index == null )
            {
                // check if such index was added in this tx
                var added = indexChanges.filterAdded( id -> id.schema().equals( schema ) && id.getIndexType() == type ).getAdded();
                index = singleOrNull( added.iterator() );
            }

            if ( indexChanges.isRemoved( index ) )
            {
                // this index was removed in this tx
                return null;
            }
        }
        return index;
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

    @Override
    public InternalIndexState indexGetStateNonLocking( IndexDescriptor index ) throws IndexNotFoundKernelException
    {
        assertValidIndex( index );
        ktx.assertOpen();
        return indexGetStateLocked( index ); // TODO: Can we call this method without locking(since we assert valid index)?
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
        return indexGetOwningUniquenessConstraintIdNonLocking(index);
    }

    @Override
    public Long indexGetOwningUniquenessConstraintIdNonLocking( IndexDescriptor index )
    {
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
    public long nodesGetCount()
    {
        return countsForNode( TokenRead.ANY_LABEL );
    }

    @Override
    public long relationshipsGetCount()
    {
        return countsForRelationship( TokenRead.ANY_LABEL, TokenRead.ANY_RELATIONSHIP_TYPE, TokenRead.ANY_LABEL );
    }

    @Override
    public IndexSample indexSample( IndexDescriptor index ) throws IndexNotFoundKernelException
    {
        ktx.assertOpen();
        assertValidIndex( index );
        return indexStatisticsStore.indexSample( index.getId() );
    }

    private static boolean checkIndexState( IndexDescriptor index, DiffSets<IndexDescriptor> diffSet )
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
        acquireSharedSchemaLock( () -> schema );
        return getConstraintsForSchema( schema );
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForSchemaNonLocking( SchemaDescriptor schema )
    {
        return getConstraintsForSchema( schema );
    }

    private Iterator<ConstraintDescriptor> getConstraintsForSchema( SchemaDescriptor schema )
    {
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

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForLabelNonLocking( int labelId )
    {
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

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetAllNonLocking()
    {
        ktx.assertOpen();
        return constraintsGetAll( storageReader );
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

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForRelationshipTypeNonLocking( int typeId )
    {
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
    public Stream<UserFunctionSignature> functionGetAll( )
    {
        ktx.assertOpen();
        return globalProcedures.getAllNonAggregatingFunctions();
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
    public Stream<UserFunctionSignature> aggregationFunctionGetAll( )
    {
        ktx.assertOpen();
        return globalProcedures.getAllAggregatingFunctions();
    }

    @Override
    public RawIterator<AnyValue[],ProcedureException> procedureCallRead( int id, AnyValue[] arguments, ProcedureCallContext context )
            throws ProcedureException
    {
        return callProcedure( id, arguments, AccessMode.Static.READ, context );
    }

    @Override
    public RawIterator<AnyValue[],ProcedureException> procedureCallWrite( int id, AnyValue[] arguments, ProcedureCallContext context )
            throws ProcedureException
    {
        return callProcedure( id, arguments, AccessMode.Static.TOKEN_WRITE, context );
    }

    @Override
    public RawIterator<AnyValue[],ProcedureException> procedureCallSchema( int id, AnyValue[] arguments, ProcedureCallContext context )
            throws ProcedureException
    {
        return callProcedure( id, arguments, AccessMode.Static.SCHEMA, context );
    }

    @Override
    public RawIterator<AnyValue[],ProcedureException> procedureCallDbms( int id, AnyValue[] arguments, ProcedureCallContext context )
            throws ProcedureException
    {
        return callProcedure( id, arguments, AccessMode.Static.ACCESS, context );
    }

    @Override
    public AnyValue functionCall( int id, AnyValue[] arguments ) throws ProcedureException
    {
        return callFunction( id, arguments );
    }

    @Override
    public AnyValue builtInFunctionCall( int id,  AnyValue[] arguments ) throws ProcedureException
    {
        return callBuiltInFunction( id, arguments );
    }

    @Override
    public UserAggregator aggregationFunction( int id ) throws ProcedureException
    {
        return createAggregationFunction( id );
    }

    @Override
    public UserAggregator builtInAggregationFunction( int id ) throws ProcedureException
    {
        return createBuiltInAggregationFunction( id );
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
        return ktx.hasTxStateWithChanges();
    }

    private RawIterator<AnyValue[],ProcedureException> callProcedure(
            int id, AnyValue[] input, final AccessMode.Static procedureMode, ProcedureCallContext procedureCallContext )
            throws ProcedureException
    {
        ktx.assertOpen();

        AccessMode mode = ktx.securityContext().mode();
        if ( !mode.allowsExecuteProcedure( id ).allowsAccess() )
        {
            String message = format("Executing procedure is not allowed for %s.", ktx.securityContext().description() );
            throw ktx.securityAuthorizationHandler().logAndGetAuthorizationException( ktx.securityContext(), message );
        }

        final SecurityContext procedureSecurityContext = mode.shouldBoostProcedure( id ).allowsAccess() ?
                              ktx.securityContext().withMode( new OverriddenAccessMode( mode, procedureMode ) ).withMode( AdminAccessMode.FULL ) :
                              ktx.securityContext().withMode( new RestrictedAccessMode( mode, procedureMode ) );

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

    private AnyValue callFunction( int id, AnyValue[] input ) throws ProcedureException
    {
        ktx.assertOpen();

        AccessMode mode = ktx.securityContext().mode();
        if ( !mode.allowsExecuteFunction( id ).allowsAccess() )
        {
            String message = format( "Executing a user defined function is not allowed for %s.", ktx.securityContext().description() );
            throw ktx.securityAuthorizationHandler().logAndGetAuthorizationException( ktx.securityContext(), message );
        }

        final SecurityContext securityContext = mode.shouldBoostFunction( id ).allowsAccess() ?
                                                ktx.securityContext().withMode( new OverriddenAccessMode( mode, AccessMode.Static.READ ) ) :
                                                ktx.securityContext().withMode( new RestrictedAccessMode( mode, AccessMode.Static.READ ) );

        try ( KernelTransaction.Revertable ignore = ktx.overrideWith( securityContext ) )
        {
            return globalProcedures.callFunction( prepareContext( securityContext, ProcedureCallContext.EMPTY ), id, input );
        }
    }

    private AnyValue callBuiltInFunction( int id, AnyValue[] input ) throws ProcedureException
    {
        ktx.assertOpen();
        return globalProcedures.callFunction( prepareContext( ktx.securityContext(), ProcedureCallContext.EMPTY ), id, input );
    }

    private UserAggregator createAggregationFunction( int id ) throws ProcedureException
    {
        ktx.assertOpen();

        AccessMode mode = ktx.securityContext().mode();
        if ( !mode.allowsExecuteAggregatingFunction( id ).allowsAccess() )
        {
            String message = format( "Executing a user defined aggregating function is not allowed for %s.", ktx.securityContext().description() );
            throw ktx.securityAuthorizationHandler().logAndGetAuthorizationException( ktx.securityContext(), message );
        }

        final SecurityContext securityContext = mode.shouldBoostAggregatingFunction( id ).allowsAccess() ?
                                                ktx.securityContext().withMode( new OverriddenAccessMode( mode, AccessMode.Static.READ ) ) :
                                                ktx.securityContext().withMode( new RestrictedAccessMode( mode, AccessMode.Static.READ ) );

        try ( KernelTransaction.Revertable ignore = ktx.overrideWith( securityContext ) )
        {
            UserAggregator aggregator = globalProcedures.createAggregationFunction( prepareContext( securityContext, ProcedureCallContext.EMPTY ), id );
            return new UserAggregator()
            {
                @Override
                public void update( AnyValue[] input ) throws ProcedureException
                {
                    try ( KernelTransaction.Revertable ignore = ktx.overrideWith( securityContext ) )
                    {
                        aggregator.update( input );
                    }
                }

                @Override
                public AnyValue result() throws ProcedureException
                {
                    try ( KernelTransaction.Revertable ignore = ktx.overrideWith( securityContext ) )
                    {
                        return aggregator.result();
                    }
                }
            };
        }
    }

    private UserAggregator createBuiltInAggregationFunction( int id ) throws ProcedureException
    {
        ktx.assertOpen();

        UserAggregator aggregator = globalProcedures.createAggregationFunction( prepareContext( ktx.securityContext(), ProcedureCallContext.EMPTY ), id );
        return new UserAggregator()
        {
            @Override
            public void update( AnyValue[] input ) throws ProcedureException
            {
                aggregator.update( input );
            }

            @Override
            public AnyValue result() throws ProcedureException
            {
                return aggregator.result();
            }
        };
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
        valueIndexReaderCache.close();
        tokenIndexReaderCache.close();
    }

    @Override
    public CursorContext cursorContext()
    {
        return ktx.cursorContext();
    }

    @Override
    public MemoryTracker memoryTracker()
    {
        return memoryTracker;
    }

    @Override
    public IndexMonitor monitor()
    {
        return indexingService.getMonitor();
    }
}
