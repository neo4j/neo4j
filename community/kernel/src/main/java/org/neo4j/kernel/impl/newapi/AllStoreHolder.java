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
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import org.neo4j.collection.RawIterator;
import org.neo4j.exceptions.KernelException;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.PopulationProgress;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.procs.ProcedureHandle;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.procs.UserAggregator;
import org.neo4j.internal.kernel.api.procs.UserFunctionHandle;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.index.IndexProviderDescriptor;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.labelscan.LabelScanReader;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.proc.BasicContext;
import org.neo4j.kernel.api.proc.Context;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.txstate.TransactionCountingStateVisitor;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.ClockContext;
import org.neo4j.kernel.impl.api.CountsRecordState;
import org.neo4j.kernel.impl.api.IndexReaderCache;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.SchemaState;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.security.OverriddenAccessMode;
import org.neo4j.kernel.impl.api.security.RestrictedAccessMode;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.IndexDescriptorFactory;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.StoreIndexDescriptor;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.register.Register;
import org.neo4j.storageengine.api.SchemaRule;
import org.neo4j.storageengine.api.StorageIndexReference;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.schema.ConstraintDescriptor;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.storageengine.api.schema.SchemaDescriptor;
import org.neo4j.storageengine.api.txstate.DiffSets;
import org.neo4j.values.AnyValue;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.storable.Value;

import static java.lang.String.format;
import static org.neo4j.common.TokenNameLookup.idTokenNameLookup;
import static org.neo4j.helpers.collection.Iterators.filter;
import static org.neo4j.helpers.collection.Iterators.iterator;
import static org.neo4j.helpers.collection.Iterators.singleOrNull;
import static org.neo4j.internal.kernel.api.schema.SchemaDescriptorPredicates.hasProperty;
import static org.neo4j.register.Registers.newDoubleLongRegister;
import static org.neo4j.storageengine.api.txstate.TxStateVisitor.EMPTY;

public class AllStoreHolder extends Read
{
    private final StorageReader storageReader;
    private final Procedures procedures;
    private final SchemaState schemaState;
    private final IndexingService indexingService;
    private final LabelScanStore labelScanStore;
    private final Dependencies dataSourceDependencies;
    private final IndexReaderCache indexReaderCache;
    private LabelScanReader labelScanReader;

    public AllStoreHolder( StorageReader storageReader,
                           KernelTransactionImplementation ktx,
                           DefaultPooledCursors cursors,
                           Procedures procedures,
                           SchemaState schemaState,
                           IndexingService indexingService,
                           LabelScanStore labelScanStore,
                           Dependencies dataSourceDependencies )
    {
        super( storageReader, cursors, ktx );
        this.storageReader = storageReader;
        this.procedures = procedures;
        this.schemaState = schemaState;
        this.indexReaderCache = new IndexReaderCache( indexingService );
        this.indexingService = indexingService;
        this.labelScanStore = labelScanStore;
        this.dataSourceDependencies = dataSourceDependencies;
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
    public IndexReader indexReader( IndexReference index, boolean fresh ) throws IndexNotFoundKernelException
    {
        assertValidIndex( index );
        return fresh ? indexReaderCache.newUnCachedReader( index )
                     : indexReaderCache.getOrCreate( index );
    }

    @Override
    public IndexReadSession indexReadSession( IndexReference index ) throws IndexNotFoundKernelException
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
    public IndexReference index( int label, int... properties )
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
            return IndexReference.NO_INDEX;
        }
        IndexDescriptor index = storageReader.indexGetForSchema( descriptor );
        if ( ktx.hasTxStateWithChanges() )
        {
            DiffSets<IndexDescriptor> diffSets = ktx.txState().indexDiffSetsByLabel( label );
            if ( index != null )
            {
                if ( diffSets.isRemoved( index ) )
                {
                    return IndexReference.NO_INDEX;
                }
                else
                {
                    return indexReference( index );
                }
            }
            else
            {
                Iterator<IndexDescriptor> fromTxState =
                        filter( SchemaDescriptor.equalTo( descriptor ), diffSets.getAdded().iterator() );
                if ( fromTxState.hasNext() )
                {
                    return indexReference( fromTxState.next() );
                }
                else
                {
                    return IndexReference.NO_INDEX;
                }
            }
        }

        return index != null ? indexReference( index ) : IndexReference.NO_INDEX;
    }

    /**
     * Mapping between {@link IndexDescriptor} --> {@link IndexReference}. {@link IndexDescriptor} can come from {@link StorageReader}
     * in the form of an {@link StorageIndexReference}, or as just an index added in this transaction.
     * @param index committed, transaction-added or even null.
     * @return an {@link IndexReference} for the given {@link IndexDescriptor}.
     */
    private IndexReference indexReference( IndexDescriptor index )
    {
        if ( index == null )
        {
            return null;
        }
        if ( index instanceof IndexReference )
        {
            // The lower-level descriptor actually implements IndexReference, so just use it directly
            return (IndexReference) index;
        }
        if ( index instanceof StorageIndexReference )
        {
            // This is a committed index. We can look up its reference from IndexingService
            try
            {
                return indexingService.getIndexProxy( index.schema() ).getDescriptor();
            }
            catch ( IndexNotFoundKernelException e )
            {
                // Will fall out to the exception throwing below
            }
        }
        throw new IllegalStateException( format( "Wasn't able to convert %s into an %s because it was neither already of that type nor a %s",
                index, IndexReference.class, StorageIndexReference.class ) );
    }

    @Override
    public IndexReference index( SchemaDescriptor schema )
    {
        ktx.assertOpen();

        IndexDescriptor index = storageReader.indexGetForSchema( schema );
        if ( ktx.hasTxStateWithChanges() )
        {
            DiffSets<IndexDescriptor> diffSets = ktx.txState().indexDiffSetsBySchema( schema );
            if ( index != null )
            {
                if ( diffSets.isRemoved( index ) )
                {
                    return IndexReference.NO_INDEX;
                }
                else
                {
                    return indexReference( index );
                }
            }
            else
            {
                Iterator<IndexDescriptor> fromTxState =
                        filter( SchemaDescriptor.equalTo( schema ), diffSets.getAdded().iterator() );
                if ( fromTxState.hasNext() )
                {
                    return indexReference( fromTxState.next() );
                }
                else
                {
                    return IndexReference.NO_INDEX;
                }
            }
        }

        return index != null ? indexReference( index ) : IndexReference.NO_INDEX;
    }

    @Override
    public IndexReference indexReferenceUnchecked( int label, int... properties )
    {
        return IndexDescriptorFactory.forSchema( SchemaDescriptorFactory.forLabel( label, properties ),
                                                 Optional.empty(),
                                                 IndexProviderDescriptor.UNDECIDED );
    }

    @Override
    public IndexReference indexReferenceUnchecked( SchemaDescriptor schema )
    {
        return IndexDescriptorFactory.forSchema( schema, Optional.empty(), IndexProviderDescriptor.UNDECIDED );
    }

    @Override
    public Iterator<IndexReference> indexesGetForLabel( int labelId )
    {
        acquireSharedLock( ResourceTypes.LABEL, labelId );
        ktx.assertOpen();

        Iterator<? extends IndexDescriptor> iterator = storageReader.indexesGetForLabel( labelId );
        if ( ktx.hasTxStateWithChanges() )
        {
            iterator = ktx.txState().indexDiffSetsByLabel( labelId ).apply( iterator );
        }
        //noinspection unchecked
        return (Iterator) iterator;
    }

    @Override
    public IndexReference indexGetForName( String name )
    {
        ktx.assertOpen();

        IndexDescriptor index = storageReader.indexGetForName( name );
        if ( ktx.hasTxStateWithChanges() )
        {
            Predicate<IndexDescriptor> namePredicate = indexDescriptor ->
            {
                try
                {
                    return indexDescriptor.name().equals( name );
                }
                catch ( NoSuchElementException e )
                {
                    //No name cannot match a name.
                    return false;
                }
            };
            Iterator<IndexDescriptor> indexes = ktx.txState().indexChanges().filterAdded( namePredicate ).apply( Iterators.iterator( index ) );
            index = singleOrNull( indexes );
        }
        if ( index == null )
        {
            return IndexReference.NO_INDEX;
        }
        acquireSharedSchemaLock( index.schema() );
        return indexReference( index );
    }

    @Override
    public Iterator<IndexReference> indexesGetAll()
    {
        ktx.assertOpen();

        Iterator<? extends IndexDescriptor> iterator = storageReader.indexesGetAll();
        if ( ktx.hasTxStateWithChanges() )
        {
            iterator = ktx.txState().indexChanges().apply( storageReader.indexesGetAll() );
        }

        return Iterators.map( index ->
        {
            acquireSharedSchemaLock( index.schema() );
            return indexReference( index );
        }, iterator );
    }

    @Override
    public InternalIndexState indexGetState( IndexReference index ) throws IndexNotFoundKernelException
    {
        assertValidIndex( index );
        SchemaDescriptor schema = index.schema();
        acquireSharedSchemaLock( schema );
        ktx.assertOpen();

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
    public PopulationProgress indexGetPopulationProgress( IndexReference index )
            throws IndexNotFoundKernelException
    {
        assertValidIndex( index );
        acquireSharedSchemaLock( index.schema() );
        ktx.assertOpen();

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
    public Long indexGetOwningUniquenessConstraintId( IndexReference index )
    {
        acquireSharedSchemaLock( index.schema() );
        ktx.assertOpen();
        return storageReader.indexGetOwningUniquenessConstraintId( storageReader.indexGetForSchema( index.schema() ) );
    }

    @Override
    public long indexGetCommittedId( IndexReference index ) throws SchemaRuleNotFoundException
    {
        acquireSharedSchemaLock( index.schema() );
        ktx.assertOpen();
        if ( index instanceof StoreIndexDescriptor )
        {
            return ((StoreIndexDescriptor) index).getId();
        }
        else
        {
            throw new SchemaRuleNotFoundException( SchemaRule.Kind.INDEX_RULE, index.schema() );
        }
    }

    @Override
    public String indexGetFailure( IndexReference index ) throws IndexNotFoundKernelException
    {
        assertValidIndex( index );
        return indexingService.getIndexProxy( index.schema() ).getPopulationFailure().asString();
    }

    @Override
    public double indexUniqueValuesSelectivity( IndexReference index ) throws IndexNotFoundKernelException
    {
        assertValidIndex( index );
        SchemaDescriptor schema = index.schema();
        acquireSharedSchemaLock( schema );
        ktx.assertOpen();
        return storageReader.indexUniqueValuesPercentage( schema );
    }

    @Override
    public long indexSize( IndexReference index ) throws IndexNotFoundKernelException
    {
        assertValidIndex( index );
        SchemaDescriptor schema = index.schema();
        acquireSharedSchemaLock( schema );
        ktx.assertOpen();
        return storageReader.indexSize( schema );
    }

    @Override
    public long nodesCountIndexed( IndexReference index, long nodeId, int propertyKeyId, Value value ) throws KernelException
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
    public Register.DoubleLongRegister indexUpdatesAndSize( IndexReference index, Register.DoubleLongRegister target )
            throws IndexNotFoundKernelException
    {
        ktx.assertOpen();
        assertValidIndex( index );
        return storageReader.indexUpdatesAndSize( index.schema(), target );

    }

    @Override
    public Register.DoubleLongRegister indexSample( IndexReference index, Register.DoubleLongRegister target )
            throws IndexNotFoundKernelException
    {
        ktx.assertOpen();
        assertValidIndex( index );
        return storageReader.indexSample( index.schema(), target );
    }

    IndexReference indexGetForSchema( SchemaDescriptor descriptor )
    {
        IndexDescriptor index = storageReader.indexGetForSchema( descriptor );
        Iterator<IndexDescriptor> indexes = iterator( index );
        if ( ktx.hasTxStateWithChanges() )
        {
            indexes = filter(
                    SchemaDescriptor.equalTo( descriptor ),
                    ktx.txState().indexDiffSetsBySchema( descriptor ).apply( indexes ) );
        }
        return indexReference( singleOrNull( indexes ) );
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
        return Iterators.map( this::lockConstraint, constraints );
    }

    Iterator<ConstraintDescriptor> constraintsGetForProperty( int propertyKey )
    {
        ktx.assertOpen();
        Iterator<ConstraintDescriptor> constraints = storageReader.constraintsGetAll();
        if ( ktx.hasTxStateWithChanges() )
        {
            constraints = ktx.txState().constraintsChanges().apply( constraints );
        }
        return Iterators.map( this::lockConstraint,
                              Iterators.filter( hasProperty( propertyKey ), constraints ) );
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForRelationshipType( int typeId )
    {
        acquireSharedLock( ResourceTypes.RELATIONSHIP_TYPE, typeId );
        ktx.assertOpen();
        Iterator<ConstraintDescriptor> constraints = storageReader.constraintsGetForRelationshipType( typeId );
        if ( ktx.hasTxStateWithChanges() )
        {
            return ktx.txState().constraintsChangesForRelationshipType( typeId ).apply( constraints );
        }
        return constraints;
    }

    boolean nodeExistsInStore( long id )
    {
        return storageReader.nodeExists( id );
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
        return callProcedure( id, arguments, new RestrictedAccessMode( ktx.securityContext().mode(), AccessMode.Static.READ ) );
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
                    .callProcedure( prepareContext( procedureSecurityContext ), id, input, statement );
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
                    .callProcedure( prepareContext( procedureSecurityContext ), name, input, statement );
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

        SecurityContext securityContext = ktx.securityContext().withMode( mode );
        try ( KernelTransaction.Revertable ignore = ktx.overrideWith( securityContext ) )
        {
            return procedures.callFunction( prepareContext( securityContext ), id, input );
        }
    }

    private AnyValue callFunction( QualifiedName name, AnyValue[] input, final AccessMode mode )
            throws ProcedureException
    {
        ktx.assertOpen();

        SecurityContext securityContext = ktx.securityContext().withMode( mode );
        try ( KernelTransaction.Revertable ignore = ktx.overrideWith( securityContext ) )
        {
            return procedures.callFunction( prepareContext( securityContext ), name, input );
        }
    }

    private UserAggregator aggregationFunction( int id, final AccessMode mode )
            throws ProcedureException
    {
        ktx.assertOpen();

        SecurityContext securityContext = ktx.securityContext().withMode( mode );
        try ( KernelTransaction.Revertable ignore = ktx.overrideWith( securityContext ) )
        {
            return procedures.createAggregationFunction( prepareContext( securityContext ), id );
        }
    }

    private UserAggregator aggregationFunction( QualifiedName name, final AccessMode mode )
            throws ProcedureException
    {
        ktx.assertOpen();

        SecurityContext securityContext = ktx.securityContext().withMode( mode );
        try ( KernelTransaction.Revertable ignore = ktx.overrideWith( securityContext ) )
        {
            return procedures.createAggregationFunction( prepareContext( securityContext ), name );
        }
    }

    private BasicContext prepareContext( SecurityContext securityContext )
    {
        BasicContext ctx = new BasicContext();
        ctx.put( Context.KERNEL_TRANSACTION, ktx );
        ctx.put( Context.DATABASE_API, dataSourceDependencies.resolveDependency( GraphDatabaseAPI.class ) );
        ctx.put( Context.DEPENDENCY_RESOLVER, dataSourceDependencies );
        ctx.put( Context.THREAD, Thread.currentThread() );
        ClockContext clocks = ktx.clocks();
        ctx.put( Context.SYSTEM_CLOCK, clocks.systemClock() );
        ctx.put( Context.STATEMENT_CLOCK, clocks.statementClock() );
        ctx.put( Context.TRANSACTION_CLOCK, clocks.transactionClock() );
        ctx.put( Context.SECURITY_CONTEXT, securityContext );
        return ctx;
    }

    private static void assertValidIndex( IndexReference index ) throws IndexNotFoundKernelException
    {
        if ( index == IndexReference.NO_INDEX )
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
        ktx.statementLocks().pessimistic().acquireShared( ktx.lockTracer(), schema.keyType(), schema.keyId() );
        return constraint;
    }
}
