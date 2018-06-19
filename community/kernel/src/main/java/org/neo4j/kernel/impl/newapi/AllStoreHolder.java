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
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import org.neo4j.collection.RawIterator;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.InternalIndexState;
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
import org.neo4j.kernel.api.ExplicitIndex;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.proc.BasicContext;
import org.neo4j.kernel.api.proc.Context;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema.index.CapableIndexDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
import org.neo4j.kernel.api.schema.index.StoreIndexDescriptor;
import org.neo4j.kernel.api.txstate.TransactionCountingStateVisitor;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.ClockContext;
import org.neo4j.kernel.impl.api.CountsRecordState;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.SchemaState;
import org.neo4j.kernel.impl.api.security.OverriddenAccessMode;
import org.neo4j.kernel.impl.api.security.RestrictedAccessMode;
import org.neo4j.kernel.impl.index.ExplicitIndexStore;
import org.neo4j.kernel.impl.index.IndexEntityType;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.register.Register;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.LabelScanReader;
import org.neo4j.storageengine.api.schema.PopulationProgress;
import org.neo4j.storageengine.api.schema.SchemaRule;
import org.neo4j.storageengine.api.txstate.ReadableDiffSets;
import org.neo4j.values.AnyValue;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.storable.Value;

import static java.lang.String.format;
import static org.neo4j.helpers.collection.Iterators.filter;
import static org.neo4j.helpers.collection.Iterators.iterator;
import static org.neo4j.helpers.collection.Iterators.singleOrNull;
import static org.neo4j.register.Registers.newDoubleLongRegister;
import static org.neo4j.storageengine.api.txstate.TxStateVisitor.EMPTY;

public class AllStoreHolder extends Read
{
    private final StorageReader storageReader;
    private final ExplicitIndexStore explicitIndexStore;
    private final Procedures procedures;
    private final SchemaState schemaState;
    private final Dependencies dataSourceDependencies;

    public AllStoreHolder( StorageReader storageReader, KernelTransactionImplementation ktx, DefaultCursors cursors, ExplicitIndexStore explicitIndexStore,
            Procedures procedures, SchemaState schemaState, Dependencies dataSourceDependencies )
    {
        super( cursors, ktx );
        this.storageReader = storageReader;
        this.explicitIndexStore = explicitIndexStore;
        this.procedures = procedures;
        this.schemaState = schemaState;
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
    long graphPropertiesReference()
    {
        return storageReader.getGraphPropertyReference();
    }

    @Override
    IndexReader indexReader( IndexReference index, boolean fresh ) throws IndexNotFoundKernelException
    {
        return fresh ? storageReader.getFreshIndexReader( (IndexDescriptor) index ) :
               storageReader.getIndexReader( (IndexDescriptor) index );
    }

    @Override
    LabelScanReader labelScanReader()
    {
        return storageReader.getLabelScanReader();
    }

    @Override
    ExplicitIndex explicitNodeIndex( String indexName ) throws ExplicitIndexNotFoundKernelException
    {
        ktx.assertOpen();
        return explicitIndexTxState().nodeChanges( indexName );
    }

    @Override
    ExplicitIndex explicitRelationshipIndex( String indexName ) throws ExplicitIndexNotFoundKernelException
    {
        ktx.assertOpen();
        return explicitIndexTxState().relationshipChanges( indexName );
    }

    @Override
    public String[] nodeExplicitIndexesGetAll()
    {
        ktx.assertOpen();
        return explicitIndexStore.getAllNodeIndexNames();
    }

    @Override
    public boolean nodeExplicitIndexExists( String indexName, Map<String,String> customConfiguration )
    {
        ktx.assertOpen();
        return explicitIndexTxState().checkIndexExistence( IndexEntityType.Node, indexName, customConfiguration  );
    }

    @Override
    public Map<String,String> nodeExplicitIndexGetConfiguration( String indexName )
            throws ExplicitIndexNotFoundKernelException
    {
        ktx.assertOpen();
        return explicitIndexStore.getNodeIndexConfiguration( indexName );
    }

    @Override
    public String[] relationshipExplicitIndexesGetAll()
    {
        ktx.assertOpen();
        return explicitIndexStore.getAllRelationshipIndexNames();
    }

    @Override
    public boolean relationshipExplicitIndexExists( String indexName, Map<String,String> customConfiguration )
    {
        ktx.assertOpen();
        return explicitIndexTxState().checkIndexExistence( IndexEntityType.Relationship, indexName, customConfiguration  );
    }

    @Override
    public Map<String,String> relationshipExplicitIndexGetConfiguration( String indexName )
            throws ExplicitIndexNotFoundKernelException
    {
        ktx.assertOpen();
        return explicitIndexStore.getRelationshipIndexConfiguration( indexName );
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
        CapableIndexDescriptor indexDescriptor = storageReader.indexGetForSchema( descriptor );
        if ( ktx.hasTxStateWithChanges() )
        {
            ReadableDiffSets<IndexDescriptor> diffSets = ktx.txState().indexDiffSetsByLabel( label );
            if ( indexDescriptor != null )
            {
                if ( diffSets.isRemoved( indexDescriptor ) )
                {
                    return IndexReference.NO_INDEX;
                }
                else
                {
                    return indexDescriptor;
                }
            }
            else
            {
                Iterator<IndexDescriptor> fromTxState =
                        filter( SchemaDescriptor.equalTo( descriptor ), diffSets.getAdded().iterator() );
                if ( fromTxState.hasNext() )
                {
                    return fromTxState.next();
                }
                else
                {
                    return IndexReference.NO_INDEX;
                }
            }
        }

        return indexDescriptor != null ? indexDescriptor : IndexReference.NO_INDEX;
    }

    @Override
    public IndexReference index( SchemaDescriptor schema )
    {
        ktx.assertOpen();

        CapableIndexDescriptor indexDescriptor = storageReader.indexGetForSchema( schema );
        if ( ktx.hasTxStateWithChanges() )
        {
            ReadableDiffSets<IndexDescriptor> diffSets = ktx.txState().indexDiffSetsBySchema( schema );
            if ( indexDescriptor != null )
            {
                if ( diffSets.isRemoved( indexDescriptor ) )
                {
                    return IndexReference.NO_INDEX;
                }
                else
                {
                    return indexDescriptor;
                }
            }
            else
            {
                Iterator<IndexDescriptor> fromTxState =
                        filter( SchemaDescriptor.equalTo( schema ), diffSets.getAdded().iterator() );
                if ( fromTxState.hasNext() )
                {
                    return fromTxState.next();
                }
                else
                {
                    return IndexReference.NO_INDEX;
                }
            }
        }

        return indexDescriptor != null ? indexDescriptor : IndexReference.NO_INDEX;
    }

    @Override
    public IndexReference indexReferenceUnchecked( int label, int... properties )
    {
        return IndexDescriptorFactory.forSchema( SchemaDescriptorFactory.forLabel( label, properties ),
                                                 Optional.empty(),
                                                 IndexProvider.UNDECIDED );
    }

    @Override
    public Iterator<IndexReference> indexesGetForLabel( int labelId )
    {
        sharedOptimisticLock( ResourceTypes.LABEL, labelId );
        ktx.assertOpen();

        Iterator<? extends IndexDescriptor> iterator = storageReader.indexesGetForLabel( labelId );
        if ( ktx.hasTxStateWithChanges() )
        {
            iterator = ktx.txState().indexDiffSetsByLabel( labelId ).apply( iterator );
        }
        return (Iterator)iterator;
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

        return Iterators.map( indexDescriptor ->
        {
            sharedOptimisticLock( indexDescriptor.schema().keyType(), indexDescriptor.schema().keyId() );
            return indexDescriptor;
        }, iterator );
    }

    @Override
    public InternalIndexState indexGetState( IndexReference index ) throws IndexNotFoundKernelException
    {
        assertValidIndex( index );
        IndexDescriptor descriptor = (IndexDescriptor) index;
        sharedOptimisticLock( descriptor.schema().keyType(), descriptor.schema().keyId() );
        ktx.assertOpen();
        return indexGetState( descriptor );
    }

    @Override
    public PopulationProgress indexGetPopulationProgress( IndexReference index )
            throws IndexNotFoundKernelException
    {
        IndexDescriptor descriptor = (IndexDescriptor) index;
        sharedOptimisticLock( descriptor.schema().keyType(), descriptor.schema().keyId() );
        ktx.assertOpen();

        if ( ktx.hasTxStateWithChanges() )
        {
            if ( checkIndexState( descriptor,
                    ktx.txState().indexDiffSetsBySchema( descriptor.schema() ) ) )
            {
                return PopulationProgress.NONE;
            }
        }

        return storageReader.indexGetPopulationProgress( descriptor.schema() );
    }

    @Override
    public Long indexGetOwningUniquenessConstraintId( IndexReference index )
    {
        IndexDescriptor descriptor = (IndexDescriptor) index;
        sharedOptimisticLock( descriptor.schema().keyType(), descriptor.schema().keyId() );
        ktx.assertOpen();
        if ( descriptor instanceof StoreIndexDescriptor )
        {
            return ((StoreIndexDescriptor) descriptor).getOwningConstraint();
        }
        else
        {
            return null;
        }
    }

    @Override
    public long indexGetCommittedId( IndexReference index ) throws SchemaRuleNotFoundException
    {
        IndexDescriptor descriptor = (IndexDescriptor) index;
        sharedOptimisticLock( descriptor.schema().keyType(), descriptor.schema().keyId() );
        ktx.assertOpen();
        if ( descriptor instanceof StoreIndexDescriptor )
        {
            return ((StoreIndexDescriptor) descriptor).getId();
        }
        else
        {
            throw new SchemaRuleNotFoundException( SchemaRule.Kind.INDEX_RULE, ((IndexDescriptor)index).schema() );
        }
    }

    @Override
    public String indexGetFailure( IndexReference index ) throws IndexNotFoundKernelException
    {
        assertValidIndex( index );
        IndexDescriptor descriptor = (IndexDescriptor) index;
        return storageReader.indexGetFailure( descriptor.schema() );
    }

    @Override
    public double indexUniqueValuesSelectivity( IndexReference index ) throws IndexNotFoundKernelException
    {
        assertValidIndex( index );
        SchemaDescriptor schema = ((IndexDescriptor) index).schema();
        sharedOptimisticLock( schema.keyType(), schema.keyId() );
        ktx.assertOpen();
        return storageReader.indexUniqueValuesPercentage( schema );
    }

    @Override
    public long indexSize( IndexReference index ) throws IndexNotFoundKernelException
    {
        assertValidIndex( index );
        SchemaDescriptor schema = ((IndexDescriptor) index).schema();
        sharedOptimisticLock( schema.keyType(), schema.keyId() );
        ktx.assertOpen();
        return storageReader.indexSize( schema );
    }

    @Override
    public long nodesCountIndexed( IndexReference index, long nodeId, Value value ) throws KernelException
    {
        ktx.assertOpen();
        IndexReader reader = storageReader.getIndexReader( (IndexDescriptor) index );
        return reader.countIndexedNodes( nodeId, value );
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

    IndexReference indexGetCapability( IndexDescriptor schemaIndexDescriptor )
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

    InternalIndexState indexGetState( IndexDescriptor descriptor ) throws IndexNotFoundKernelException
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

    Long indexGetOwningUniquenessConstraintId( IndexDescriptor index )
    {
        return storageReader.indexGetOwningUniquenessConstraintId( index );
    }

    IndexDescriptor indexGetForSchema( SchemaDescriptor descriptor )
    {
        IndexDescriptor indexDescriptor = storageReader.indexGetForSchema( descriptor );
        Iterator<IndexDescriptor> indexes = iterator( indexDescriptor );
        if ( ktx.hasTxStateWithChanges() )
        {
            indexes = filter(
                    SchemaDescriptor.equalTo( descriptor ),
                    ktx.txState().indexDiffSetsByLabel( descriptor.keyId() ).apply( indexes ) );
        }
        return singleOrNull( indexes );
    }

    private boolean checkIndexState( IndexDescriptor index, ReadableDiffSets<IndexDescriptor> diffSet )
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
    public Iterator<ConstraintDescriptor> constraintsGetForSchema( SchemaDescriptor descriptor )
    {
        sharedOptimisticLock( descriptor.keyType(), descriptor.keyId() );
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
        sharedOptimisticLock( schema.keyType(), schema.keyId() );
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
        sharedOptimisticLock( ResourceTypes.LABEL, labelId );
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
        sharedOptimisticLock( ResourceTypes.RELATIONSHIP_TYPE, typeId );
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

    void getOrCreateNodeIndexConfig( String indexName, Map<String,String> customConfig )
    {
        explicitIndexStore.getOrCreateNodeIndexConfig( indexName, customConfig );
    }

    void getOrCreateRelationshipIndexConfig( String indexName, Map<String,String> customConfig )
    {
        explicitIndexStore.getOrCreateRelationshipIndexConfig( indexName, customConfig );
    }

    String indexGetFailure( IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        return storageReader.indexGetFailure( descriptor.schema() );
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
    public <K, V> V schemaStateGet( K key )
    {
        return schemaState.get( key );
    }

    @Override
    public void schemaStateFlush()
    {
        schemaState.clear();
    }

    ExplicitIndexStore explicitIndexStore()
    {
        return explicitIndexStore;
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
        ctx.put( Context.DEPENDENCY_RESOLVER, dataSourceDependencies );
        ctx.put( Context.THREAD, Thread.currentThread() );
        ClockContext clocks = ktx.clocks();
        ctx.put( Context.SYSTEM_CLOCK, clocks.systemClock() );
        ctx.put( Context.STATEMENT_CLOCK, clocks.statementClock() );
        ctx.put( Context.TRANSACTION_CLOCK, clocks.transactionClock() );
        ctx.put( Context.SECURITY_CONTEXT, securityContext );
        return ctx;
    }

    private void assertValidIndex( IndexReference index ) throws IndexNotFoundKernelException
    {
        if ( index == IndexReference.NO_INDEX )
        {
            throw new IndexNotFoundKernelException( "No index was found" );
        }
    }
}
