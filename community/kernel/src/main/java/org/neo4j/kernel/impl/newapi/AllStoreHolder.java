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

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.neo4j.collection.RawIterator;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.CapableIndexReference;
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
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.api.ExplicitIndex;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.proc.BasicContext;
import org.neo4j.kernel.api.proc.Context;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptorFactory;
import org.neo4j.kernel.api.txstate.TransactionCountingStateVisitor;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.ClockContext;
import org.neo4j.kernel.impl.api.CountsRecordState;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.SchemaState;
import org.neo4j.kernel.impl.api.security.OverriddenAccessMode;
import org.neo4j.kernel.impl.api.security.RestrictedAccessMode;
import org.neo4j.kernel.impl.api.store.DefaultCapableIndexReference;
import org.neo4j.kernel.impl.api.store.DefaultIndexReference;
import org.neo4j.kernel.impl.api.store.PropertyUtil;
import org.neo4j.kernel.impl.index.ExplicitIndexStore;
import org.neo4j.kernel.impl.index.IndexEntityType;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.store.RecordCursor;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.register.Register;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageStatement;
import org.neo4j.storageengine.api.StoreReadLayer;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.LabelScanReader;
import org.neo4j.storageengine.api.schema.PopulationProgress;
import org.neo4j.storageengine.api.txstate.ReadableDiffSets;
import org.neo4j.string.UTF8;
import org.neo4j.values.AnyValue;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.lang.String.format;
import static org.neo4j.helpers.collection.Iterators.emptyResourceIterator;
import static org.neo4j.helpers.collection.Iterators.filter;
import static org.neo4j.helpers.collection.Iterators.iterator;
import static org.neo4j.helpers.collection.Iterators.singleOrNull;
import static org.neo4j.internal.kernel.api.schema.SchemaDescriptorPredicates.hasProperty;
import static org.neo4j.kernel.impl.api.store.DefaultIndexReference.fromDescriptor;
import static org.neo4j.register.Registers.newDoubleLongRegister;
import static org.neo4j.storageengine.api.txstate.TxStateVisitor.EMPTY;

public class AllStoreHolder extends Read
{
    private final StorageStatement.Nodes nodes;
    private final StorageStatement.Groups groups;
    private final StorageStatement.Properties properties;
    private final StorageStatement.Relationships relationships;
    private final StorageStatement statement;
    private final StoreReadLayer storeReadLayer;
    private final ExplicitIndexStore explicitIndexStore;
    private final Procedures procedures;
    private final SchemaState schemaState;

    public AllStoreHolder( StorageEngine engine,
            StorageStatement statement,
            KernelTransactionImplementation ktx,
            DefaultCursors cursors,
            ExplicitIndexStore explicitIndexStore,
            Procedures procedures,
            SchemaState schemaState )
    {
        super( cursors, ktx );
        this.storeReadLayer = engine.storeReadLayer();
        this.statement = statement; // use provided statement, to assert no leakage
        this.nodes = statement.nodes();
        this.relationships = statement.relationships();
        this.groups = statement.groups();
        this.properties = statement.properties();
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
        return storeReadLayer.nodeExists( reference );
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
                txState.accept( new TransactionCountingStateVisitor( EMPTY, storeReadLayer,
                        statement, txState, counts ) );
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
        return storeReadLayer.countsForNode( labelId );
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
                txState.accept( new TransactionCountingStateVisitor( EMPTY, storeReadLayer,
                        statement, txState, counts ) );
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
        return storeReadLayer.countsForRelationship( startLabelId, typeId, endLabelId );
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
        return storeReadLayer.relationshipExists( reference );
    }

    @Override
    long graphPropertiesReference()
    {
        return statement.getGraphPropertyReference();
    }

    @Override
    IndexReader indexReader( IndexReference index, boolean fresh ) throws IndexNotFoundKernelException
    {
        SchemaIndexDescriptor schemaIndexDescriptor = index.isUnique() ?
                                                      SchemaIndexDescriptorFactory.uniqueForLabel( index.label(), index.properties() ) :
                                                      SchemaIndexDescriptorFactory.forLabel( index.label(), index.properties() );
        return fresh ? statement.getFreshIndexReader( schemaIndexDescriptor ) :
               statement.getIndexReader( schemaIndexDescriptor );
    }

    @Override
    LabelScanReader labelScanReader()
    {
        return statement.getLabelScanReader();
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
        SchemaIndexDescriptor indexDescriptor = storeReadLayer.indexGetForSchema( descriptor );
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

    @Override
    public Iterator<IndexReference> indexesGetForLabel( int labelId )
    {
        sharedOptimisticLock( ResourceTypes.LABEL, labelId );
        ktx.assertOpen();

        Iterator<SchemaIndexDescriptor> iterator = storeReadLayer.indexesGetForLabel( labelId );
        if ( ktx.hasTxStateWithChanges() )
        {
            iterator = ktx.txState().indexDiffSetsByLabel( labelId ).apply( iterator );
        }
        return Iterators.map( DefaultIndexReference::fromDescriptor, iterator );
    }

    @Override
    public Iterator<IndexReference> indexesGetAll()
    {
        ktx.assertOpen();

        Iterator<SchemaIndexDescriptor> iterator = storeReadLayer.indexesGetAll();
        if ( ktx.hasTxStateWithChanges() )
        {
            iterator = ktx.txState().indexChanges().apply( storeReadLayer.indexesGetAll() );
        }

        return Iterators.map( indexDescriptor ->
        {
            sharedOptimisticLock( ResourceTypes.LABEL, indexDescriptor.schema().keyId() );
            return fromDescriptor( indexDescriptor );
        }, iterator );
    }

    @Override
    public InternalIndexState indexGetState( IndexReference index ) throws IndexNotFoundKernelException
    {
        assertValidIndex( index );
        sharedOptimisticLock( ResourceTypes.LABEL, index.label() );
        ktx.assertOpen();
        return indexGetState( indexDescriptor( index ) );
    }

    @Override
    public PopulationProgress indexGetPopulationProgress( IndexReference index )
            throws IndexNotFoundKernelException
    {
        sharedOptimisticLock( ResourceTypes.LABEL, index.label() );
        ktx.assertOpen();
        SchemaIndexDescriptor descriptor = indexDescriptor( index );

        if ( ktx.hasTxStateWithChanges() )
        {
            if ( checkIndexState( descriptor,
                    ktx.txState().indexDiffSetsByLabel( index.label() ) ) )
            {
                return PopulationProgress.NONE;
            }
        }

        return storeReadLayer.indexGetPopulationProgress( descriptor.schema() );
    }

    @Override
    public Long indexGetOwningUniquenessConstraintId( IndexReference index )
    {
        sharedOptimisticLock( ResourceTypes.LABEL, index.label() );
        ktx.assertOpen();
        return indexGetOwningUniquenessConstraintId( indexDescriptor( index ) );
    }

    @Override
    public long indexGetCommittedId( IndexReference index ) throws SchemaRuleNotFoundException
    {
        sharedOptimisticLock( ResourceTypes.LABEL, index.label() );
        ktx.assertOpen();
        return storeReadLayer.indexGetCommittedId( indexDescriptor( index ) );
    }

    SchemaIndexDescriptor indexDescriptor( IndexReference index )
    {
        if ( index.isUnique() )
        {
            return SchemaIndexDescriptorFactory.uniqueForLabel( index.label(), index.properties() );
        }
        else
        {
            return SchemaIndexDescriptorFactory.forLabel( index.label(), index.properties() );
        }
    }

    @Override
    public String indexGetFailure( IndexReference index ) throws IndexNotFoundKernelException
    {
        return storeReadLayer.indexGetFailure( SchemaDescriptorFactory.forLabel( index.label(), index.properties() ) );
    }

    @Override
    public double indexUniqueValuesSelectivity( IndexReference index ) throws IndexNotFoundKernelException
    {
        acquireSharedLabelLock( index.label() );
        ktx.assertOpen();
        return storeReadLayer
                .indexUniqueValuesPercentage( SchemaDescriptorFactory.forLabel( index.label(), index.properties() ) );
    }

    @Override
    public long indexSize( IndexReference index ) throws IndexNotFoundKernelException
    {
        acquireSharedLabelLock( index.label() );
        ktx.assertOpen();
        return storeReadLayer.indexSize( SchemaDescriptorFactory.forLabel( index.label(), index.properties() ) );
    }

    @Override
    public long nodesCountIndexed( IndexReference index, long nodeId, Value value ) throws KernelException
    {
        ktx.assertOpen();
        IndexReader reader = statement.getIndexReader( DefaultIndexReference.toDescriptor( index ) );
        return reader.countIndexedNodes( nodeId, value );
    }

    @Override
    public long nodesGetCount( )
    {
        ktx.assertOpen();
        long base = storeReadLayer.nodesGetCount();
        return ktx.hasTxStateWithChanges() ? base + ktx.txState().addedAndRemovedNodes().delta() : base;
    }

    @Override
    public long relationshipsGetCount( )
    {
        ktx.assertOpen();
        long base = storeReadLayer.relationshipsGetCount();
        return ktx.hasTxStateWithChanges() ? base + ktx.txState().addedAndRemovedRelationships().delta() : base;
    }

    @Override
    public Register.DoubleLongRegister indexUpdatesAndSize( IndexReference index, Register.DoubleLongRegister target )
            throws IndexNotFoundKernelException
    {
        ktx.assertOpen();
        return storeReadLayer.indexUpdatesAndSize(
                SchemaDescriptorFactory.forLabel( index.label(), index.properties() ), target );

    }

    @Override
    public Register.DoubleLongRegister indexSample( IndexReference index, Register.DoubleLongRegister target )
            throws IndexNotFoundKernelException
    {
        ktx.assertOpen();
        return storeReadLayer.indexSample(
                SchemaDescriptorFactory.forLabel( index.label(), index.properties() ), target );
    }

    CapableIndexReference indexGetCapability( SchemaIndexDescriptor schemaIndexDescriptor )
    {
        try
        {
            return storeReadLayer.indexReference( schemaIndexDescriptor );
        }
        catch ( IndexNotFoundKernelException e )
        {
            throw new IllegalStateException( "Could not find capability for index " + schemaIndexDescriptor, e );
        }
    }

    InternalIndexState indexGetState( SchemaIndexDescriptor descriptor ) throws IndexNotFoundKernelException
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

        return storeReadLayer.indexGetState( descriptor );
    }

    Long indexGetOwningUniquenessConstraintId( SchemaIndexDescriptor index )
    {
        return storeReadLayer.indexGetOwningUniquenessConstraintId( index );
    }

    SchemaIndexDescriptor indexGetForSchema( SchemaDescriptor descriptor )
    {
        SchemaIndexDescriptor indexDescriptor = storeReadLayer.indexGetForSchema( descriptor );
        Iterator<SchemaIndexDescriptor> rules = iterator( indexDescriptor );
        if ( ktx.hasTxStateWithChanges() )
        {
            rules = filter(
                    SchemaDescriptor.equalTo( descriptor ),
                    ktx.txState().indexDiffSetsByLabel( descriptor.keyId() ).apply( rules ) );
        }
        return singleOrNull( rules );
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
    public Iterator<ConstraintDescriptor> constraintsGetForSchema( SchemaDescriptor descriptor )
    {
        sharedOptimisticLock( descriptor.keyType(), descriptor.keyId() );
        ktx.assertOpen();
        Iterator<ConstraintDescriptor> constraints = storeReadLayer.constraintsGetForSchema( descriptor );
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
        boolean inStore = storeReadLayer.constraintExists( descriptor );
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
        Iterator<ConstraintDescriptor> constraints = storeReadLayer.constraintsGetForLabel( labelId );
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
        Iterator<ConstraintDescriptor> constraints = storeReadLayer.constraintsGetAll();
        if ( ktx.hasTxStateWithChanges() )
        {
            constraints = ktx.txState().constraintsChanges().apply( constraints );
        }
        return Iterators.map( this::lockConstraint, constraints );
    }

    Iterator<ConstraintDescriptor> constraintsGetForProperty( int propertyKey )
    {
        ktx.assertOpen();
        Iterator<ConstraintDescriptor> constraints = storeReadLayer.constraintsGetAll();
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
        sharedOptimisticLock( ResourceTypes.RELATIONSHIP_TYPE, typeId );
        ktx.assertOpen();
        Iterator<ConstraintDescriptor> constraints = storeReadLayer.constraintsGetForRelationshipType( typeId );
        if ( ktx.hasTxStateWithChanges() )
        {
            return ktx.txState().constraintsChangesForRelationshipType( typeId ).apply( constraints );
        }
        return constraints;
    }

    @Override
    PageCursor nodePage( long reference )
    {
        return nodes.openPageCursorForReading( reference );
    }

    @Override
    PageCursor relationshipPage( long reference )
    {
        return relationships.openPageCursorForReading( reference );
    }

    @Override
    PageCursor groupPage( long reference )
    {
        return groups.openPageCursorForReading( reference );
    }

    @Override
    PageCursor propertyPage( long reference )
    {
        return properties.openPageCursorForReading( reference );
    }

    @Override
    PageCursor stringPage( long reference )
    {
        return properties.openStringPageCursor( reference );
    }

    @Override
    PageCursor arrayPage( long reference )
    {
        return properties.openArrayPageCursor( reference );
    }

    @Override
    RecordCursor<DynamicRecord> labelCursor()
    {
        return nodes.newLabelCursor();
    }

    @Override
    void node( NodeRecord record, long reference, PageCursor pageCursor )
    {
        nodes.getRecordByCursor( reference, record, RecordLoad.CHECK, pageCursor );
    }

    @Override
    void relationship( RelationshipRecord record, long reference, PageCursor pageCursor )
    {
        // When scanning, we inspect RelationshipRecord.inUse(), so using RecordLoad.CHECK is fine
        relationships.getRecordByCursor( reference, record, RecordLoad.CHECK, pageCursor );
    }

    @Override
    void relationshipFull( RelationshipRecord record, long reference, PageCursor pageCursor )
    {
        // We need to load forcefully for relationship chain traversal since otherwise we cannot
        // traverse over relationship records which have been concurrently deleted
        // (flagged as inUse = false).
        // see
        //      org.neo4j.kernel.impl.store.RelationshipChainPointerChasingTest
        //      org.neo4j.kernel.impl.locking.RelationshipCreateDeleteIT
        relationships.getRecordByCursor( reference, record, RecordLoad.FORCE, pageCursor );
    }

    @Override
    void property( PropertyRecord record, long reference, PageCursor pageCursor )
    {
        // We need to load forcefully here since otherwise we can have inconsistent reads
        // for properties across blocks, see org.neo4j.graphdb.ConsistentPropertyReadsIT
        properties.getRecordByCursor( reference, record, RecordLoad.FORCE, pageCursor );
    }

    @Override
    void group( RelationshipGroupRecord record, long reference, PageCursor page )
    {
        // We need to load forcefully here since otherwise we cannot traverse over groups
        // records which have been concurrently deleted (flagged as inUse = false).
        // @see #org.neo4j.kernel.impl.store.RelationshipChainPointerChasingTest
        groups.getRecordByCursor( reference, record, RecordLoad.FORCE, page );
    }

    @Override
    long nodeHighMark()
    {
        return nodes.getHighestPossibleIdInUse();
    }

    @Override
    long relationshipHighMark()
    {
        return relationships.getHighestPossibleIdInUse();
    }

    @Override
    TextValue string( DefaultPropertyCursor cursor, long reference, PageCursor page )
    {
        ByteBuffer buffer = cursor.buffer = properties.loadString( reference, cursor.buffer, page );
        buffer.flip();
        return Values.stringValue( UTF8.decode( buffer.array(), 0, buffer.limit() ) );
    }

    @Override
    ArrayValue array( DefaultPropertyCursor cursor, long reference, PageCursor page )
    {
        ByteBuffer buffer = cursor.buffer = properties.loadArray( reference, cursor.buffer, page );
        buffer.flip();
        return PropertyUtil.readArrayFromBuffer( buffer );
    }

    boolean nodeExistsInStore( long id )
    {
        return storeReadLayer.nodeExists( id );
    }

    void getOrCreateNodeIndexConfig( String indexName, Map<String,String> customConfig )
    {
        explicitIndexStore.getOrCreateNodeIndexConfig( indexName, customConfig );
    }

    void getOrCreateRelationshipIndexConfig( String indexName, Map<String,String> customConfig )
    {
        explicitIndexStore.getOrCreateRelationshipIndexConfig( indexName, customConfig );
    }

    String indexGetFailure( SchemaIndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        return storeReadLayer.indexGetFailure( descriptor.schema() );
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
    public Set<ProcedureSignature> proceduresGetAll( ) throws ProcedureException
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

    private void assertValidIndex( IndexReference index ) throws IndexNotFoundKernelException
    {
        if ( index == CapableIndexReference.NO_INDEX )
        {
            throw new IndexNotFoundKernelException( "No index was found" );
        }
    }

    private ConstraintDescriptor lockConstraint( ConstraintDescriptor constraint )
    {
        SchemaDescriptor schema = constraint.schema();
        ktx.statementLocks().pessimistic().acquireShared( ktx.lockTracer(), schema.keyType(), schema.keyId() );
        return constraint;
    }
}
