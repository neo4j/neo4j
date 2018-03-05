/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.neo4j.collection.RawIterator;
import org.neo4j.function.Suppliers;
import org.neo4j.function.Suppliers.Lazy;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.CapableIndexReference;
import org.neo4j.internal.kernel.api.IndexCapability;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.exceptions.explicitindex.ExplicitIndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.kernel.api.procs.ProcedureHandle;
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
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.proc.BasicContext;
import org.neo4j.kernel.api.proc.Context;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
import org.neo4j.kernel.api.txstate.ExplicitIndexTransactionState;
import org.neo4j.kernel.api.txstate.TransactionCountingStateVisitor;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.ClockContext;
import org.neo4j.kernel.impl.api.CountsRecordState;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.security.OverriddenAccessMode;
import org.neo4j.kernel.impl.api.security.RestrictedAccessMode;
import org.neo4j.kernel.impl.api.store.PropertyUtil;
import org.neo4j.kernel.impl.index.ExplicitIndexStore;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.store.RecordCursor;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageStatement;
import org.neo4j.storageengine.api.StoreReadLayer;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.LabelScanReader;
import org.neo4j.storageengine.api.txstate.ReadableDiffSets;
import org.neo4j.string.UTF8;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Values;

import static java.lang.String.format;
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
    private final Lazy<ExplicitIndexTransactionState> explicitIndexes;
    private final Procedures procedures;

    public AllStoreHolder( StorageEngine engine,
            StorageStatement statement,
            KernelTransactionImplementation ktx,
            DefaultCursors cursors,
            ExplicitIndexStore explicitIndexStore, Procedures procedures )
    {
        super( cursors, ktx );
        this.storeReadLayer = engine.storeReadLayer();
        this.statement = statement; // use provided statement, to assert no leakage
        this.explicitIndexes = Suppliers.lazySingleton( ktx::explicitIndexTxState );

        this.nodes = statement.nodes();
        this.relationships = statement.relationships();
        this.groups = statement.groups();
        this.properties = statement.properties();
        this.explicitIndexStore = explicitIndexStore;
        this.procedures = procedures;
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
    IndexReader indexReader( IndexReference index ) throws IndexNotFoundKernelException
    {
        IndexDescriptor indexDescriptor = index.isUnique() ?
                                          IndexDescriptorFactory.uniqueForLabel( index.label(), index.properties() ) :
                                          IndexDescriptorFactory.forLabel( index.label(), index.properties() );
        return statement.getIndexReader( indexDescriptor );
    }

    @Override
    LabelScanReader labelScanReader()
    {
        return statement.getLabelScanReader();
    }

    @Override
    ExplicitIndex explicitNodeIndex( String indexName ) throws ExplicitIndexNotFoundKernelException
    {
        return explicitIndexes.get().nodeChanges( indexName );
    }

    @Override
    ExplicitIndex explicitRelationshipIndex( String indexName ) throws ExplicitIndexNotFoundKernelException
    {
        return explicitIndexes.get().relationshipChanges( indexName );
    }

    @Override
    public CapableIndexReference index( int label, int... properties )
    {
        ktx.assertOpen();
        IndexDescriptor indexDescriptor = storeReadLayer.indexGetForSchema( new LabelSchemaDescriptor( label, properties ) );
        if ( indexDescriptor == null )
        {
            return CapableIndexReference.NO_INDEX;
        }

        return indexGetCapability( indexDescriptor);
    }

    @Override
    public Iterator<CapableIndexReference> indexesGetForLabel( int labelId )
    {
        sharedOptimisticLock( ResourceTypes.LABEL, labelId );
        ktx.assertOpen();

        Iterator<IndexDescriptor> iterator = storeReadLayer.indexesGetForLabel( labelId );
        if ( ktx.hasTxStateWithChanges() )
        {
            iterator = ktx.txState().indexDiffSetsByLabel( labelId ).apply( iterator );
        }
        return Iterators.map( this::indexGetCapability, iterator );
    }

    CapableIndexReference indexGetCapability( IndexDescriptor indexDescriptor )
    {
        boolean unique = indexDescriptor.type() == IndexDescriptor.Type.UNIQUE;
        try
        {
            IndexCapability indexCapability = storeReadLayer.indexGetCapability( indexDescriptor );
            return new DefaultCapableIndexReference( unique, indexCapability, indexDescriptor.schema().getLabelId(),
                    indexDescriptor.schema().getPropertyIds() );
        }
        catch ( IndexNotFoundKernelException e )
        {
            throw new IllegalStateException( "Could not find capability for index " + indexDescriptor, e );
        }
    }

    InternalIndexState indexGetState( IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        // If index is in our state, then return populating
        if ( ktx.hasTxStateWithChanges() )
        {
            if ( checkIndexState( descriptor,
                    ktx.txState().indexDiffSetsByLabel( descriptor.schema().getLabelId() ) ) )
            {
                return InternalIndexState.POPULATING;
            }
        }

        return storeReadLayer.indexGetState( descriptor );
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
        return Iterators.map( constraintDescriptor ->
        {
            SchemaDescriptor schema = constraintDescriptor.schema();
            ktx.locks().pessimistic().acquireShared( ktx.lockTracer(), schema.keyType(), schema.keyId() );
            return constraintDescriptor;
        }, constraints );
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

    String indexGetFailure( IndexDescriptor descriptor ) throws IndexNotFoundKernelException
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
        return callProcedure( id, arguments, new RestrictedAccessMode( ktx.securityContext().mode(), AccessMode.Static.TOKEN_WRITE ) );
    }

    @Override
    public RawIterator<Object[],ProcedureException> procedureCallWriteOverride( int id, Object[] arguments )
            throws ProcedureException
    {
        return callProcedure( id, arguments, new OverriddenAccessMode( ktx.securityContext().mode(), AccessMode.Static.TOKEN_WRITE ) );

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
        return aggregationFunction( id, new RestrictedAccessMode( ktx.securityContext().mode(), AccessMode.Static.READ ) );
    }

    @Override
    public UserAggregator aggregationFunctionOverride( int id ) throws ProcedureException
    {
        return aggregationFunction( id,
                new OverriddenAccessMode( ktx.securityContext().mode(), AccessMode.Static.READ ) );
    }
    private RawIterator<Object[],ProcedureException> callProcedure(
            int id, Object[] input, final AccessMode override  )
            throws ProcedureException
    {
        ktx.assertOpen();

        final SecurityContext procedureSecurityContext = ktx.securityContext().withMode( override );
        final RawIterator<Object[],ProcedureException> procedureCall;
        try ( KernelTransaction.Revertable ignore = ktx.overrideWith( procedureSecurityContext ) )
        {
            BasicContext ctx = new BasicContext();
            ctx.put( Context.KERNEL_TRANSACTION, ktx );
            ctx.put( Context.THREAD, Thread.currentThread() );
            ctx.put( Context.SECURITY_CONTEXT, procedureSecurityContext );
            procedureCall = procedures.callProcedure( ctx, id, input, ktx.acquireStatement() );
        }
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
            BasicContext ctx = new BasicContext();
            ctx.put( Context.KERNEL_TRANSACTION, ktx );
            ctx.put( Context.THREAD, Thread.currentThread() );
            ClockContext clocks = ktx.clocks();
            ctx.put( Context.SYSTEM_CLOCK, clocks.systemClock() );
            ctx.put( Context.STATEMENT_CLOCK, clocks.statementClock() );
            ctx.put( Context.TRANSACTION_CLOCK, clocks.transactionClock() );
            return procedures.callFunction( ctx, id, input );
        }
    }

    private UserAggregator aggregationFunction( int id, final AccessMode mode )
            throws ProcedureException
    {
        ktx.assertOpen();

        try ( KernelTransaction.Revertable ignore = ktx.overrideWith( ktx.securityContext().withMode( mode ) ) )
        {
            BasicContext ctx = new BasicContext();
            ctx.put( Context.KERNEL_TRANSACTION, ktx );
            ctx.put( Context.THREAD, Thread.currentThread() );
            return procedures.createAggregationFunction( ctx, id );
        }
    }
}
