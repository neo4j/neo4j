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

import org.neo4j.common.EntityType;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.index.label.LabelScan;
import org.neo4j.internal.index.label.LabelScanReader;
import org.neo4j.internal.kernel.api.AutoCloseablePlus;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.DefaultCloseListenable;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.LabelSet;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipIndexCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.Scan;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptorSupplier;
import org.neo4j.kernel.api.AssertOpen;
import org.neo4j.kernel.api.exceptions.schema.IndexBrokenKernelException;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.ResourceType;
import org.neo4j.lock.ResourceTypes;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.Values;

import static java.lang.String.format;
import static org.neo4j.kernel.impl.newapi.RelationshipReferenceEncoding.clearEncoding;
import static org.neo4j.storageengine.api.RelationshipDirection.INCOMING;
import static org.neo4j.storageengine.api.RelationshipDirection.LOOP;
import static org.neo4j.storageengine.api.RelationshipDirection.OUTGOING;
import static org.neo4j.values.storable.ValueGroup.GEOMETRY;
import static org.neo4j.values.storable.ValueGroup.NUMBER;

abstract class Read implements TxStateHolder,
        org.neo4j.internal.kernel.api.Read,
        org.neo4j.internal.kernel.api.SchemaRead,
        org.neo4j.internal.kernel.api.Procedures,
        org.neo4j.internal.kernel.api.Locks,
        AssertOpen,
        LockingNodeUniqueIndexSeek.UniqueNodeIndexSeeker<DefaultNodeValueIndexCursor>,
        QueryContext
{
    private final StorageReader storageReader;
    protected final DefaultPooledCursors cursors;
    final KernelTransactionImplementation ktx;

    Read( StorageReader storageReader, DefaultPooledCursors cursors,
            KernelTransactionImplementation ktx )
    {
        this.storageReader = storageReader;
        this.cursors = cursors;
        this.ktx = ktx;
    }

    @Override
    public final void nodeIndexSeek( IndexReadSession index, NodeValueIndexCursor cursor, IndexOrder indexOrder, boolean needsValues, IndexQuery... query )
            throws IndexNotApplicableKernelException
    {
        ktx.assertOpen();
        DefaultIndexReadSession indexSession = (DefaultIndexReadSession) index;

        if ( indexSession.reference.schema().entityType() != EntityType.NODE )
        {
            throw new IndexNotApplicableKernelException( "Node index seek can only be performed on node indexes: " + index );
        }

        EntityIndexSeekClient client = (EntityIndexSeekClient) cursor;
        client.setRead( this );
        IndexProgressor.EntityValueClient withSecurity = injectSecurity( client, ktx.securityContext().mode(), indexSession.reference );
        IndexProgressor.EntityValueClient withFullPrecision = injectFullValuePrecision( withSecurity, query, indexSession.reader );
        indexSession.reader.query( this, withFullPrecision, indexOrder, needsValues, query );
    }

    @Override
    public final void relationshipIndexSeek( IndexDescriptor index, RelationshipIndexCursor cursor, IndexQuery... query )
            throws IndexNotApplicableKernelException, IndexNotFoundKernelException
    {
        ktx.assertOpen();
        if ( index.schema().entityType() != EntityType.RELATIONSHIP )
        {
            throw new IndexNotApplicableKernelException( "Relationship index seek can only be performed on node indexes: " + index );
        }

        EntityIndexSeekClient client = (EntityIndexSeekClient) cursor;
        IndexReader reader = indexReader( index, false );
        client.setRead( this );
        IndexProgressor.EntityValueClient withSecurity = injectSecurity( client, ktx.securityContext().mode(), index );
        IndexProgressor.EntityValueClient withFullPrecision = injectFullValuePrecision( withSecurity, query, reader );
        reader.query( this, withFullPrecision, IndexOrder.NONE, false, query );
    }

    @Override
    public void nodeIndexDistinctValues( IndexDescriptor index, NodeValueIndexCursor cursor, boolean needsValues ) throws IndexNotFoundKernelException
    {
        ktx.assertOpen();
        DefaultNodeValueIndexCursor cursorImpl = (DefaultNodeValueIndexCursor) cursor;
        IndexReader reader = indexReader( index, true );
        cursorImpl.setRead( this );
        CursorPropertyAccessor accessor = new CursorPropertyAccessor( cursors.allocateNodeCursor(), cursors.allocatePropertyCursor(), this );
        reader.distinctValues( cursorImpl, accessor, needsValues );
    }

    private IndexProgressor.EntityValueClient injectSecurity( IndexProgressor.EntityValueClient cursor, AccessMode accessMode,
            IndexDescriptor index )
    {
        SchemaDescriptor schema = index.schema();
        int[] propertyIds = schema.getPropertyIds();

        boolean allowsForAllLabels = true;
        for ( int prop : propertyIds )
        {
            allowsForAllLabels &= accessMode.allowsReadPropertyAllLabels( prop ) && accessMode.allowsTraverseAllLabels();
        }

        if ( schema.entityType().equals( EntityType.NODE ) && !allowsForAllLabels )
        {
            for ( int prop : propertyIds )
            {
                for ( int label : schema.getEntityTokenIds() )
                {
                    if ( !accessMode.allowsTraverseAllNodesWithLabel( label ) || accessMode.disallowsReadPropertyForSomeLabel( prop ) ||
                            !accessMode.allowsReadNodeProperty( () -> Labels.from( label ), prop ) )
                    {
                        // We need to filter the index result if the property is not allowed on some label
                        // since the nodes in the index might have both an allowed and a disallowed label for the property
                        return new NodeLabelSecurityFilter( propertyIds, cursor, cursors.allocateNodeCursor(), this, accessMode );
                    }
                }
            }
        }

        if ( schema.entityType().equals( EntityType.RELATIONSHIP ) )
        {
            for ( int prop : propertyIds )
            {
                for ( int relType : schema.getEntityTokenIds() )
                {
                    if ( !accessMode.allowsTraverseAllLabels() || !accessMode.allowsTraverseRelType( relType ) ||
                            !accessMode.allowsReadRelationshipProperty( () -> relType, prop ) )
                    {
                        return new RelationshipSecurityFilter( propertyIds, cursor, cursors.allocateRelationshipScanCursor(), this, accessMode );
                    }
                }
            }
        }

        // everything in this index is whitelisted
        return cursor;
    }

    private IndexProgressor.EntityValueClient injectFullValuePrecision( IndexProgressor.EntityValueClient cursor,
            IndexQuery[] query, IndexReader reader )
    {
        IndexProgressor.EntityValueClient target = cursor;
        if ( !reader.hasFullValuePrecision( query ) )
        {
            IndexQuery[] filters = new IndexQuery[query.length];
            int count = 0;
            for ( int i = 0; i < query.length; i++ )
            {
                IndexQuery q = query[i];
                switch ( q.type() )
                {
                case range:
                    ValueGroup valueGroup = q.valueGroup();
                    if ( ( valueGroup == NUMBER || valueGroup == GEOMETRY) && !reader.hasFullValuePrecision( q ) )
                    {
                        filters[i] = q;
                        count++;
                    }
                    break;
                case exact:
                    Value value = ((IndexQuery.ExactPredicate) q).value();
                    if ( value.valueGroup() == ValueGroup.NUMBER || Values.isArrayValue( value ) || value.valueGroup() == ValueGroup.GEOMETRY )
                    {
                        if ( !reader.hasFullValuePrecision( q ) )
                        {
                            filters[i] = q;
                            count++;
                        }
                    }
                    break;
                default:
                    break;
                }
            }
            if ( count > 0 )
            {
                // filters[] can contain null elements. The non-null elements are the filters and each sit in the designated slot
                // matching the values from the index.
                target = new NodeValueClientFilter( target, cursors.allocateNodeCursor(),
                        cursors.allocatePropertyCursor(), this, filters );
            }
        }
        return target;
    }

    @Override
    public org.neo4j.internal.kernel.api.Read getRead()
    {
        return this;
    }

    @Override
    public CursorFactory cursors()
    {
        return cursors;
    }

    @Override
    public ReadableTransactionState getTransactionStateOrNull()
    {
        return hasTxStateWithChanges() ? txState() : null;
    }

    @Override
    public long lockingNodeUniqueIndexSeek( IndexDescriptor index,
                                            NodeValueIndexCursor cursor,
                                            IndexQuery.ExactPredicate... predicates )
            throws IndexNotApplicableKernelException, IndexNotFoundKernelException, IndexBrokenKernelException
    {
        assertIndexOnline( index );
        assertPredicatesMatchSchema( index, predicates );

        Locks.Client locks = ktx.statementLocks().optimistic();
        LockTracer lockTracer = ktx.lockTracer();

        return LockingNodeUniqueIndexSeek.apply( locks, lockTracer, (DefaultNodeValueIndexCursor)cursor, this, this, index, predicates );
    }

    @Override // UniqueNodeIndexSeeker
    public void nodeIndexSeekWithFreshIndexReader(
            DefaultNodeValueIndexCursor cursor,
            IndexReader indexReader,
            IndexQuery.ExactPredicate... query ) throws IndexNotApplicableKernelException
    {
        cursor.setRead( this );
        IndexProgressor.EntityValueClient target = injectFullValuePrecision( cursor, query, indexReader );
        // we never need values for exact predicates
        indexReader.query( this, target, IndexOrder.NONE, false, query );
    }

    @Override
    public final void nodeIndexScan( IndexReadSession index,
                                     NodeValueIndexCursor cursor,
                                     IndexOrder indexOrder,
                                     boolean needsValues ) throws KernelException
    {
        ktx.assertOpen();
        DefaultIndexReadSession indexSession = (DefaultIndexReadSession) index;

        if ( indexSession.reference.schema().entityType() != EntityType.NODE )
        {
            throw new IndexNotApplicableKernelException( "Node index scan can only be performed on node indexes: " + index );
        }

        // for a scan, we simply query for existence of the first property, which covers all entries in an index
        int firstProperty = indexSession.reference.schema().getPropertyIds()[0];

        DefaultNodeValueIndexCursor cursorImpl = (DefaultNodeValueIndexCursor) cursor;
        cursorImpl.setRead( this );
        IndexProgressor.EntityValueClient withSecurity = injectSecurity( cursorImpl, ktx.securityContext().mode(), indexSession.reference );
        indexSession.reader.query( this, withSecurity, indexOrder, needsValues, IndexQuery.exists( firstProperty ) );
    }

    @Override
    public final void nodeLabelScan( int label, NodeLabelIndexCursor cursor )
    {
        ktx.assertOpen();

        DefaultNodeLabelIndexCursor indexCursor = (DefaultNodeLabelIndexCursor) cursor;
        indexCursor.setRead( this );
        IndexProgressor indexProgressor;
        AccessMode accessMode = ktx.securityContext().mode();
        if ( accessMode.allowsTraverseAllNodesWithLabel( label ) )
        {
            // all nodes will be allowed
            LabelScan labelScan = labelScanReader().nodeLabelScan( label );
            indexProgressor = labelScan.initialize( indexCursor.nodeLabelClient() );
        }
        else if ( accessMode.disallowsTraverseLabel( label ) )
        {
            // no nodes of this label will be allowed
            indexProgressor = IndexProgressor.EMPTY;
        }
        else
        {
            // some nodes of this label might be blocked. we need to filter
            LabelScan labelScan = labelScanReader().nodeLabelScan( label );
            indexProgressor = labelScan.initialize( filteringNodeLabelClient( indexCursor.nodeLabelClient(), accessMode ) );
        }
        // TODO: When we have a blacklisted label, perhaps we should not consider labels added within the current transaction
        indexCursor.scan( indexProgressor, label );
    }

    IndexProgressor.NodeLabelClient filteringNodeLabelClient( IndexProgressor.NodeLabelClient inner, AccessMode accessMode )
    {
        return new FilteringNodeLabelClient( inner, accessMode );
    }

    private class FilteringNodeLabelClient extends DefaultCloseListenable implements IndexProgressor.NodeLabelClient, AutoCloseablePlus
    {
        private FullAccessNodeCursor node;
        private final IndexProgressor.NodeLabelClient inner;
        private final AccessMode accessMode;

        private FilteringNodeLabelClient( IndexProgressor.NodeLabelClient inner, AccessMode accessMode )
        {
            this.inner = inner;
            this.accessMode = accessMode;
            this.node = Read.this.cursors.allocateFullAccessNodeCursor();
        }

        @Override
        public boolean acceptNode( long reference, LabelSet labels )
        {
            if ( labels == null )
            {
                node.single( reference, Read.this );
                if ( !node.next() )
                {
                    return false;
                }
                labels = node.labelsIgnoringTxStateSetRemove();
            }
            return inner.acceptNode( reference, labels ) && accessMode.allowsTraverseNode( labels.all() );
        }

        @Override
        public void close()
        {
            closeInternal();
            var listener = closeListener;
            if ( listener != null )
            {
                listener.onClosed( this );
            }
        }

        @Override
        public void closeInternal()
        {
            if ( !isClosed() )
            {
                node.close();
                node = null;
            }
        }

        @Override
        public boolean isClosed()
        {
            return node == null;
        }
    }

    @Override
    public final Scan<NodeLabelIndexCursor> nodeLabelScan( int label )
    {
        ktx.assertOpen();
        return new NodeLabelIndexCursorScan( this, label, labelScanReader().nodeLabelScan( label ) );
    }

    @Override
    public final void allNodesScan( NodeCursor cursor )
    {
        ktx.assertOpen();
        ((DefaultNodeCursor) cursor).scan( this );
    }

    @Override
    public final Scan<NodeCursor> allNodesScan()
    {
        ktx.assertOpen();
        return new NodeCursorScan( storageReader.allNodeScan(), this );
    }

    @Override
    public final void singleNode( long reference, NodeCursor cursor )
    {
        ktx.assertOpen();
        ((DefaultNodeCursor) cursor).single( reference, this );
    }

    @Override
    public final void singleRelationship( long reference, RelationshipScanCursor cursor )
    {
        ktx.assertOpen();
        ((DefaultRelationshipScanCursor) cursor).single( reference, this );
    }

    @Override
    public final void allRelationshipsScan( RelationshipScanCursor cursor )
    {
        ktx.assertOpen();
        ((DefaultRelationshipScanCursor) cursor).scan( -1/*include all labels*/, this );
    }

    @Override
    public final Scan<RelationshipScanCursor> allRelationshipsScan()
    {
        ktx.assertOpen();
        return new RelationshipCursorScan( storageReader.allRelationshipScan(), this );
    }

    @Override
    public final void relationshipTypeScan( int type, RelationshipScanCursor cursor )
    {
        ktx.assertOpen();
        ((DefaultRelationshipScanCursor) cursor).scan( type, this );
    }

    @Override
    public void relationshipGroups( long nodeReference, long reference, RelationshipGroupCursor cursor )
    {
        RelationshipReferenceEncoding encoding = RelationshipReferenceEncoding.parseEncoding( reference );
        switch ( encoding )
        {
        case NONE:
            // Reference was retrieved from NodeCursor#relationshipGroupReference() for a sparse node
            ((DefaultRelationshipGroupCursor) cursor).init( nodeReference, reference, false, this );
            break;
        case DENSE:
            // Reference was retrieved from NodeCursor#relationshipGroupReference() for a sparse node
            ((DefaultRelationshipGroupCursor) cursor).init( nodeReference, clearEncoding( reference ), true, this );
            break;
        default:
            throw new IllegalArgumentException( "Unexpected encoding " + encoding );
        }
    }

    @Override
    public void relationships( long nodeReference, long reference, RelationshipTraversalCursor cursor )
    {
        RelationshipReferenceEncoding encoding = RelationshipReferenceEncoding.parseEncoding( reference );
        switch ( encoding )
        {
        case NONE:
            // Reference was retrieved from NodeCursor#allRelationshipsReference() for a sparse node.
            ((DefaultRelationshipTraversalCursor) cursor).init( nodeReference, reference, false, this );
            break;
        case DENSE:
            // Reference was retrieved from NodeCursor#allRelationshipsReference() for a dense node
            ((DefaultRelationshipTraversalCursor) cursor).init( nodeReference, clearEncoding( reference ), true, this );
            break;
        case SELECTION:
            // Reference was retrieved from RelationshipGroupCursor#outgoingReference() or similar for a sparse node
            // Do lazy selection, i.e. discover type/direction from the first relationship read, so that it can be used to query tx-state.
            ((DefaultRelationshipTraversalCursor) cursor).init( nodeReference, clearEncoding( reference ), TokenRead.ANY_RELATIONSHIP_TYPE, null, false, this );
            break;
        case DENSE_SELECTION:
            // Reference was retrieved from RelationshipGroupCursor#outgoingReference() or similar for a dense node
            // Do lazy selection, i.e. discover type/direction from the first relationship read, so that it can be used to query tx-state.
            ((DefaultRelationshipTraversalCursor) cursor).init( nodeReference, clearEncoding( reference ), TokenRead.ANY_RELATIONSHIP_TYPE, null, true, this );
            break;
        case NO_OUTGOING_OF_TYPE:
            // Reference was retrieved from RelationshipGroupCursor#outgoingReference() where there were no relationships in store
            // and so therefore the type and direction was encoded into the reference instead
            ((DefaultRelationshipTraversalCursor) cursor).init( nodeReference, NO_ID, (int) reference, OUTGOING, true, this );
            break;
        case NO_INCOMING_OF_TYPE:
            // Reference was retrieved from RelationshipGroupCursor#incomingReference() where there were no relationships in store
            // and so therefore the type and direction was encoded into the reference instead
            ((DefaultRelationshipTraversalCursor) cursor).init( nodeReference, NO_ID, (int) reference, INCOMING, true, this );
            break;
        case NO_LOOPS_OF_TYPE:
            // Reference was retrieved from RelationshipGroupCursor#loopsReference() where there were no relationships in store
            // and so therefore the type and direction was encoded into the reference instead
            ((DefaultRelationshipTraversalCursor) cursor).init( nodeReference, NO_ID, (int) reference, LOOP, true, this );
            break;
        default:
            throw new IllegalArgumentException( "Unexpected encoding " + encoding );
        }
    }

    @Override
    public void nodeProperties( long nodeReference, long reference, PropertyCursor cursor )
    {
        ((DefaultPropertyCursor) cursor).initNode( nodeReference, reference, this, ktx );
    }

    @Override
    public void relationshipProperties( long relationshipReference, long reference, PropertyCursor cursor )
    {
        ((DefaultPropertyCursor) cursor).initRelationship( relationshipReference, reference, this, ktx );
    }

    public abstract IndexReader indexReader( IndexDescriptor index, boolean fresh ) throws IndexNotFoundKernelException;

    abstract LabelScanReader labelScanReader();

    @Override
    public TransactionState txState()
    {
        return ktx.txState();
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
    public void releaseSharedLabelLock( long... ids )
    {
        releaseSharedLock( ResourceTypes.LABEL, ids );
        ktx.assertOpen();
    }

    <T extends SchemaDescriptorSupplier> T acquireSharedSchemaLock( T schemaLike )
    {
        SchemaDescriptor schema = schemaLike.schema();
        long[] lockingKeys = schema.lockingKeys();
        ktx.statementLocks().optimistic().acquireShared( ktx.lockTracer(), schema.keyType(), lockingKeys );
        return schemaLike;
    }

    <T extends SchemaDescriptorSupplier> void releaseSharedSchemaLock( T schemaLike )
    {
        SchemaDescriptor schema = schemaLike.schema();
        long[] lockingKeys = schema.lockingKeys();
        ktx.statementLocks().optimistic().releaseShared( schema.keyType(), lockingKeys );
    }

    void acquireSharedLock( ResourceType resource, long resourceId )
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

    private void assertIndexOnline( IndexDescriptor index )
            throws IndexNotFoundKernelException, IndexBrokenKernelException
    {
        if ( indexGetState( index ) == InternalIndexState.ONLINE )
        {
            return;
        }
        throw new IndexBrokenKernelException( indexGetFailure( index ) );
    }

    private static void assertPredicatesMatchSchema( IndexDescriptor index, IndexQuery.ExactPredicate[] predicates )
            throws IndexNotApplicableKernelException
    {
        int[] propertyIds = index.schema().getPropertyIds();
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
    public void assertOpen()
    {
        ktx.assertOpen();
    }
}
