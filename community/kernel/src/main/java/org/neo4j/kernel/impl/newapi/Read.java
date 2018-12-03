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

import org.neo4j.common.EntityType;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.IndexReference;
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
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.kernel.api.AssertOpen;
import org.neo4j.kernel.api.exceptions.schema.IndexBrokenKernelException;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.labelscan.LabelScan;
import org.neo4j.kernel.api.labelscan.LabelScanReader;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.lock.LockTracer;
import org.neo4j.storageengine.api.lock.ResourceType;
import org.neo4j.storageengine.api.schema.SchemaDescriptor;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.Values;

import static java.lang.String.format;
import static org.neo4j.kernel.impl.newapi.RelationshipReferenceEncoding.clearEncoding;
import static org.neo4j.storageengine.api.RelationshipDirection.INCOMING;
import static org.neo4j.storageengine.api.RelationshipDirection.LOOP;
import static org.neo4j.storageengine.api.RelationshipDirection.OUTGOING;
import static org.neo4j.storageengine.api.schema.SchemaDescriptor.schemaTokenLockingIds;
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
    static final int NO_ID = -1;

    private final StorageReader storageReader;
    private final DefaultPooledCursors cursors;
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
            throws IndexNotApplicableKernelException, IndexNotFoundKernelException
    {
        ktx.assertOpen();
        DefaultIndexReadSession indexSession = (DefaultIndexReadSession) index;
        if ( hasForbiddenProperties( indexSession.reference ) )
        {
            cursor.close();
            return;
        }
        if ( indexSession.reference.schema().entityType() != EntityType.NODE )
        {
            throw new IndexNotApplicableKernelException( "Node index seek can only be performed on node indexes: " + index );
        }

        EntityIndexSeekClient client = (EntityIndexSeekClient) cursor;
        client.setRead( this, null );
        IndexProgressor.EntityValueClient withFullPrecision = injectFullValuePrecision( client, query, indexSession.reader );
        indexSession.reader.query( this, withFullPrecision, indexOrder, needsValues, query );
    }

    @Override
    public final void relationshipIndexSeek( IndexReference index, RelationshipIndexCursor cursor, IndexQuery... query )
            throws IndexNotApplicableKernelException, IndexNotFoundKernelException
    {
        ktx.assertOpen();
        if ( hasForbiddenProperties( index ) )
        {
            cursor.close();
            return;
        }
        if ( index.schema().entityType() != EntityType.RELATIONSHIP )
        {
            throw new IndexNotApplicableKernelException( "Relationship index seek can only be performed on node indexes: " + index );
        }

        EntityIndexSeekClient client = (EntityIndexSeekClient) cursor;
        IndexReader reader = indexReader( index, false );
        client.setRead( this, null );
        IndexProgressor.EntityValueClient withFullPrecision = injectFullValuePrecision( client, query, reader );
        reader.query( this, withFullPrecision, IndexOrder.NONE, false, query );
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
    public long lockingNodeUniqueIndexSeek( IndexReference index,
                                            NodeValueIndexCursor cursor,
                                            IndexQuery.ExactPredicate... predicates )
            throws IndexNotApplicableKernelException, IndexNotFoundKernelException, IndexBrokenKernelException
    {
        assertIndexOnline( index );
        assertPredicatesMatchSchema( index, predicates );

        Locks.Client locks = ktx.statementLocks().optimistic();
        LockTracer lockTracer = ktx.lockTracer();

        return LockingNodeUniqueIndexSeek.apply( locks, lockTracer, (DefaultNodeValueIndexCursor)cursor, this, index, predicates );
    }

    @Override // UniqueNodeIndexSeeker
    public void nodeIndexSeekWithFreshIndexReader(
            IndexReference index,
            DefaultNodeValueIndexCursor cursor,
            IndexQuery.ExactPredicate... query ) throws IndexNotFoundKernelException, IndexNotApplicableKernelException
    {
        IndexReader reader = indexReader( index, true );
        cursor.setRead( this, reader );
        IndexProgressor.EntityValueClient target = injectFullValuePrecision( cursor, query, reader );
        // we never need values for exact predicates
        reader.query( this, target, IndexOrder.NONE, false, query );
    }

    @Override
    public final void nodeIndexScan( IndexReadSession index,
                                     NodeValueIndexCursor cursor,
                                     IndexOrder indexOrder,
                                     boolean needsValues ) throws KernelException
    {
        ktx.assertOpen();
        DefaultIndexReadSession indexSession = (DefaultIndexReadSession) index;
        if ( hasForbiddenProperties( indexSession.reference ) )
        {
            cursor.close();
            return;
        }

        // for a scan, we simply query for existence of the first property, which covers all entries in an index
        int firstProperty = indexSession.reference.properties()[0];

        DefaultNodeValueIndexCursor cursorImpl = (DefaultNodeValueIndexCursor) cursor;
        cursorImpl.setRead( this, null );
        indexSession.reader.query( this, cursorImpl, indexOrder, needsValues, IndexQuery.exists( firstProperty ) );
    }

    private boolean hasForbiddenProperties( IndexReference index )
    {
        AccessMode mode = ktx.securityContext().mode();
        for ( int prop : index.properties() )
        {
            if ( !mode.allowsPropertyReads( prop ) )
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public final void nodeLabelScan( int label, NodeLabelIndexCursor cursor )
    {
        ktx.assertOpen();

        DefaultNodeLabelIndexCursor indexCursor = (DefaultNodeLabelIndexCursor) cursor;
        indexCursor.setRead( this );
        LabelScan labelScan = labelScanReader().nodeLabelScan( label );
        indexCursor.scan( labelScan.initialize( indexCursor ), label );
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
    public final Scan<RelationshipScanCursor> relationshipTypeScan( int type )
    {
        ktx.assertOpen();
        throw new UnsupportedOperationException( "not implemented" );
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
            ((DefaultRelationshipTraversalCursor) cursor).init( nodeReference, clearEncoding( reference ), NO_ID, null, false, this );
            break;
        case DENSE_SELECTION:
            // Reference was retrieved from RelationshipGroupCursor#outgoingReference() or similar for a dense node
            // Do lazy selection, i.e. discover type/direction from the first relationship read, so that it can be used to query tx-state.
            ((DefaultRelationshipTraversalCursor) cursor).init( nodeReference, clearEncoding( reference ), NO_ID, null, true, this );
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
    public final void graphProperties( PropertyCursor cursor )
    {
        ktx.assertOpen();
        ((DefaultPropertyCursor) cursor).initGraph( this, ktx );
    }

    public abstract IndexReader indexReader( IndexReference index, boolean fresh ) throws IndexNotFoundKernelException;

    abstract LabelScanReader labelScanReader();

    @Override
    public abstract IndexReference index( int label, int... properties );

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

    void acquireSharedSchemaLock( SchemaDescriptor schema )
    {
        long[] lockingIds = schemaTokenLockingIds( schema );
        ktx.statementLocks().optimistic().acquireShared( ktx.lockTracer(), schema.keyType(), lockingIds );
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

    private static void assertPredicatesMatchSchema( IndexReference index, IndexQuery.ExactPredicate[] predicates )
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
    public void assertOpen()
    {
        ktx.assertOpen();
    }
}
