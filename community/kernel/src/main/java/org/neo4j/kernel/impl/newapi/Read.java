/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import org.neo4j.configuration.Config;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.RelationshipTypeIndexCursor;
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor;
import org.neo4j.internal.kernel.api.Scan;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptorSupplier;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.AssertOpen;
import org.neo4j.kernel.api.exceptions.schema.IndexBrokenKernelException;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.index.schema.RelationshipTypeScanStoreSettings;
import org.neo4j.kernel.impl.index.schema.TokenScan;
import org.neo4j.kernel.impl.index.schema.TokenScanReader;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.ResourceType;
import org.neo4j.lock.ResourceTypes;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;

import static java.lang.String.format;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;

abstract class Read implements TxStateHolder,
        org.neo4j.internal.kernel.api.Read,
        org.neo4j.internal.kernel.api.SchemaRead,
        org.neo4j.internal.kernel.api.Procedures,
        org.neo4j.internal.kernel.api.Locks,
        AssertOpen,
        LockingNodeUniqueIndexSeek.UniqueNodeIndexSeeker<DefaultNodeValueIndexCursor>,
        QueryContext
{
    protected final StorageReader storageReader;
    protected final DefaultPooledCursors cursors;
    protected final PageCursorTracer cursorTracer;
    final KernelTransactionImplementation ktx;
    private final boolean relationshipTypeScanStoreEnabled;

    Read( StorageReader storageReader, DefaultPooledCursors cursors, PageCursorTracer cursorTracer,
            KernelTransactionImplementation ktx, Config config )
    {
        this.storageReader = storageReader;
        this.cursors = cursors;
        this.cursorTracer = cursorTracer;
        this.ktx = ktx;
        this.relationshipTypeScanStoreEnabled = config.get( RelationshipTypeScanStoreSettings.enable_relationship_type_scan_store );
    }

    @Override
    public final void nodeIndexSeek( IndexReadSession index, NodeValueIndexCursor cursor, IndexQueryConstraints constraints, IndexQuery... query )
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
        indexSession.reader.query( this, client, constraints, query );
    }

    @Override
    public final void relationshipIndexSeek( IndexReadSession index, RelationshipValueIndexCursor cursor, IndexQueryConstraints constraints,
            IndexQuery... query ) throws IndexNotApplicableKernelException
    {
        ktx.assertOpen();
        DefaultIndexReadSession indexSession = (DefaultIndexReadSession) index;
        if ( indexSession.reference.schema().entityType() != EntityType.RELATIONSHIP )
        {
            throw new IndexNotApplicableKernelException( "Relationship index seek can only be performed on relationship indexes: " + index );
        }

        EntityIndexSeekClient client = (EntityIndexSeekClient) cursor;
        client.setRead( this );
        indexSession.reader.query( this, client, constraints, query );
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
        indexReader.query( this, cursor, unconstrained(), query );
    }

    @Override
    public final void nodeIndexScan( IndexReadSession index,
                                     NodeValueIndexCursor cursor,
                                     IndexQueryConstraints constraints ) throws KernelException
    {
        ktx.assertOpen();
        DefaultIndexReadSession indexSession = (DefaultIndexReadSession) index;

        if ( indexSession.reference.schema().entityType() != EntityType.NODE )
        {
            throw new IndexNotApplicableKernelException( "Node index scan can only be performed on node indexes: " + index );
        }

        scanIndex( indexSession, (EntityIndexSeekClient) cursor, constraints );
    }

    @Override
    public final void relationshipIndexScan( IndexReadSession index,
            RelationshipValueIndexCursor cursor,
            IndexQueryConstraints constraints ) throws KernelException
    {
        ktx.assertOpen();
        DefaultIndexReadSession indexSession = (DefaultIndexReadSession) index;

        if ( indexSession.reference.schema().entityType() != EntityType.RELATIONSHIP )
        {
            throw new IndexNotApplicableKernelException( "Relationship index scan can only be performed on relationship indexes: " + index );
        }

        scanIndex( indexSession, (EntityIndexSeekClient) cursor, constraints );
    }

    private void scanIndex( DefaultIndexReadSession indexSession,
            EntityIndexSeekClient indexSeekClient,
            IndexQueryConstraints constraints ) throws KernelException
    {
        // for a scan, we simply query for existence of the first property, which covers all entries in an index
        int firstProperty = indexSession.reference.schema().getPropertyIds()[0];

        indexSeekClient.setRead( this );
        indexSession.reader.query( this, indexSeekClient, constraints, IndexQuery.exists( firstProperty ) );
    }

    @Override
    public final void nodeLabelScan( int label, NodeLabelIndexCursor cursor, IndexOrder order )
    {
        ktx.assertOpen();

        DefaultNodeLabelIndexCursor indexCursor = (DefaultNodeLabelIndexCursor) cursor;
        indexCursor.setRead( this );
        TokenScan labelScan = labelScanReader().entityTokenScan( label, cursorTracer );
        indexCursor.scan( labelScan.initialize( indexCursor.entityTokenClient(), order, cursorTracer ), label, order );
    }

    @Override
    public final Scan<NodeLabelIndexCursor> nodeLabelScan( int label )
    {
        ktx.assertOpen();
        return new NodeLabelIndexCursorScan( this, label, labelScanReader().entityTokenScan( label, cursorTracer ), cursorTracer );
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
        return new NodeCursorScan( storageReader.allNodeScan(), this, cursorTracer );
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
        return new RelationshipCursorScan( storageReader.allRelationshipScan(), this, cursorTracer );
    }

    @Override
    public final void relationshipTypeScan( int type, RelationshipScanCursor cursor )
    {
        ktx.assertOpen();
        ((DefaultRelationshipScanCursor) cursor).scan( type, this );
    }

    @Override
    public final void relationshipTypeScan( int type, RelationshipTypeIndexCursor relationshipTypeIndexCursor, IndexOrder order )
    {
        ktx.assertOpen();
        if ( relationshipTypeScanStoreEnabled() )
        {
            DefaultRelationshipTypeIndexCursor cursor = (DefaultRelationshipTypeIndexCursor)relationshipTypeIndexCursor;
            cursor.setRead( this );

            TokenScanReader relationshipTypeScanReader = relationshipTypeScanReader();
            TokenScan relationshipTypeScan = relationshipTypeScanReader.entityTokenScan( type, cursorTracer );
            IndexProgressor progressor = relationshipTypeScan.initialize( cursor.entityTokenClient(), order, cursorTracer );

            cursor.scan( progressor, type, order );
        }
        else
        {
            throw new IllegalStateException( "Cannot search relationship type scan store when feature is not enabled." );
        }
    }

    @Override
    public void relationships( long nodeReference, long reference, RelationshipSelection selection, RelationshipTraversalCursor cursor )
    {
        ((DefaultRelationshipTraversalCursor) cursor).init( nodeReference, reference, selection, this );
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

    abstract TokenScanReader labelScanReader();

    abstract TokenScanReader relationshipTypeScanReader();

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

    @Override
    public boolean relationshipTypeScanStoreEnabled()
    {
        return relationshipTypeScanStoreEnabled;
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
