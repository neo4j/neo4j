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

import java.util.Arrays;

import org.neo4j.internal.kernel.api.CapableIndexReference;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeExplicitIndexCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipExplicitIndexCursor;
import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.Scan;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.explicitindex.ExplicitIndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.api.ExplicitIndex;
import org.neo4j.kernel.api.ExplicitIndexHits;
import org.neo4j.kernel.api.exceptions.index.IndexNotApplicableKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.IndexBrokenKernelException;
import org.neo4j.kernel.api.txstate.ExplicitIndexTransactionState;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.locking.LockTracer;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.kernel.impl.store.RecordCursor;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.storageengine.api.lock.ResourceType;
import org.neo4j.storageengine.api.schema.IndexProgressor;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.LabelScanReader;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.Values;

import static java.lang.String.format;
import static org.neo4j.kernel.impl.locking.ResourceTypes.INDEX_ENTRY;
import static org.neo4j.kernel.impl.locking.ResourceTypes.indexEntryResourceId;
import static org.neo4j.kernel.impl.newapi.GroupReferenceEncoding.isRelationship;
import static org.neo4j.kernel.impl.newapi.References.clearEncoding;
import static org.neo4j.kernel.impl.newapi.RelationshipDirection.INCOMING;
import static org.neo4j.kernel.impl.newapi.RelationshipDirection.LOOP;
import static org.neo4j.kernel.impl.newapi.RelationshipDirection.OUTGOING;
import static org.neo4j.kernel.impl.store.record.AbstractBaseRecord.NO_ID;
import static org.neo4j.values.storable.ValueGroup.GEOMETRY;
import static org.neo4j.values.storable.ValueGroup.NUMBER;

abstract class Read implements TxStateHolder,
        org.neo4j.internal.kernel.api.Read,
        org.neo4j.internal.kernel.api.ExplicitIndexRead,
        org.neo4j.internal.kernel.api.SchemaRead,
        org.neo4j.internal.kernel.api.Procedures,
        org.neo4j.internal.kernel.api.Locks
{
    private final DefaultCursors cursors;
    final KernelTransactionImplementation ktx;

    Read( DefaultCursors cursors, KernelTransactionImplementation ktx )
    {
        this.cursors = cursors;
        this.ktx = ktx;
    }

    @Override
    public final void nodeIndexSeek(
            IndexReference index,
            NodeValueIndexCursor cursor,
            IndexOrder indexOrder,
            IndexQuery... query ) throws IndexNotApplicableKernelException, IndexNotFoundKernelException
    {
        ktx.assertOpen();
        if ( hasForbiddenProperties( index ) )
        {
            cursor.close();
            return;
        }

        DefaultNodeValueIndexCursor cursorImpl = (DefaultNodeValueIndexCursor) cursor;
        IndexReader reader = indexReader( index, false );
        cursorImpl.setRead( this );
        IndexProgressor.NodeValueClient target = withFullValuePrecision( cursorImpl, query, reader );
        reader.query( target, indexOrder, query );
    }

    @Override
    public void nodeIndexDistinctValues( IndexReference index, NodeValueIndexCursor cursor ) throws IndexNotFoundKernelException
    {
        ktx.assertOpen();
        DefaultNodeValueIndexCursor cursorImpl = (DefaultNodeValueIndexCursor) cursor;
        IndexReader reader = indexReader( index, true );
        cursorImpl.setRead( this );
        try ( CursorPropertyAccessor accessor = new CursorPropertyAccessor( cursors.allocateNodeCursor(), cursors.allocatePropertyCursor(), this ) )
        {
            reader.distinctValues( cursorImpl, accessor );
        }
    }

    private IndexProgressor.NodeValueClient withFullValuePrecision( DefaultNodeValueIndexCursor cursor,
            IndexQuery[] query, IndexReader reader )
    {
        IndexProgressor.NodeValueClient target = cursor;
        if ( !reader.hasFullValuePrecision( query ) )
        {
            IndexQuery[] filters = new IndexQuery[query.length];
            int j = 0;
            for ( IndexQuery q : query )
            {
                switch ( q.type() )
                {
                case range:
                    ValueGroup valueGroup = q.valueGroup();
                    if ( ( valueGroup == NUMBER || valueGroup == GEOMETRY) && !reader.hasFullValuePrecision( q ) )
                    {
                        filters[j++] = q;
                    }
                    break;
                case exact:
                    Value value = ((IndexQuery.ExactPredicate) q).value();
                    if ( value.valueGroup() == ValueGroup.NUMBER || Values.isArrayValue( value ) || value.valueGroup() == ValueGroup.GEOMETRY )
                    {
                        if ( !reader.hasFullValuePrecision( q ) )
                        {
                            filters[j++] = q;
                        }
                    }
                    break;
                default:
                    break;
                }
            }
            if ( j > 0 )
            {
                filters = Arrays.copyOf( filters, j );
                target = new NodeValueClientFilter( target, cursors.allocateNodeCursor(),
                        cursors.allocatePropertyCursor(), this, filters );
            }
        }
        return target;
    }

    @Override
    public final long lockingNodeUniqueIndexSeek(
            IndexReference index,
            IndexQuery.ExactPredicate... predicates )
            throws IndexNotApplicableKernelException, IndexNotFoundKernelException, IndexBrokenKernelException
    {
        assertIndexOnline( index );
        assertPredicatesMatchSchema( index, predicates );
        int labelId = index.label();

        Locks.Client locks = ktx.statementLocks().optimistic();
        LockTracer lockTracer = ktx.lockTracer();
        long indexEntryId = indexEntryResourceId( labelId, predicates );

        //First try to find node under a shared lock
        //if not found upgrade to exclusive and try again
        locks.acquireShared( lockTracer, INDEX_ENTRY, indexEntryId );
        try ( DefaultNodeValueIndexCursor cursor = cursors.allocateNodeValueIndexCursor();
              IndexReaders readers = new IndexReaders( index, this ) )
        {
            nodeIndexSeekWithFreshIndexReader( cursor, readers.createReader(), predicates );
            if ( !cursor.next() )
            {
                locks.releaseShared( INDEX_ENTRY, indexEntryId );
                locks.acquireExclusive( lockTracer, INDEX_ENTRY, indexEntryId );
                nodeIndexSeekWithFreshIndexReader( cursor, readers.createReader(), predicates );
                if ( cursor.next() ) // we found it under the exclusive lock
                {
                    // downgrade to a shared lock
                    locks.acquireShared( lockTracer, INDEX_ENTRY, indexEntryId );
                    locks.releaseExclusive( INDEX_ENTRY, indexEntryId );
                }
            }

            return cursor.nodeReference();
        }
    }

    void nodeIndexSeekWithFreshIndexReader(
            DefaultNodeValueIndexCursor cursor,
            IndexReader indexReader,
            IndexQuery.ExactPredicate... query ) throws IndexNotApplicableKernelException
    {
        cursor.setRead( this );
        IndexProgressor.NodeValueClient target = withFullValuePrecision( cursor, query, indexReader );
        indexReader.query( target, IndexOrder.NONE, query );
    }

    @Override
    public final void nodeIndexScan(
            IndexReference index,
            NodeValueIndexCursor cursor,
            IndexOrder indexOrder ) throws KernelException
    {
        ktx.assertOpen();
        if ( hasForbiddenProperties( index ) )
        {
            cursor.close();
            return;
        }

        // for a scan, we simply query for existence of the first property, which covers all entries in an index
        int firstProperty = index.properties()[0];
        ((DefaultNodeValueIndexCursor) cursor).setRead( this );
        indexReader( index, false ).query( (DefaultNodeValueIndexCursor) cursor, indexOrder, IndexQuery.exists( firstProperty ) );
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
        labelScanReader().nodesWithLabel( indexCursor, label);
    }

    @Override
    public void nodeLabelUnionScan( NodeLabelIndexCursor cursor, int... labels )
    {
        ktx.assertOpen();

        DefaultNodeLabelIndexCursor client = (DefaultNodeLabelIndexCursor) cursor;
        client.setRead( this );
        client.unionScan( new NodeLabelIndexProgressor( labelScanReader().nodesWithAnyOfLabels( labels ), client ),
                false, labels );
    }

    @Override
    public void nodeLabelIntersectionScan( NodeLabelIndexCursor cursor, int... labels )
    {
        ktx.assertOpen();

        DefaultNodeLabelIndexCursor client = (DefaultNodeLabelIndexCursor) cursor;
        client.setRead( this );
        client.intersectionScan(
                new NodeLabelIndexProgressor( labelScanReader().nodesWithAllLabels( labels ), client ),
                false, labels );
    }

    @Override
    public final Scan<NodeLabelIndexCursor> nodeLabelScan( int label )
    {
        ktx.assertOpen();
        throw new UnsupportedOperationException( "not implemented" );
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
        throw new UnsupportedOperationException( "not implemented" );
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
        throw new UnsupportedOperationException( "not implemented" );
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
    public final void relationshipGroups(
            long nodeReference, long reference, RelationshipGroupCursor cursor )
    {
        ktx.assertOpen();
        // the relationships for this node are not grouped in the store
        if ( reference != NO_ID && isRelationship( reference ) )
        {
            ((DefaultRelationshipGroupCursor) cursor).buffer( nodeReference, clearEncoding( reference ), this );
        }
        else // this is a normal group reference.
        {
            ((DefaultRelationshipGroupCursor) cursor).direct( nodeReference, reference, this );
        }
    }

    @Override
    public final void relationships(
            long nodeReference, long reference, RelationshipTraversalCursor cursor )
    {
        /* There are 5 different ways a relationship traversal cursor can be initialized:
         *
         * 1. From a batched group in a detached way. This happens when the user manually retrieves the relationships
         *    references from the group cursor and passes it to this method and if the group cursor was based on having
         *    batched all the different types in the single (mixed) chain of relationships.
         *    In this case we should pass a reference marked with some flag to the first relationship in the chain that
         *    has the type of the current group in the group cursor. The traversal cursor then needs to read the type
         *    from that first record and use that type as a filter for when reading the rest of the chain.
         *    - NOTE: we probably have to do the same sort of filtering for direction - so we need a flag for that too.
         *
         * 2. From a batched group in a DIRECT way. This happens when the traversal cursor is initialized directly from
         *    the group cursor, in this case we can simply initialize the traversal cursor with the buffered state from
         *    the group cursor, so this method here does not have to be involved, and things become pretty simple.
         *
         * 3. Traversing all relationships - regardless of type - of a node that has grouped relationships. In this case
         *    the traversal cursor needs to traverse through the group records in order to get to the actual
         *    relationships. The initialization of the cursor (through this here method) should be with a FLAGGED
         *    reference to the (first) group record.
         *
         * 4. Traversing a single chain - this is what happens in the cases when
         *    a) Traversing all relationships of a node without grouped relationships.
         *    b) Traversing the relationships of a particular group of a node with grouped relationships.
         *
         * 5. There are no relationships - i.e. passing in NO_ID to this method.
         *
         * This means that we need reference encodings (flags) for cases: 1, 3, 4, 5
         */
        ktx.assertOpen();

        int relationshipType;
        RelationshipReferenceEncoding encoding = RelationshipReferenceEncoding.parseEncoding( reference );
        DefaultRelationshipTraversalCursor internalCursor = (DefaultRelationshipTraversalCursor)cursor;

        switch ( encoding )
        {
        case NONE: // this is a normal relationship reference
            internalCursor.chain( nodeReference, reference, this );
            break;

        case FILTER: // this relationship chain needs to be filtered
            internalCursor.filtered( nodeReference, clearEncoding( reference ), this, true );
            break;

        case FILTER_TX_STATE: // tx-state changes should be filtered by the head of this chain
            internalCursor.filtered( nodeReference, clearEncoding( reference ), this, false );
            break;

        case GROUP: // this reference is actually to a group record
            internalCursor.groups( nodeReference, clearEncoding( reference ), this );
            break;

        case NO_OUTGOING_OF_TYPE: // nothing in store, but proceed to check tx-state changes
            relationshipType = (int) clearEncoding( reference );
            internalCursor.filteredTxState( nodeReference, this, relationshipType, OUTGOING );
            break;

        case NO_INCOMING_OF_TYPE: // nothing in store, but proceed to check tx-state changes
            relationshipType = (int) clearEncoding( reference );
            internalCursor.filteredTxState( nodeReference, this, relationshipType, INCOMING );
            break;

        case NO_LOOP_OF_TYPE: // nothing in store, but proceed to check tx-state changes
            relationshipType = (int) clearEncoding( reference );
            internalCursor.filteredTxState( nodeReference, this, relationshipType, LOOP );
            break;

        default:
            throw new IllegalStateException( "Unknown encoding " + encoding );
        }
    }

    @Override
    public final void nodeProperties( long nodeReference, long reference, PropertyCursor cursor )
    {
        ktx.assertOpen();
        ((DefaultPropertyCursor) cursor).initNode( nodeReference, reference, this, ktx );
    }

    @Override
    public final void relationshipProperties( long relationshipReference, long reference,
            PropertyCursor cursor )
    {
        ktx.assertOpen();
        ((DefaultPropertyCursor) cursor).initRelationship( relationshipReference, reference, this, ktx );
    }

    @Override
    public final void graphProperties( PropertyCursor cursor )
    {
        ktx.assertOpen();
        ((DefaultPropertyCursor) cursor).initGraph( graphPropertiesReference(), this, ktx );
    }

    abstract long graphPropertiesReference();

    @Override
    public final void nodeExplicitIndexLookup(
            NodeExplicitIndexCursor cursor, String index, String key, Object value )
            throws ExplicitIndexNotFoundKernelException
    {
        ktx.assertOpen();
        ((DefaultNodeExplicitIndexCursor) cursor).setRead( this );
        explicitIndex( (DefaultNodeExplicitIndexCursor) cursor,
                explicitNodeIndex( index ).get( key, value ) );
    }

    @Override
    public final void nodeExplicitIndexQuery(
            NodeExplicitIndexCursor cursor, String index, Object query )
            throws ExplicitIndexNotFoundKernelException
    {
        ktx.assertOpen();
        ((DefaultNodeExplicitIndexCursor) cursor).setRead( this );
        explicitIndex( (DefaultNodeExplicitIndexCursor) cursor, explicitNodeIndex( index ).query(
                query instanceof Value ? ((Value) query).asObject() : query ) );
    }

    @Override
    public final void nodeExplicitIndexQuery(
            NodeExplicitIndexCursor cursor, String index, String key, Object query )
            throws ExplicitIndexNotFoundKernelException
    {
        ktx.assertOpen();
        ((DefaultNodeExplicitIndexCursor) cursor).setRead( this );
        explicitIndex( (DefaultNodeExplicitIndexCursor) cursor, explicitNodeIndex( index ).query(
                key, query instanceof Value ? ((Value) query).asObject() : query ) );
    }

    @Override
    public void relationshipExplicitIndexLookup(
            RelationshipExplicitIndexCursor cursor,
            String index,
            String key,
            Object value,
            long source,
            long target ) throws ExplicitIndexNotFoundKernelException
    {
        ktx.assertOpen();
        ((DefaultRelationshipExplicitIndexCursor) cursor).setRead( this );
        explicitIndex(
                (DefaultRelationshipExplicitIndexCursor) cursor,
                explicitRelationshipIndex( index ).get( key, value, source, target ) );
    }

    @Override
    public void relationshipExplicitIndexQuery(
            RelationshipExplicitIndexCursor cursor,
            String index,
            Object query,
            long source,
            long target ) throws ExplicitIndexNotFoundKernelException
    {
        ktx.assertOpen();
        ((DefaultRelationshipExplicitIndexCursor) cursor).setRead( this );
        explicitIndex(
                (DefaultRelationshipExplicitIndexCursor) cursor,
                explicitRelationshipIndex( index )
                        .query( query instanceof Value ? ((Value) query).asObject() : query, source, target ) );
    }

    @Override
    public void relationshipExplicitIndexQuery(
            RelationshipExplicitIndexCursor cursor,
            String index,
            String key,
            Object query,
            long source,
            long target ) throws ExplicitIndexNotFoundKernelException
    {
        ktx.assertOpen();
        ((DefaultRelationshipExplicitIndexCursor) cursor).setRead( this );
        explicitIndex(
                (DefaultRelationshipExplicitIndexCursor) cursor,
                explicitRelationshipIndex( index ).query(
                        key, query instanceof Value ? ((Value) query).asObject() : query, source, target ) );
    }

    private static void explicitIndex( IndexProgressor.ExplicitClient client, ExplicitIndexHits hits )
    {
        client.initialize( new ExplicitIndexProgressor( hits, client ), hits.size() );
    }

    @Override
    public final void futureNodeReferenceRead( long reference )
    {
        ktx.assertOpen();
    }

    @Override
    public final void futureRelationshipsReferenceRead( long reference )
    {
        ktx.assertOpen();
    }

    @Override
    public final void futureNodePropertyReferenceRead( long reference )
    {
        ktx.assertOpen();
    }

    @Override
    public final void futureRelationshipPropertyReferenceRead( long reference )
    {
        ktx.assertOpen();
    }

    abstract IndexReader indexReader( IndexReference index, boolean fresh ) throws IndexNotFoundKernelException;

    abstract LabelScanReader labelScanReader();

    abstract ExplicitIndex explicitNodeIndex( String indexName ) throws ExplicitIndexNotFoundKernelException;

    abstract ExplicitIndex explicitRelationshipIndex( String indexName ) throws ExplicitIndexNotFoundKernelException;

    @Override
    public abstract CapableIndexReference index( int label, int... properties );

    abstract PageCursor nodePage( long reference );

    abstract PageCursor relationshipPage( long reference );

    abstract PageCursor groupPage( long reference );

    abstract PageCursor propertyPage( long reference );

    abstract PageCursor stringPage( long reference );

    abstract PageCursor arrayPage( long reference );

    abstract RecordCursor<DynamicRecord> labelCursor();

    abstract void node( NodeRecord record, long reference, PageCursor pageCursor );

    abstract void relationship( RelationshipRecord record, long reference, PageCursor pageCursor );

    abstract void relationshipFull( RelationshipRecord record, long reference, PageCursor pageCursor );

    abstract void property( PropertyRecord record, long reference, PageCursor pageCursor );

    abstract void group( RelationshipGroupRecord record, long reference, PageCursor page );

    abstract long nodeHighMark();

    abstract long relationshipHighMark();

    abstract TextValue string( DefaultPropertyCursor cursor, long reference, PageCursor page );

    abstract ArrayValue array( DefaultPropertyCursor cursor, long reference, PageCursor page );

    @Override
    public TransactionState txState()
    {
        return ktx.txState();
    }

    @Override
    public ExplicitIndexTransactionState explicitIndexTxState()
    {
        return ktx.explicitIndexTxState();
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

    void sharedOptimisticLock( ResourceType resource, long resourceId )
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

    private void assertPredicatesMatchSchema( IndexReference index, IndexQuery.ExactPredicate[] predicates )
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
}
