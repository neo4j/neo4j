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

import java.util.Arrays;

import org.neo4j.internal.kernel.api.CapableIndexReference;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.Scan;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.api.ExplicitIndex;
import org.neo4j.kernel.api.ExplicitIndexHits;
import org.neo4j.kernel.api.exceptions.index.IndexNotApplicableKernelException;
import org.neo4j.kernel.api.txstate.ExplicitIndexTransactionState;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
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

import static org.neo4j.kernel.impl.newapi.GroupReferenceEncoding.isRelationship;
import static org.neo4j.kernel.impl.newapi.References.clearEncoding;
import static org.neo4j.kernel.impl.newapi.RelationshipDirection.INCOMING;
import static org.neo4j.kernel.impl.newapi.RelationshipDirection.LOOP;
import static org.neo4j.kernel.impl.newapi.RelationshipDirection.OUTGOING;
import static org.neo4j.kernel.impl.store.record.AbstractBaseRecord.NO_ID;

abstract class Read implements TxStateHolder,
        org.neo4j.internal.kernel.api.Read,
        org.neo4j.internal.kernel.api.ExplicitIndexRead,
        org.neo4j.internal.kernel.api.SchemaRead
{
    private final Cursors cursors;
    final KernelTransactionImplementation ktx;

    Read( Cursors cursors, KernelTransactionImplementation ktx )
    {
        this.cursors = cursors;
        this.ktx = ktx;
    }

    @Override
    public final void nodeIndexSeek(
            org.neo4j.internal.kernel.api.IndexReference index,
            org.neo4j.internal.kernel.api.NodeValueIndexCursor cursor,
            IndexOrder indexOrder,
            IndexQuery... query ) throws IndexNotApplicableKernelException
    {
        ktx.assertOpen();

        ((NodeValueIndexCursor) cursor).setRead( this );
        IndexProgressor.NodeValueClient target = (NodeValueIndexCursor) cursor;
        IndexReader reader = indexReader( index );
        if ( !reader.hasFullNumberPrecision( query ) )
        {
            IndexQuery[] filters = new IndexQuery[query.length];
            int j = 0;
            for ( IndexQuery q : query )
            {
                switch ( q.type() )
                {
                case rangeNumeric:
                    if ( !reader.hasFullNumberPrecision( q ) )
                    {
                        filters[j++] = q;
                    }
                    break;
                case exact:
                    Value value = ((IndexQuery.ExactPredicate) q).value();
                    if ( value.valueGroup() == ValueGroup.NUMBER )
                    {
                        if ( !reader.hasFullNumberPrecision( q ) )
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
        reader.query( target, indexOrder, query );
    }

    @Override
    public final void nodeIndexScan(
            org.neo4j.internal.kernel.api.IndexReference index,
            org.neo4j.internal.kernel.api.NodeValueIndexCursor cursor,
            IndexOrder indexOrder ) throws KernelException
    {
        ktx.assertOpen();

        // for a scan, we simply query for existence of the first property, which covers all entries in an index
        int firstProperty = index.properties()[0];
        ((NodeValueIndexCursor) cursor).setRead( this );
        indexReader( index ).query( (NodeValueIndexCursor) cursor, indexOrder, IndexQuery.exists( firstProperty ) );
    }

    @Override
    public final void nodeLabelScan( int label, org.neo4j.internal.kernel.api.NodeLabelIndexCursor cursor )
    {
        ktx.assertOpen();

        NodeLabelIndexCursor indexCursor = (NodeLabelIndexCursor) cursor;
        indexCursor.setRead( this );
        labelScanReader().nodesWithLabel( indexCursor, label);
    }

    @Override
    public void nodeLabelUnionScan( org.neo4j.internal.kernel.api.NodeLabelIndexCursor cursor, int... labels )
    {
        ktx.assertOpen();

        NodeLabelIndexCursor client = (NodeLabelIndexCursor) cursor;
        client.setRead( this );
        client.unionScan( new NodeLabelIndexProgressor( labelScanReader().nodesWithAnyOfLabels( labels ), client ),
                false, labels );
    }

    @Override
    public void nodeLabelIntersectionScan( org.neo4j.internal.kernel.api.NodeLabelIndexCursor cursor, int... labels )
    {
        ktx.assertOpen();

        NodeLabelIndexCursor client = (NodeLabelIndexCursor) cursor;
        client.setRead( this );
        client.intersectionScan(
                new NodeLabelIndexProgressor( labelScanReader().nodesWithAllLabels( labels ), client ),
                false, labels );
    }

    @Override
    public final Scan<org.neo4j.internal.kernel.api.NodeLabelIndexCursor> nodeLabelScan( int label )
    {
        ktx.assertOpen();
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public final void allNodesScan( org.neo4j.internal.kernel.api.NodeCursor cursor )
    {
        ktx.assertOpen();
        ((NodeCursor) cursor).scan( this );
    }

    @Override
    public final Scan<org.neo4j.internal.kernel.api.NodeCursor> allNodesScan()
    {
        ktx.assertOpen();
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public final void singleNode( long reference, org.neo4j.internal.kernel.api.NodeCursor cursor )
    {
        ktx.assertOpen();
        ((NodeCursor) cursor).single( reference, this );
    }

    @Override
    public final void singleRelationship( long reference, org.neo4j.internal.kernel.api.RelationshipScanCursor cursor )
    {
        ktx.assertOpen();
        ((RelationshipScanCursor) cursor).single( reference, this );
    }

    @Override
    public final void allRelationshipsScan( org.neo4j.internal.kernel.api.RelationshipScanCursor cursor )
    {
        ktx.assertOpen();
        ((RelationshipScanCursor) cursor).scan( -1/*include all labels*/, this );
    }

    @Override
    public final Scan<org.neo4j.internal.kernel.api.RelationshipScanCursor> allRelationshipsScan()
    {
        ktx.assertOpen();
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public final void relationshipLabelScan( int label, org.neo4j.internal.kernel.api.RelationshipScanCursor cursor )
    {
        ktx.assertOpen();
        ((RelationshipScanCursor) cursor).scan( label, this );
    }

    @Override
    public final Scan<org.neo4j.internal.kernel.api.RelationshipScanCursor> relationshipLabelScan( int label )
    {
        ktx.assertOpen();
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public final void relationshipGroups(
            long nodeReference, long reference, org.neo4j.internal.kernel.api.RelationshipGroupCursor cursor )
    {
        ktx.assertOpen();
        if ( reference == NO_ID ) // there are no relationships for this node
        {
            cursor.close();
        }
        else if ( isRelationship( reference ) ) // the relationships for this node are not grouped in the store
        {
            ((RelationshipGroupCursor) cursor).buffer( nodeReference, clearEncoding( reference ), this );
        }
        else // this is a normal group reference.
        {
            ((RelationshipGroupCursor) cursor).direct( nodeReference, reference, this );
        }
    }

    @Override
    public final void relationships(
            long nodeReference, long reference, org.neo4j.internal.kernel.api.RelationshipTraversalCursor cursor )
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

        switch ( encoding )
        {
        case NONE: // this is a normal relationship reference
            ((RelationshipTraversalCursor) cursor).chain( nodeReference, reference, this );
            break;

        case FILTER: // this relationship chain needs to be filtered
            ((RelationshipTraversalCursor) cursor).filtered( nodeReference, clearEncoding( reference ), this, true );
            break;

        case FILTER_TX_STATE: // tx-state changes should be filtered by the head of this chain
            ((RelationshipTraversalCursor) cursor).filtered( nodeReference, clearEncoding( reference ), this, false );
            break;

        case GROUP: // this reference is actually to a group record
            ((RelationshipTraversalCursor) cursor).groups( nodeReference, clearEncoding( reference ), this );
            break;

        case NO_OUTGOING_OF_TYPE: // nothing in store, but proceed to check tx-state changes
            relationshipType = (int) clearEncoding( reference );
            ((RelationshipTraversalCursor) cursor).filteredTxState( nodeReference, this, relationshipType, OUTGOING );
            break;

        case NO_INCOMING_OF_TYPE: // nothing in store, but proceed to check tx-state changes
            relationshipType = (int) clearEncoding( reference );
            ((RelationshipTraversalCursor) cursor).filteredTxState( nodeReference, this, relationshipType, INCOMING );
            break;

        case NO_LOOP_OF_TYPE: // nothing in store, but proceed to check tx-state changes
            relationshipType = (int) clearEncoding( reference );
            ((RelationshipTraversalCursor) cursor).filteredTxState( nodeReference, this, relationshipType, LOOP );
            break;

        default:
            throw new IllegalStateException( "Unknown encoding " + encoding );
        }
    }

    @Override
    public final void nodeProperties( long nodeReference, long reference, org.neo4j.internal.kernel.api.PropertyCursor cursor )
    {
        ktx.assertOpen();
        ((PropertyCursor) cursor).initNode( nodeReference, reference, this, ktx );
    }

    @Override
    public final void relationshipProperties( long relationshipReference, long reference,
            org.neo4j.internal.kernel.api.PropertyCursor cursor )
    {
        ktx.assertOpen();
        ((PropertyCursor) cursor).initRelationship( relationshipReference, reference, this, ktx );
    }

    @Override
    public final void graphProperties( org.neo4j.internal.kernel.api.PropertyCursor cursor )
    {
        ktx.assertOpen();
        ((PropertyCursor) cursor).initGraph( graphPropertiesReference(), this, ktx );
    }

    abstract long graphPropertiesReference();

    @Override
    public final void nodeExplicitIndexLookup(
            org.neo4j.internal.kernel.api.NodeExplicitIndexCursor cursor, String index, String key, Value value )
            throws KernelException
    {
        ktx.assertOpen();
        ((NodeExplicitIndexCursor) cursor).setRead( this );
        explicitIndex( (org.neo4j.kernel.impl.newapi.NodeExplicitIndexCursor) cursor,
                explicitNodeIndex( index ).get( key, value.asObject() ) );
    }

    @Override
    public final void nodeExplicitIndexQuery(
            org.neo4j.internal.kernel.api.NodeExplicitIndexCursor cursor, String index, Object query )
            throws KernelException
    {
        ktx.assertOpen();
        ((NodeExplicitIndexCursor) cursor).setRead( this );
        explicitIndex( (org.neo4j.kernel.impl.newapi.NodeExplicitIndexCursor) cursor, explicitNodeIndex( index ).query(
                query instanceof Value ? ((Value) query).asObject() : query ) );
    }

    @Override
    public final void nodeExplicitIndexQuery(
            org.neo4j.internal.kernel.api.NodeExplicitIndexCursor cursor, String index, String key, Object query )
            throws KernelException
    {
        ktx.assertOpen();
        ((NodeExplicitIndexCursor) cursor).setRead( this );
        explicitIndex( (NodeExplicitIndexCursor) cursor, explicitNodeIndex( index ).query(
                key, query instanceof Value ? ((Value) query).asObject() : query ) );
    }

    @Override
    public void relationshipExplicitIndexGet(
            org.neo4j.internal.kernel.api.RelationshipExplicitIndexCursor cursor,
            String index,
            String key,
            Value value,
            long source,
            long target ) throws KernelException
    {
        ktx.assertOpen();
        ((RelationshipExplicitIndexCursor) cursor).setRead( this );
        explicitIndex(
                (RelationshipExplicitIndexCursor) cursor,
                explicitRelationshipIndex( index ).get( key, value.asObject(), source, target ) );
    }

    @Override
    public void relationshipExplicitIndexQuery(
            org.neo4j.internal.kernel.api.RelationshipExplicitIndexCursor cursor,
            String index,
            Object query,
            long source,
            long target ) throws KernelException
    {
        ktx.assertOpen();
        ((RelationshipExplicitIndexCursor) cursor).setRead( this );
        explicitIndex(
                (RelationshipExplicitIndexCursor) cursor,
                explicitRelationshipIndex( index )
                        .query( query instanceof Value ? ((Value) query).asObject() : query, source, target ) );
    }

    @Override
    public void relationshipExplicitIndexQuery(
            org.neo4j.internal.kernel.api.RelationshipExplicitIndexCursor cursor,
            String index,
            String key,
            Object query,
            long source,
            long target ) throws KernelException
    {
        ktx.assertOpen();
        ((RelationshipExplicitIndexCursor) cursor).setRead( this );
        explicitIndex(
                (RelationshipExplicitIndexCursor) cursor,
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

    abstract IndexReader indexReader( org.neo4j.internal.kernel.api.IndexReference index );

    abstract LabelScanReader labelScanReader();

    abstract ExplicitIndex explicitNodeIndex( String indexName ) throws KernelException;

    abstract ExplicitIndex explicitRelationshipIndex( String indexName ) throws KernelException;

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

    abstract TextValue string( PropertyCursor cursor, long reference, PageCursor page );

    abstract ArrayValue array( PropertyCursor cursor, long reference, PageCursor page );

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

    void sharedOptimisticLock( ResourceType resource, long resourceId )
    {
        ktx.locks().optimistic().acquireShared( ktx.lockTracer(), resource, resourceId );
    }
}
