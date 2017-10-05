/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.Scan;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.RecordCursor;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.LabelScanReader;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

import static org.neo4j.kernel.impl.store.record.AbstractBaseRecord.NO_ID;

abstract class Read implements org.neo4j.internal.kernel.api.Read
{
    static final long FILTER_MASK = 0x2000_0000_0000_0000L;

    @Override
    public final void nodeIndexSeek(
            org.neo4j.internal.kernel.api.IndexReference index,
            org.neo4j.internal.kernel.api.NodeValueIndexCursor cursor,
            IndexQuery... query )
    {
        IndexCursorProgressor.NodeValueCursor target = (NodeValueIndexCursor) cursor;
        IndexReader reader = indexReader( (IndexReference) index );
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
                }
            }
            if ( j > 0 )
            {
                filters = Arrays.copyOf( filters, j );
                target = new IndexCursorFilter( target, new NodeCursor( this ), new PropertyCursor( this ), filters );
            }
        }
        reader.query( target, query );
    }

    @Override
    public final void nodeIndexScan(
            org.neo4j.internal.kernel.api.IndexReference index,
            org.neo4j.internal.kernel.api.NodeValueIndexCursor cursor )
    {
        indexReader( (IndexReference) index ).scan( (NodeValueIndexCursor) cursor );
    }

    @Override
    public final void nodeLabelScan( int label, org.neo4j.internal.kernel.api.NodeLabelIndexCursor cursor )
    {
        LabelScanReader reader = labelScanReader();
        IndexCursorProgressor.NodeLabelCursor target = (NodeLabelIndexCursor) cursor;
        target.initialize( new NodeLabelIndexProgressor( reader.nodesWithLabel( label ), target ), false );
    }

    @Override
    public final Scan<org.neo4j.internal.kernel.api.NodeLabelIndexCursor> nodeLabelScan( int label )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public final void allNodesScan( org.neo4j.internal.kernel.api.NodeCursor cursor )
    {
        ((NodeCursor) cursor).scan();
    }

    @Override
    public final Scan<org.neo4j.internal.kernel.api.NodeCursor> allNodesScan()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public final void singleNode( long reference, org.neo4j.internal.kernel.api.NodeCursor cursor )
    {
        ((NodeCursor) cursor).single( reference );
    }

    @Override
    public final void singleRelationship( long reference, org.neo4j.internal.kernel.api.RelationshipScanCursor cursor )
    {
        ((RelationshipScanCursor) cursor).single( reference );
    }

    @Override
    public final void allRelationshipsScan( org.neo4j.internal.kernel.api.RelationshipScanCursor cursor )
    {
        ((RelationshipScanCursor) cursor).scan( -1/*include all labels*/ );
    }

    @Override
    public final Scan<org.neo4j.internal.kernel.api.RelationshipScanCursor> allRelationshipsScan()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public final void relationshipLabelScan( int label, org.neo4j.internal.kernel.api.RelationshipScanCursor cursor )
    {
        ((RelationshipScanCursor) cursor).scan( label );
    }

    @Override
    public final Scan<org.neo4j.internal.kernel.api.RelationshipScanCursor> relationshipLabelScan( int label )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public final void relationshipGroups(
            long nodeReference, long reference, org.neo4j.internal.kernel.api.RelationshipGroupCursor cursor )
    {
        if ( reference == NO_ID ) // there are no relationships for this node
        {
            cursor.close();
        }
        else if ( reference < NO_ID ) // the relationships for this node are not grouped
        {
            ((RelationshipGroupCursor) cursor).buffer( nodeReference, invertReference( reference ) );
        }
        else // this is a normal group reference.
        {
            ((RelationshipGroupCursor) cursor).direct( nodeReference, reference );
        }
    }

    @Override
    public final void relationships(
            long nodeReference, long reference, org.neo4j.internal.kernel.api.RelationshipTraversalCursor cursor )
    {
        /* TODO: There are actually five (5!) different ways a relationship traversal cursor can be initialized:
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
        if ( reference == NO_ID ) // there are no relationships for this node
        {
            cursor.close();
        }
        else if ( reference < NO_ID ) // this reference is actually to a group record
        {
            ((RelationshipTraversalCursor) cursor).groups( nodeReference, invertReference( reference ) );
        }
        else if ( needsFiltering( reference ) )
        {
            ((RelationshipTraversalCursor) cursor).filtered( nodeReference, removeFilteringFlag( reference ) );
        }
        else // this is a normal relationship reference
        {
            ((RelationshipTraversalCursor) cursor).chain( nodeReference, reference );
        }
    }

    @Override
    public final void nodeProperties( long reference, org.neo4j.internal.kernel.api.PropertyCursor cursor )
    {
        ((PropertyCursor) cursor).init( reference );
    }

    @Override
    public final void relationshipProperties( long reference, org.neo4j.internal.kernel.api.PropertyCursor cursor )
    {
        ((PropertyCursor) cursor).init( reference );
    }

    @Override
    public final void futureNodeReferenceRead( long reference )
    {
    }

    @Override
    public final void futureRelationshipsReferenceRead( long reference )
    {
    }

    @Override
    public final void futureNodePropertyReferenceRead( long reference )
    {
    }

    @Override
    public final void futureRelationshipPropertyReferenceRead( long reference )
    {
    }

    abstract IndexReader indexReader( IndexReference index );

    abstract LabelScanReader labelScanReader();

    @Override
    public abstract IndexReference index( int label, int... properties );

    abstract PageCursor nodePage( long reference );

    abstract PageCursor relationshipPage( long reference );

    abstract PageCursor groupPage( long reference );

    abstract PageCursor propertyPage( long reference );

    abstract PageCursor stringPage( long reference );

    abstract PageCursor arrayPage( long reference );

    abstract RecordCursor<DynamicRecord> labelCursor();

    abstract void node( NodeRecord record, long reference, PageCursor pageCursor );

    abstract void relationship( RelationshipRecord record, long reference, PageCursor pageCursor );

    abstract void property( PropertyRecord record, long reference, PageCursor pageCursor );

    abstract void group( RelationshipGroupRecord record, long reference, PageCursor page );

    abstract long nodeHighMark();

    abstract long relationshipHighMark();

    abstract TextValue string( PropertyCursor cursor, long reference, PageCursor page );

    abstract ArrayValue array( PropertyCursor cursor, long reference, PageCursor page );

    /**
     * Inverted references are used to signal that the reference is of a special type.
     * <p>
     * For example that it is a direct reference to a relationship record rather than a reference to relationship group
     * record in {@link NodeCursor#relationshipGroupReference()}.
     * <p>
     * Since {@code -1} is used to encode {@link AbstractBaseRecord#NO_ID that the reference is invalid}, we reserve
     * this value from the range by subtracting the reference from {@code -2} when inverting.
     * <p>
     * This function is its own inverse function.
     *
     * @param reference
     *         the reference to invert.
     * @return the inverted reference.
     */
    static long invertReference( long reference )
    {
        return -2 - reference;
    }

    static long addFilteringFlag( long reference )
    {
        assert reference >= -1;
        // set a high order bit as flag noting that "filtering is required"
        return reference | FILTER_MASK;
    }

    static long removeFilteringFlag( long reference )
    {
        return reference & ~FILTER_MASK;
    }

    static boolean needsFiltering( long reference )
    {
        return (reference & FILTER_MASK) != 0L;
    }
}
