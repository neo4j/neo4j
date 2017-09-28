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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.neo4j.internal.kernel.api.IndexPredicate;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.Scan;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.api.store.PropertyUtil;
import org.neo4j.kernel.impl.store.AbstractDynamicStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RecordCursor;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.string.UTF8;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Values;

import static org.neo4j.kernel.impl.store.record.AbstractBaseRecord.NO_ID;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;

public class Read implements org.neo4j.internal.kernel.api.Read
{
    static final long FILTER_MASK = 0x2000_0000_0000_0000L;
    private final RelationshipGroupStore groupStore;
    private final PropertyStore propertyStore;
    private NodeStore nodeStore;
    private RelationshipStore relationshipStore;

    public Read( NeoStores stores )
    {
        this.nodeStore = stores.getNodeStore();
        this.relationshipStore = stores.getRelationshipStore();
        this.groupStore = stores.getRelationshipGroupStore();
        this.propertyStore = stores.getPropertyStore();
    }

    @Override
    public void nodeIndexSeek(
            IndexReference index,
            org.neo4j.internal.kernel.api.NodeValueIndexCursor cursor,
            IndexPredicate... predicates )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void nodeIndexScan( IndexReference index, org.neo4j.internal.kernel.api.NodeValueIndexCursor cursor )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void nodeLabelScan( int label, org.neo4j.internal.kernel.api.NodeLabelIndexCursor cursor )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public Scan<org.neo4j.internal.kernel.api.NodeLabelIndexCursor> nodeLabelScan( int label )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void allNodesScan( org.neo4j.internal.kernel.api.NodeCursor cursor )
    {
        ((NodeCursor) cursor).scan();
    }

    @Override
    public Scan<org.neo4j.internal.kernel.api.NodeCursor> allNodesScan()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void singleNode( long reference, org.neo4j.internal.kernel.api.NodeCursor cursor )
    {
        ((NodeCursor) cursor).single( reference );
    }

    @Override
    public void singleRelationship( long reference, org.neo4j.internal.kernel.api.RelationshipScanCursor cursor )
    {
        ((RelationshipScanCursor) cursor).single( reference );
    }

    @Override
    public void allRelationshipsScan( org.neo4j.internal.kernel.api.RelationshipScanCursor cursor )
    {
        ((RelationshipScanCursor) cursor).scan( -1/*include all labels*/ );
    }

    @Override
    public Scan<org.neo4j.internal.kernel.api.RelationshipScanCursor> allRelationshipsScan()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void relationshipLabelScan( int label, org.neo4j.internal.kernel.api.RelationshipScanCursor cursor )
    {
        ((RelationshipScanCursor) cursor).scan( label );
    }

    @Override
    public Scan<org.neo4j.internal.kernel.api.RelationshipScanCursor> relationshipLabelScan( int label )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void relationshipGroups(
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
    public void relationships(
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
    public void nodeProperties( long reference, org.neo4j.internal.kernel.api.PropertyCursor cursor )
    {
        ((PropertyCursor) cursor).init( reference );
    }

    @Override
    public void relationshipProperties( long reference, org.neo4j.internal.kernel.api.PropertyCursor cursor )
    {
        ((PropertyCursor) cursor).init( reference );
    }

    @Override
    public void futureNodeReferenceRead( long reference )
    {
    }

    @Override
    public void futureRelationshipsReferenceRead( long reference )
    {
    }

    @Override
    public void futureNodePropertyReferenceRead( long reference )
    {
    }

    @Override
    public void futureRelationshipPropertyReferenceRead( long reference )
    {
    }

    PageCursor nodePage( long reference )
    {
        return nodeStore.openPageCursor( reference );
    }

    PageCursor relationshipPage( long reference )
    {
        return relationshipStore.openPageCursor( reference );
    }

    PageCursor groupPage( long reference )
    {
        return groupStore.openPageCursor( reference );
    }

    PageCursor propertyPage( long reference )
    {
        return propertyStore.openPageCursor( reference );
    }

    PageCursor stringPage( long reference )
    {
        return propertyStore.getStringStore().openPageCursor( reference );
    }

    PageCursor arrayPage( long reference )
    {
        return propertyStore.getArrayStore().openPageCursor( reference );
    }

    RecordCursor<DynamicRecord> labelCursor()
    {
        return newCursor( nodeStore.getDynamicLabelStore() );
    }

    private static <R extends AbstractBaseRecord> RecordCursor<R> newCursor( RecordStore<R> store )
    {
        return store.newRecordCursor( store.newRecord() ).acquire( store.getNumberOfReservedLowIds(), NORMAL );
    }

    void node( NodeRecord record, long reference, PageCursor pageCursor )
    {
        nodeStore.getRecordByCursor( reference, record, RecordLoad.CHECK, pageCursor );
    }

    void relationship( RelationshipRecord record, long reference, PageCursor pageCursor )
    {
        relationshipStore.getRecordByCursor( reference, record, RecordLoad.CHECK, pageCursor );
    }

    void property( PropertyRecord record, long reference, PageCursor pageCursor )
    {
        propertyStore.getRecordByCursor( reference, record, RecordLoad.NORMAL, pageCursor );
    }

    void group( RelationshipGroupRecord record, long reference, PageCursor page )
    {
        groupStore.getRecordByCursor( reference, record, RecordLoad.NORMAL, page );
    }

    long nodeHighMark()
    {
        return nodeStore.getHighestPossibleIdInUse();
    }

    long relationshipHighMark()
    {
        return relationshipStore.getHighestPossibleIdInUse();
    }

    TextValue string( PropertyCursor cursor, long reference, PageCursor page )
    {
        ByteBuffer buffer =
                cursor.buffer = readDynamic( propertyStore.getStringStore(), reference, cursor.buffer, page );
        buffer.flip();
        return Values.stringValue( UTF8.decode( buffer.array(), 0, buffer.limit() ) );
    }

    ArrayValue array( PropertyCursor cursor, long reference, PageCursor page )
    {
        ByteBuffer buffer =
                cursor.buffer = readDynamic( propertyStore.getArrayStore(), reference, cursor.buffer, page );
        buffer.flip();
        return PropertyUtil.readArrayFromBuffer( buffer );
    }

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
     * @param reference the reference to invert.
     * @return the inverted reference.
     */
    static long invertReference( long reference )
    {
        return -2 - reference;
    }

    static long addFilteringFlag( long reference )
    {
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

    private static ByteBuffer readDynamic( AbstractDynamicStore store, long reference, ByteBuffer buffer,
            PageCursor page )
    {
        if ( buffer == null )
        {
            buffer = ByteBuffer.allocate( 512 );
        }
        else
        {
            buffer.clear();
        }
        DynamicRecord record = store.newRecord();
        do
        {
            store.getRecordByCursor( reference, record, RecordLoad.CHECK, page );
            reference = record.getNextBlock();
            byte[] data = record.getData();
            if ( buffer.remaining() < data.length )
            {
                buffer = grow( buffer, data.length );
            }
            buffer.put( data, 0, data.length );
        }
        while ( reference != NO_ID );
        return buffer;
    }

    private static ByteBuffer grow( ByteBuffer buffer, int required )
    {
        buffer.flip();
        int capacity = buffer.capacity();
        do
        {
            capacity *= 2;
        }
        while ( capacity - buffer.limit() < required );
        return ByteBuffer.allocate( capacity ).order( ByteOrder.LITTLE_ENDIAN ).put( buffer );
    }
}
