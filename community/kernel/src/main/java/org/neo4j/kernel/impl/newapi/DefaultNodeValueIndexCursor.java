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

import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.eclipse.collections.impl.iterator.ImmutableEmptyLongIterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.newapi.TxStateIndexChanges.AddedAndRemoved;
import org.neo4j.kernel.impl.newapi.TxStateIndexChanges.AddedWithValuesAndRemoved;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueTuple;

import static java.util.Arrays.stream;
import static org.neo4j.collection.PrimitiveLongCollections.mergeToSet;
import static org.neo4j.kernel.impl.newapi.Read.NO_ID;
import static org.neo4j.kernel.impl.newapi.TxStateIndexChanges.indexUpdatesForRangeSeek;
import static org.neo4j.kernel.impl.newapi.TxStateIndexChanges.indexUpdatesForRangeSeekByPrefix;
import static org.neo4j.kernel.impl.newapi.TxStateIndexChanges.indexUpdatesForScan;
import static org.neo4j.kernel.impl.newapi.TxStateIndexChanges.indexUpdatesForSeek;
import static org.neo4j.kernel.impl.newapi.TxStateIndexChanges.indexUpdatesForSuffixOrContains;
import static org.neo4j.kernel.impl.newapi.TxStateIndexChanges.indexUpdatesWithValuesForRangeSeek;
import static org.neo4j.kernel.impl.newapi.TxStateIndexChanges.indexUpdatesWithValuesForRangeSeekByPrefix;
import static org.neo4j.kernel.impl.newapi.TxStateIndexChanges.indexUpdatesWithValuesForScan;
import static org.neo4j.kernel.impl.newapi.TxStateIndexChanges.indexUpdatesWithValuesForSuffixOrContains;

final class DefaultNodeValueIndexCursor extends IndexCursor<IndexProgressor>
        implements NodeValueIndexCursor, EntityIndexSeekClient, SortedMergeJoin.Sink
{
    private Read read;
    private long node;
    private float score;
    private IndexQuery[] query;
    private Value[] values;
    private LongIterator added = ImmutableEmptyLongIterator.INSTANCE;
    private Iterator<NodeWithPropertyValues> addedWithValues = Collections.emptyIterator();
    private LongSet removed = LongSets.immutable.empty();
    private boolean needsValues;
    private IndexOrder indexOrder;
    private final CursorPool<DefaultNodeValueIndexCursor> pool;
    private SortedMergeJoin sortedMergeJoin = new SortedMergeJoin();

    DefaultNodeValueIndexCursor( CursorPool<DefaultNodeValueIndexCursor> pool )
    {
        this.pool = pool;
        node = NO_ID;
        score = Float.NaN;
        indexOrder = IndexOrder.NONE;
    }

    @Override
    public void initialize( IndexDescriptor descriptor,
                            IndexProgressor progressor,
                            IndexQuery[] query,
                            IndexOrder indexOrder,
                            boolean needsValues,
                            boolean indexIncludesTransactionState )
    {
        assert query != null;
        super.initialize( progressor );
        sortedMergeJoin.initialize( indexOrder );

        this.indexOrder = indexOrder;
        this.needsValues = needsValues;
        this.query = query;

        getTracer().onIndexSeek();

        if ( !indexIncludesTransactionState && read.hasTxStateWithChanges() && query.length > 0 )
        {
           // Extract out the equality queries
            ArrayList<Value> exactQueryValues = new ArrayList<>( query.length );
            int i = 0;
            while ( i < query.length && query[i] instanceof IndexQuery.ExactPredicate )
            {
                exactQueryValues.add( ((IndexQuery.ExactPredicate) query[i]).value() );
                i++;
            }
            Value[] exactValues = exactQueryValues.toArray( new Value[0] );

            if ( i == query.length )
            {
                // Only exact queries
                // No need to order, all values are the same
                this.indexOrder = IndexOrder.NONE;
                seekQuery( descriptor, exactValues );
            }
            else
            {
                IndexQuery nextQuery = query[i];
                switch ( nextQuery.type() )
                {
                case exists:
                    // This also covers the rewritten suffix/contains for composite index
                    // If composite index all following will be exists as well so no need to consider those
                    setNeedsValuesIfRequiresOrder();
                    if ( exactQueryValues.isEmpty() )
                    {
                        // First query is exists, use scan
                        scanQuery( descriptor );
                    }
                    else
                    {
                        rangeQuery( descriptor, exactValues, null );
                    }
                    break;

                case range:
                    // This case covers first query to be range or exact followed by range
                    // If composite index all following will be exists as well so no need to consider those
                    setNeedsValuesIfRequiresOrder();
                    rangeQuery( descriptor, exactValues, (IndexQuery.RangePredicate) nextQuery );
                    break;

                case stringPrefix:
                    // This case covers first query to be prefix or exact followed by prefix
                    // If composite index all following will be exists as well so no need to consider those
                    setNeedsValuesIfRequiresOrder();
                    prefixQuery( descriptor, exactValues, (IndexQuery.StringPrefixPredicate) nextQuery );
                    break;

                case stringSuffix:
                case stringContains:
                    // This case covers suffix/contains for singular indexes
                    // for composite index, the suffix/contains should already
                    // have been rewritten as exists + filter, so no need to consider it here
                    assert query.length == 1;
                    suffixOrContainsQuery( descriptor, nextQuery );
                    break;

                default:
                    throw new UnsupportedOperationException( "Query not supported: " + Arrays.toString( query ) );
                }
            }
        }
    }

    /**
     * If we require order, we can only do the merge sort if we also get values.
     * This implicitly relies on the fact that if we can get order, we can also get values.
     */
    private void setNeedsValuesIfRequiresOrder()
    {
        if ( indexOrder != IndexOrder.NONE )
        {
            this.needsValues = true;
        }
    }

    private boolean isRemoved( long reference )
    {
        return removed.contains( reference );
    }

    @Override
    public boolean acceptEntity( long reference, float score, Value[] values )
    {
        if ( isRemoved( reference ) )
        {
            return false;
        }
        else
        {
            this.node = reference;
            this.score = score;
            this.values = values;
            return true;
        }
    }

    @Override
    public boolean needsValues()
    {
        return needsValues;
    }

    @Override
    public boolean next()
    {
        if ( indexOrder == IndexOrder.NONE )
        {
            return nextWithoutOrder();
        }
        else
        {
            return nextWithOrdering();
        }
    }

    private boolean nextWithoutOrder()
    {
        if ( !needsValues && added.hasNext() )
        {
            this.node = added.next();
            this.values = null;
            getTracer().onNode( nodeReference() );
            return true;
        }
        else if ( needsValues && addedWithValues.hasNext() )
        {
            NodeWithPropertyValues nodeWithPropertyValues = addedWithValues.next();
            this.node = nodeWithPropertyValues.getNodeId();
            this.values = nodeWithPropertyValues.getValues();
            getTracer().onNode( nodeReference() );
            return true;
        }
        else if ( added.hasNext() || addedWithValues.hasNext() )
        {
            throw new IllegalStateException( "Index cursor cannot have transaction state with values and without values simultaneously" );
        }
        else
        {
            boolean next = innerNext();
            if ( next )
            {
                getTracer().onNode( nodeReference() );
            }
            return next;
        }
    }

    private boolean nextWithOrdering()
    {
        if ( sortedMergeJoin.needsA() && addedWithValues.hasNext() )
        {
            NodeWithPropertyValues nodeWithPropertyValues = addedWithValues.next();
            sortedMergeJoin.setA( nodeWithPropertyValues.getNodeId(), nodeWithPropertyValues.getValues() );
        }

        if ( sortedMergeJoin.needsB() && innerNext() )
        {
            sortedMergeJoin.setB( node, values );
        }

        sortedMergeJoin.next( this );
        boolean next = node != -1;
        if ( next )
        {
            getTracer().onNode( nodeReference() );
        }
        return next;
    }

    @Override
    public void acceptSortedMergeJoin( long nodeId, Value[] values )
    {
        this.node = nodeId;
        this.values = values;
    }

    @Override
    public void setRead( Read read )
    {
        this.read = read;
    }

    @Override
    public void node( NodeCursor cursor )
    {
        read.singleNode( node, cursor );
    }

    @Override
    public long nodeReference()
    {
        return node;
    }

    @Override
    public int numberOfProperties()
    {
        return query == null ? 0 : query.length;
    }

    @Override
    public int propertyKey( int offset )
    {
        return query[offset].propertyKeyId();
    }

    @Override
    public boolean hasValue()
    {
        return values != null;
    }

    @Override
    public float score()
    {
        return score;
    }

    @Override
    public Value propertyValue( int offset )
    {
        return values[offset];
    }

    @Override
    public void close()
    {
        if ( !isClosed() )
        {
            closeProgressor();
            this.node = NO_ID;
            this.score = Float.NaN;
            this.query = null;
            this.values = null;
            this.read = null;
            this.added = ImmutableEmptyLongIterator.INSTANCE;
            this.addedWithValues = Collections.emptyIterator();
            this.removed = LongSets.immutable.empty();

            pool.accept( this );
        }
    }

    @Override
    public boolean isClosed()
    {
        return isProgressorClosed();
    }

    @Override
    public String toString()
    {
        if ( isClosed() )
        {
            return "NodeValueIndexCursor[closed state]";
        }
        else
        {
            String keys = query == null ? "unknown" : Arrays.toString( stream( query ).map( IndexQuery::propertyKeyId ).toArray( Integer[]::new ) );
            return "NodeValueIndexCursor[node=" + node + ", open state with: keys=" + keys +
                    ", values=" + Arrays.toString( values ) +
                    ", underlying record=" + super.toString() + "]";
        }
    }

    private void prefixQuery( IndexDescriptor descriptor, Value[] equalityPrefix, IndexQuery.StringPrefixPredicate predicate )
    {
        TransactionState txState = read.txState();

        if ( needsValues )
        {
            AddedWithValuesAndRemoved changes =
                    indexUpdatesWithValuesForRangeSeekByPrefix( txState, descriptor, equalityPrefix, predicate.prefix(), indexOrder );
            addedWithValues = changes.getAdded().iterator();
            removed = removed( txState, changes.getRemoved() );
        }
        else
        {
            AddedAndRemoved changes = indexUpdatesForRangeSeekByPrefix( txState, descriptor, equalityPrefix, predicate.prefix(), indexOrder );
            added = changes.getAdded().longIterator();
            removed = removed( txState, changes.getRemoved() );
        }
    }

    private void rangeQuery( IndexDescriptor descriptor, Value[] equalityPrefix, IndexQuery.RangePredicate<?> predicate )
    {
        TransactionState txState = read.txState();

        if ( needsValues )
        {
            AddedWithValuesAndRemoved changes = indexUpdatesWithValuesForRangeSeek( txState, descriptor, equalityPrefix, predicate, indexOrder );
            addedWithValues = changes.getAdded().iterator();
            removed = removed( txState, changes.getRemoved() );
        }
        else
        {
            AddedAndRemoved changes = indexUpdatesForRangeSeek( txState, descriptor, equalityPrefix, predicate, indexOrder );
            added = changes.getAdded().longIterator();
            removed = removed( txState, changes.getRemoved() );
        }
    }

    private void scanQuery( IndexDescriptor descriptor )
    {
        TransactionState txState = read.txState();

        if ( needsValues )
        {
            AddedWithValuesAndRemoved changes = indexUpdatesWithValuesForScan( txState, descriptor, indexOrder );
            addedWithValues = changes.getAdded().iterator();
            removed = removed( txState, changes.getRemoved() );
        }
        else
        {
            AddedAndRemoved changes = indexUpdatesForScan( txState, descriptor, indexOrder );
            added = changes.getAdded().longIterator();
            removed = removed( txState, changes.getRemoved() );
        }
    }

    private void suffixOrContainsQuery( IndexDescriptor descriptor, IndexQuery query )
    {
        TransactionState txState = read.txState();

        if ( needsValues )
        {
            AddedWithValuesAndRemoved changes = indexUpdatesWithValuesForSuffixOrContains( txState, descriptor, query, indexOrder );
            addedWithValues = changes.getAdded().iterator();
            removed = removed( txState, changes.getRemoved() );
        }
        else
        {
            AddedAndRemoved changes = indexUpdatesForSuffixOrContains( txState, descriptor, query, indexOrder );
            added = changes.getAdded().longIterator();
            removed = removed( txState, changes.getRemoved() );
        }
    }

    private void seekQuery( IndexDescriptor descriptor, Value[] values )
    {
        TransactionState txState = read.txState();

        AddedAndRemoved changes = indexUpdatesForSeek( txState, descriptor, ValueTuple.of( values ) );
        added = changes.getAdded().longIterator();
        removed = removed( txState, changes.getRemoved() );
    }

    private LongSet removed( TransactionState txState, LongSet removedFromIndex )
    {
        return mergeToSet( txState.addedAndRemovedNodes().getRemoved(), removedFromIndex );
    }

    public void release()
    {
        // nothing to do
    }
}
