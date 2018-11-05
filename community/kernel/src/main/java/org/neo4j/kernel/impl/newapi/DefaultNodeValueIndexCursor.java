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

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import org.neo4j.graphdb.Resource;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.newapi.TxStateIndexChanges.AddedAndRemoved;
import org.neo4j.kernel.impl.newapi.TxStateIndexChanges.AddedWithValuesAndRemoved;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.storageengine.api.schema.IndexProgressor;
import org.neo4j.storageengine.api.schema.IndexProgressor.NodeValueClient;
import org.neo4j.values.storable.Value;

import static java.util.Arrays.stream;
import static org.neo4j.collection.PrimitiveLongCollections.mergeToSet;
import static org.neo4j.kernel.impl.newapi.TxStateIndexChanges.indexUpdatesForRangeSeek;
import static org.neo4j.kernel.impl.newapi.TxStateIndexChanges.indexUpdatesForRangeSeekByPrefix;
import static org.neo4j.kernel.impl.newapi.TxStateIndexChanges.indexUpdatesForScan;
import static org.neo4j.kernel.impl.newapi.TxStateIndexChanges.indexUpdatesForSeek;
import static org.neo4j.kernel.impl.newapi.TxStateIndexChanges.indexUpdatesForSuffixOrContains;
import static org.neo4j.kernel.impl.newapi.TxStateIndexChanges.indexUpdatesWithValuesForRangeSeek;
import static org.neo4j.kernel.impl.newapi.TxStateIndexChanges.indexUpdatesWithValuesForRangeSeekByPrefix;
import static org.neo4j.kernel.impl.newapi.TxStateIndexChanges.indexUpdatesWithValuesForScan;
import static org.neo4j.kernel.impl.newapi.TxStateIndexChanges.indexUpdatesWithValuesForSuffixOrContains;
import static org.neo4j.kernel.impl.store.record.AbstractBaseRecord.NO_ID;

final class DefaultNodeValueIndexCursor extends IndexCursor<IndexProgressor>
        implements NodeValueIndexCursor, NodeValueClient, SortedMergeJoin.Sink
{
    private Read read;
    private long node;
    private IndexQuery[] query;
    private Value[] values;
    private LongIterator added = ImmutableEmptyLongIterator.INSTANCE;
    private Iterator<NodeWithPropertyValues> addedWithValues = Collections.emptyIterator();
    private LongSet removed = LongSets.immutable.empty();
    private boolean needsValues;
    private IndexOrder indexOrder;
    private final DefaultCursors pool;
    private SortedMergeJoin sortedMergeJoin = new SortedMergeJoin();

    DefaultNodeValueIndexCursor( DefaultCursors pool )
    {
        this.pool = pool;
        node = NO_ID;
        indexOrder = IndexOrder.NONE;
    }

    @Override
    public void initialize( IndexDescriptor descriptor,
                            IndexProgressor progressor,
                            IndexQuery[] query,
                            IndexOrder indexOrder,
                            boolean needsValues )
    {
        assert query != null;
        super.initialize( progressor );
        sortedMergeJoin.initialize( indexOrder );

        this.indexOrder = indexOrder;
        this.needsValues = needsValues;
        this.query = query;

        if ( read.hasTxStateWithChanges() && query.length > 0 )
        {
            IndexQuery firstPredicate = query[0];
            switch ( firstPredicate.type() )
            {
            case exact:
                // No need to order, all values are the same
                this.indexOrder = IndexOrder.NONE;
                seekQuery( descriptor, query );
                break;

            case exists:
                setNeedsValuesIfRequiresOrder();
                scanQuery( descriptor );
                break;

            case range:
                assert query.length == 1;
                setNeedsValuesIfRequiresOrder();
                rangeQuery( descriptor, (IndexQuery.RangePredicate) firstPredicate );
                break;

            case stringPrefix:
                assert query.length == 1;
                setNeedsValuesIfRequiresOrder();
                prefixQuery( descriptor, (IndexQuery.StringPrefixPredicate) firstPredicate );
                break;

            case stringSuffix:
            case stringContains:
                assert query.length == 1;
                suffixOrContainsQuery( descriptor, firstPredicate );
                break;

            default:
                throw new UnsupportedOperationException( "Query not supported: " + Arrays.toString( query ) );
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
    public boolean acceptNode( long reference, Value[] values )
    {
        if ( isRemoved( reference ) )
        {
            return false;
        }
        else
        {
            this.node = reference;
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
            return true;
        }
        else if ( needsValues && addedWithValues.hasNext() )
        {
            NodeWithPropertyValues nodeWithPropertyValues = addedWithValues.next();
            this.node = nodeWithPropertyValues.getNodeId();
            this.values = nodeWithPropertyValues.getValues();
            return true;
        }
        else if ( added.hasNext() || addedWithValues.hasNext() )
        {
            throw new IllegalStateException( "Index cursor cannot have transaction state with values and without values simultaneously" );
        }
        else
        {
            return innerNext();
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
        return node != -1;
    }

    @Override
    public void acceptSortedMergeJoin( long nodeId, Value[] values )
    {
        this.node = nodeId;
        this.values = values;
    }

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
    public Value propertyValue( int offset )
    {
        return values[offset];
    }

    @Override
    public void close()
    {
        if ( !isClosed() )
        {
            super.close();
            this.node = NO_ID;
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
        return super.isClosed();
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

    private void prefixQuery( IndexDescriptor descriptor, IndexQuery.StringPrefixPredicate predicate )
    {
        TransactionState txState = read.txState();

        if ( needsValues )
        {
            AddedWithValuesAndRemoved changes = indexUpdatesWithValuesForRangeSeekByPrefix( txState, descriptor, predicate.prefix(), indexOrder );
            addedWithValues = changes.getAdded().iterator();
            removed = removed( txState, changes.getRemoved() );
        }
        else
        {
            AddedAndRemoved changes = indexUpdatesForRangeSeekByPrefix( txState, descriptor, predicate.prefix(), indexOrder );
            added = changes.getAdded().longIterator();
            removed = removed( txState, changes.getRemoved() );
        }
    }

    private void rangeQuery( IndexDescriptor descriptor, IndexQuery.RangePredicate<?> predicate )
    {
        TransactionState txState = read.txState();

        if ( needsValues )
        {
            AddedWithValuesAndRemoved changes = indexUpdatesWithValuesForRangeSeek( txState, descriptor, predicate, indexOrder );
            addedWithValues = changes.getAdded().iterator();
            removed = removed( txState, changes.getRemoved() );
        }
        else
        {
            AddedAndRemoved changes = indexUpdatesForRangeSeek( txState, descriptor, predicate, indexOrder );
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

    private void seekQuery( IndexDescriptor descriptor, IndexQuery[] query )
    {
        IndexQuery.ExactPredicate[] exactPreds = assertOnlyExactPredicates( query );
        TransactionState txState = read.txState();

        AddedAndRemoved changes = indexUpdatesForSeek( txState, descriptor, IndexQuery.asValueTuple( exactPreds ) );
        added = changes.getAdded().longIterator();
        removed = removed( txState, changes.getRemoved() );
    }

    private LongSet removed( TransactionState txState, LongSet removedFromIndex )
    {
        return mergeToSet( txState.addedAndRemovedNodes().getRemoved(), removedFromIndex );
    }

    private static IndexQuery.ExactPredicate[] assertOnlyExactPredicates( IndexQuery[] predicates )
    {
        IndexQuery.ExactPredicate[] exactPredicates;
        if ( predicates.getClass() == IndexQuery.ExactPredicate[].class )
        {
            exactPredicates = (IndexQuery.ExactPredicate[]) predicates;
        }
        else
        {
            exactPredicates = new IndexQuery.ExactPredicate[predicates.length];
            for ( int i = 0; i < predicates.length; i++ )
            {
                if ( predicates[i] instanceof IndexQuery.ExactPredicate )
                {
                    exactPredicates[i] = (IndexQuery.ExactPredicate) predicates[i];
                }
                else
                {
                    // TODO: what to throw?
                    throw new IllegalArgumentException( "Query not supported: " + Arrays.toString( predicates ) );
                }
            }
        }
        return exactPredicates;
    }

    public void release()
    {
        // nothing to do
    }
}
