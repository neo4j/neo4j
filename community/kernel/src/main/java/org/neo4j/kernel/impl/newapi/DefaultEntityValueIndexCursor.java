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

import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.set.primitive.ImmutableLongSet;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.api.tuple.primitive.LongObjectPair;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.eclipse.collections.impl.iterator.ImmutableEmptyLongIterator;
import org.eclipse.collections.impl.tuple.primitive.PrimitiveTuples;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.neo4j.collection.trackable.HeapTrackingArrayList;
import org.neo4j.graphdb.Resource;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.internal.helpers.collection.ResourceClosingIterator;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.IndexResultScore;
import org.neo4j.internal.kernel.api.KernelReadTracer;
import org.neo4j.internal.kernel.api.ValueIndexCursor;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.newapi.TxStateIndexChanges.AddedAndRemoved;
import org.neo4j.kernel.impl.newapi.TxStateIndexChanges.AddedWithValuesAndRemoved;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.values.storable.PointArray;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueTuple;
import org.neo4j.values.storable.Values;

import static java.util.Arrays.stream;
import static org.neo4j.internal.kernel.api.Read.NO_ID;
import static org.neo4j.kernel.impl.newapi.TxStateIndexChanges.indexUpdatesForRangeSeek;
import static org.neo4j.kernel.impl.newapi.TxStateIndexChanges.indexUpdatesForRangeSeekByPrefix;
import static org.neo4j.kernel.impl.newapi.TxStateIndexChanges.indexUpdatesForScan;
import static org.neo4j.kernel.impl.newapi.TxStateIndexChanges.indexUpdatesForSeek;
import static org.neo4j.kernel.impl.newapi.TxStateIndexChanges.indexUpdatesForSuffixOrContains;
import static org.neo4j.kernel.impl.newapi.TxStateIndexChanges.indexUpdatesWithValuesForRangeSeek;
import static org.neo4j.kernel.impl.newapi.TxStateIndexChanges.indexUpdatesWithValuesForRangeSeekByPrefix;
import static org.neo4j.kernel.impl.newapi.TxStateIndexChanges.indexUpdatesWithValuesForScan;
import static org.neo4j.kernel.impl.newapi.TxStateIndexChanges.indexUpdatesWithValuesForSuffixOrContains;

abstract class DefaultEntityValueIndexCursor<CURSOR> extends IndexCursor<IndexProgressor,CURSOR> implements
        ValueIndexCursor, IndexResultScore, EntityIndexSeekClient, SortedMergeJoin.Sink
{
    private static final Comparator<LongObjectPair<Value[]>> ASCENDING_COMPARATOR = computeComparator( Values.COMPARATOR );
    private static final Comparator<LongObjectPair<Value[]>> DESCENDING_COMPARATOR = computeComparator( ( o1, o2 ) -> - Values.COMPARATOR.compare( o1, o2 ) );

    private Read read;
    private long entity;
    private float score;
    private IndexQuery[] query;
    private Value[] values;
    private LongObjectPair<Value[]> cachedValues;
    private ResourceIterator<LongObjectPair<Value[]>> eagerPointIterator;
    private LongIterator added = ImmutableEmptyLongIterator.INSTANCE;
    private Iterator<EntityWithPropertyValues> addedWithValues = Collections.emptyIterator();
    private ImmutableLongSet removed = LongSets.immutable.empty();
    private boolean needsValues;
    private IndexOrder indexOrder;
    private final MemoryTracker memoryTracker;
    private final SortedMergeJoin sortedMergeJoin = new SortedMergeJoin();
    private AccessMode accessMode;
    private boolean shortcutSecurity;

    DefaultEntityValueIndexCursor( CursorPool<CURSOR> pool, MemoryTracker memoryTracker )
    {
        super( pool );
        this.memoryTracker = memoryTracker;
        entity = NO_ID;
        score = Float.NaN;
        indexOrder = IndexOrder.NONE;
    }

    @Override
    public final void initialize( IndexDescriptor descriptor,
            IndexProgressor progressor,
            IndexQuery[] query,
            IndexQueryConstraints constraints,
            boolean indexIncludesTransactionState )
    {
        assert query != null;
        super.initialize( progressor );
        this.indexOrder = constraints.order();
        this.needsValues = constraints.needsValues();
        sortedMergeJoin.initialize( indexOrder );

        this.query = query;

        if ( tracer != null )
        {
            tracer.onIndexSeek( );
        }

        shortcutSecurity = setupSecurity( descriptor );

        if ( !indexIncludesTransactionState && read.hasTxStateWithChanges() && query.length > 0 )
        {
            // Extract out the equality queries
            List<Value> exactQueryValues = new ArrayList<>( query.length );
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
                    rangeQuery( descriptor, exactValues, (IndexQuery.RangePredicate<?>) nextQuery );
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
    public final boolean acceptEntity( long reference, float score, Value... values )
    {
        if ( isRemoved( reference ) || !allowed( reference ) )
        {
            return false;
        }
        else
        {
            this.entity = reference;
            this.score = score;
            this.values = values;
            return true;
        }
    }

    boolean allowsAll()
    {
        return false;
    }

    @Override
    public final boolean needsValues()
    {
        return needsValues;
    }

    @Override
    public final boolean next()
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
            this.entity = added.next();
            this.values = null;
            if ( tracer != null )
            {
                traceOnEntity( tracer, entity );
            }
            return true;
        }
        else if ( needsValues && addedWithValues.hasNext() )
        {
            EntityWithPropertyValues entityWithPropertyValues = addedWithValues.next();
            this.entity = entityWithPropertyValues.getEntityId();
            this.values = entityWithPropertyValues.getValues();
            if ( tracer != null )
            {
                traceOnEntity( tracer, entity );
            }
            return true;
        }
        else if ( added.hasNext() || addedWithValues.hasNext() )
        {
            throw new IllegalStateException( "Index cursor cannot have transaction state with values and without values simultaneously" );
        }
        else
        {
            boolean next = innerNext();
            if ( tracer != null && next )
            {
                traceOnEntity( tracer, entity );
            }
            return next;
        }
    }

    private boolean nextWithOrdering()
    {
        if ( sortedMergeJoin.needsA() && addedWithValues.hasNext() )
        {
            EntityWithPropertyValues entityWithPropertyValues = addedWithValues.next();
            sortedMergeJoin.setA( entityWithPropertyValues.getEntityId(), entityWithPropertyValues.getValues() );
        }

        if ( sortedMergeJoin.needsB() && innerNextFromBuffer() )
        {
            sortedMergeJoin.setB( entity, values );
        }

        boolean next = sortedMergeJoin.next( this );
        if ( tracer != null && next )
        {
            traceOnEntity( tracer, entity );
        }
        return next;
    }

    private boolean innerNextFromBuffer()
    {
        if ( eagerPointIterator != null )
        {
            return streamPointsFromIterator();
        }

        boolean innerNext = innerNext();
        if ( values != null && innerNext && indexOrder != IndexOrder.NONE )
        {
            return eagerizingPoints();
        }
        else
        {
            return innerNext;
        }
    }

    private boolean containsPoints()
    {
        for ( Value value : values )
        {
            if ( value instanceof PointValue || value instanceof PointArray )
            {
                return true;
            }
        }
        return false;
    }

    private boolean eagerizingPoints()
    {
        HeapTrackingArrayList<LongObjectPair<Value[]>> eagerPointBuffer = null;
        boolean shouldContinue = true;

        while ( shouldContinue && containsPoints() )
        {
            if ( eagerPointBuffer == null )
            {
                eagerPointBuffer = HeapTrackingArrayList.newArrayList( 256, memoryTracker );
            }
            eagerPointBuffer.add( PrimitiveTuples.pair( entity, Arrays.copyOf( values, values.length ) ));
            shouldContinue = innerNext();
        }
        if ( eagerPointBuffer != null )
        {
            if ( shouldContinue )
            {
                this.cachedValues = PrimitiveTuples.pair( entity, Arrays.copyOf( values, values.length ) );
            }

            eagerPointBuffer.sort( comparator() );
            eagerPointIterator = ResourceClosingIterator.newResourceIterator( eagerPointBuffer.autoClosingIterator(), asResource( eagerPointBuffer ) );
            return streamPointsFromIterator();
        }
        else
        {
            return true;
        }
    }

    private Resource asResource( AutoCloseable resource )
    {
        return () -> IOUtils.closeAllUnchecked( resource );
    }

    private static Comparator<LongObjectPair<Value[]>> computeComparator( Comparator<Value> comparator )
    {
        return ( o1, o2 ) ->
        {
            Value[] v1 = o1.getTwo();
            Value[] v2 = o2.getTwo();
            for ( int i = 0; i < v1.length; i++ )
            {
                int compare = comparator.compare( v1[i], v2[i] );
                if ( compare != 0 )
                {
                    return compare;
                }
            }

            return 0;
        };
    }

    private Comparator<LongObjectPair<Value[]>> comparator()
    {
        switch ( indexOrder )
        {
        case ASCENDING:
            return ASCENDING_COMPARATOR;
        case DESCENDING:
            return DESCENDING_COMPARATOR;
        default:
            throw new IllegalStateException( "can't sort if no indexOrder defined" );
        }
    }

    private boolean streamPointsFromIterator()
    {
        if ( eagerPointIterator.hasNext() )
        {
            LongObjectPair<Value[]> nextPair = eagerPointIterator.next();
            entity = nextPair.getOne();
            values = nextPair.getTwo();
            return true;
        }
        else if ( cachedValues != null )
        {
            values = cachedValues.getTwo();
            entity = cachedValues.getOne();
            eagerPointIterator = null;
            cachedValues = null;
            return true;
        }
        else
        {
            return false;
        }
    }

    @Override
    public final void acceptSortedMergeJoin( long entityId, Value[] values )
    {
        this.entity = entityId;
        this.values = values;
    }

    @Override
    public final void setRead( Read read )
    {
        this.read = read;
    }

    @Override
    public final int numberOfProperties()
    {
        return query == null ? 0 : query.length;
    }

    @Override
    public final int propertyKey( int offset )
    {
        return query[offset].propertyKeyId();
    }

    @Override
    public final boolean hasValue()
    {
        return values != null;
    }

    @Override
    public final float score()
    {
        return score;
    }

    @Override
    public final Value propertyValue( int offset )
    {
        return values[offset];
    }

    @Override
    public final void closeInternal()
    {
        if ( !isClosed() )
        {
            closeProgressor();
            this.entity = NO_ID;
            this.score = Float.NaN;
            this.query = null;
            this.values = null;
            this.read = null;
            this.accessMode = null;
            this.added = ImmutableEmptyLongIterator.INSTANCE;
            this.addedWithValues = Collections.emptyIterator();
            this.removed = LongSets.immutable.empty();

            if ( eagerPointIterator  != null )
            {
                eagerPointIterator.close();
            }
        }
        super.closeInternal();
    }

    @Override
    public final boolean isClosed()
    {
        return isProgressorClosed();
    }

    @Override
    public String toString()
    {
        if ( isClosed() )
        {
            return implementationName() + "[closed state]";
        }
        else
        {
            String keys = query == null ? "unknown" : Arrays.toString( stream( query ).map( IndexQuery::propertyKeyId ).toArray( Integer[]::new ) );
            return implementationName() + "[entity=" + entity + ", open state with: keys=" + keys +
                    ", values=" + Arrays.toString( values ) + "]";
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
            AddedAndRemoved changes =
                    indexUpdatesForRangeSeekByPrefix( txState, descriptor, equalityPrefix, predicate.prefix(), indexOrder );
            added = changes.getAdded().longIterator();
            removed = removed( txState, changes.getRemoved() );
        }
    }

    private void rangeQuery( IndexDescriptor descriptor, Value[] equalityPrefix, IndexQuery.RangePredicate<?> predicate )
    {
        TransactionState txState = read.txState();

        if ( needsValues )
        {
            AddedWithValuesAndRemoved
                    changes = indexUpdatesWithValuesForRangeSeek( txState, descriptor, equalityPrefix, predicate, indexOrder );
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

    final long entityReference()
    {
        return entity;
    }

    final void readEntity( EntityReader entityReader )
    {
        entityReader.read( read );
    }

    private boolean setupSecurity( IndexDescriptor descriptor )
    {
        if ( allowsAll() )
        {
            return true;
        }

        if ( accessMode == null )
        {
            accessMode = read.ktx.securityContext().mode();
        }

        return canAccessAllDescribedEntities( descriptor, accessMode );
    }

    boolean allowed( long reference )
    {
        if ( shortcutSecurity )
        {
            return true;
        }

        return allowed( reference, accessMode );
    }

    /**
     * Check that the user is allowed to access all entities and properties given by the index descriptor.
     * <p>
     * If {@code true} is returned, it means that security check does not need to be performed for each item in the cursor.
     */
    abstract boolean canAccessAllDescribedEntities( IndexDescriptor descriptor, AccessMode accessMode );

    /**
     * Gets entities removed in the current transaction that are relevant for the index.
     */
    abstract ImmutableLongSet removed( TransactionState txState, LongSet removedFromIndex );

    /**
     * Checks if the user is allowed to see the entity and properties the cursor is currently pointing at.
     */
    abstract boolean allowed( long reference, AccessMode accessMode );

    /**
     * An abstraction over {@link KernelReadTracer#onNode(long)} and {@link KernelReadTracer#onRelationship(long)}.
     */
    abstract void traceOnEntity( KernelReadTracer tracer, long entity );

    /**
     * Name of the concrete implementation used in {@link #toString()}.
     */
    abstract String implementationName();

    @FunctionalInterface
    interface EntityReader
    {
        void read( Read read );
    }
}
