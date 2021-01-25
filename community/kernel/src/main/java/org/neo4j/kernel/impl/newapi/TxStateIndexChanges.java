/*
 * Copyright (c) "Neo4j"
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

import org.eclipse.collections.api.LongIterable;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.UnmodifiableMap;
import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.eclipse.collections.impl.factory.primitive.LongSets;

import java.util.Collections;
import java.util.Map;
import java.util.NavigableMap;

import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.storageengine.api.txstate.LongDiffSets;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.ValueTuple;
import org.neo4j.values.storable.Values;

import static org.neo4j.values.storable.Values.NO_VALUE;

/**
 * This class provides static utility methods that calculate relevant index updates from a transaction state for several index operations.
 */
class TxStateIndexChanges
{

    private static final AddedWithValuesAndRemoved EMPTY_ADDED_AND_REMOVED_WITH_VALUES =
            new AddedWithValuesAndRemoved( Collections.emptyList(), LongSets.immutable.empty() );
    private static final AddedAndRemoved EMPTY_ADDED_AND_REMOVED =
            new AddedAndRemoved( LongLists.immutable.empty(), LongSets.immutable.empty() );

    // SCAN

    static AddedAndRemoved indexUpdatesForScan( ReadableTransactionState txState, IndexDescriptor descriptor, IndexOrder indexOrder )
    {
        return indexUpdatesForScanAndFilter( txState, descriptor, null, indexOrder );
    }

    static AddedWithValuesAndRemoved indexUpdatesWithValuesForScan( ReadableTransactionState txState,
                                                                    IndexDescriptor descriptor,
                                                                    IndexOrder indexOrder )
    {
        return indexUpdatesWithValuesScanAndFilter( txState, descriptor, null, indexOrder );
    }

    // SUFFIX or CONTAINS

    static AddedAndRemoved indexUpdatesForSuffixOrContains( ReadableTransactionState txState,
                                                            IndexDescriptor descriptor,
                                                            IndexQuery query,
                                                            IndexOrder indexOrder )
    {
        if ( descriptor.schema().getPropertyIds().length != 1 )
        {
            throw new IllegalStateException(
                    "Suffix and contains queries on multiple property queries should have been rewritten as existence and filter before now" );
        }
        return indexUpdatesForScanAndFilter( txState, descriptor, query, indexOrder );
    }

    static AddedWithValuesAndRemoved indexUpdatesWithValuesForSuffixOrContains( ReadableTransactionState txState,
                                                                                IndexDescriptor descriptor,
                                                                                IndexQuery query,
                                                                                IndexOrder indexOrder )
    {
        if ( descriptor.schema().getPropertyIds().length != 1 )
        {
            throw new IllegalStateException(
                    "Suffix and contains queries on multiple property queries should have been rewritten as existence and filter before now" );
        }
        return indexUpdatesWithValuesScanAndFilter( txState, descriptor, query, indexOrder );
    }

    // SEEK

    static AddedAndRemoved indexUpdatesForSeek( ReadableTransactionState txState,
                                                IndexDescriptor descriptor,
                                                ValueTuple values )
    {
        UnmodifiableMap<ValueTuple,? extends LongDiffSets> updates = txState.getIndexUpdates( descriptor.schema() );
        if ( updates != null )
        {
            LongDiffSets indexUpdatesForSeek = updates.get( values );
            return indexUpdatesForSeek == null ? EMPTY_ADDED_AND_REMOVED :
                   new AddedAndRemoved( LongLists.mutable.ofAll( indexUpdatesForSeek.getAdded() ), indexUpdatesForSeek.getRemoved() );
        }
        return EMPTY_ADDED_AND_REMOVED;
    }

    // RANGE SEEK

    static AddedAndRemoved indexUpdatesForRangeSeek( ReadableTransactionState txState,
                                                     IndexDescriptor descriptor,
                                                     Value[] equalityPrefix,
                                                     IndexQuery.RangePredicate<?> predicate,
                                                     IndexOrder indexOrder )
    {
        NavigableMap<ValueTuple,? extends LongDiffSets> sortedUpdates = txState.getSortedIndexUpdates( descriptor.schema() );
        if ( sortedUpdates == null )
        {
            return EMPTY_ADDED_AND_REMOVED;
        }

        int size = descriptor.schema().getPropertyIds().length;
        RangeFilterValues rangeFilter = predicate == null ?
                                        RangeFilterValues.fromExists( size, equalityPrefix ) :
                                        RangeFilterValues.fromRange( size, equalityPrefix, predicate );

        MutableLongList added = LongLists.mutable.empty();
        MutableLongSet removed = LongSets.mutable.empty();

        Map<ValueTuple,? extends LongDiffSets> inRange = sortedUpdates.subMap( rangeFilter.lower, true, rangeFilter.upper, true );
        for ( Map.Entry<ValueTuple,? extends LongDiffSets> entry : inRange.entrySet() )
        {
            ValueTuple values = entry.getKey();
            Value rangeKey = values.valueAt( equalityPrefix.length );
            LongDiffSets diffForSpecificValue = entry.getValue();

            // Needs to manually filter for if lower or upper should be included
            // since we only wants to compare the first value of the key and not all of them for composite indexes
            boolean allowed = rangeFilter.allowedEntry( rangeKey, equalityPrefix.length );

            // The TreeMap cannot perfectly order multi-dimensional types (spatial) and need additional filtering out false positives
            if ( allowed && (predicate == null || predicate.isRegularOrder() || predicate.acceptsValue( rangeKey )) )
            {
                added.addAll( diffForSpecificValue.getAdded() );
                removed.addAll( diffForSpecificValue.getRemoved() );
            }
        }
        return new AddedAndRemoved( indexOrder == IndexOrder.DESCENDING ? added.asReversed() : added, removed );
    }

    static AddedWithValuesAndRemoved indexUpdatesWithValuesForRangeSeek( ReadableTransactionState txState,
                                                                         IndexDescriptor descriptor,
                                                                         Value[] equalityPrefix,
                                                                         IndexQuery.RangePredicate<?> predicate,
                                                                         IndexOrder indexOrder )
    {
        NavigableMap<ValueTuple, ? extends LongDiffSets> sortedUpdates = txState.getSortedIndexUpdates( descriptor.schema() );
        if ( sortedUpdates == null )
        {
            return EMPTY_ADDED_AND_REMOVED_WITH_VALUES;
        }

        int size = descriptor.schema().getPropertyIds().length;
        RangeFilterValues rangeFilter = predicate == null ?
                                        RangeFilterValues.fromExists( size, equalityPrefix ) :
                                        RangeFilterValues.fromRange( size, equalityPrefix, predicate );

        MutableList<EntityWithPropertyValues> added = Lists.mutable.empty();
        MutableLongSet removed = LongSets.mutable.empty();

        Map<ValueTuple,? extends LongDiffSets> inRange = sortedUpdates.subMap( rangeFilter.lower, true, rangeFilter.upper, true );
        for ( Map.Entry<ValueTuple,? extends LongDiffSets> entry : inRange.entrySet() )
        {
            ValueTuple values = entry.getKey();
            Value rangeKey = values.valueAt( equalityPrefix.length );
            Value[] valuesArray = values.getValues();
            LongDiffSets diffForSpecificValue = entry.getValue();

            // Needs to manually filter for if lower or upper should be included
            // since we only wants to compare the first value of the key and not all of them for composite indexes
            boolean allowed = rangeFilter.allowedEntry( rangeKey, equalityPrefix.length );

            // The TreeMap cannot perfectly order multi-dimensional types (spatial) and need additional filtering out false positives
            if ( allowed && (predicate == null || predicate.isRegularOrder() || predicate.acceptsValue( rangeKey )) )
            {
                diffForSpecificValue.getAdded().each( nodeId -> added.add( new EntityWithPropertyValues( nodeId, valuesArray ) ) );
                removed.addAll( diffForSpecificValue.getRemoved() );
            }
        }
        return new AddedWithValuesAndRemoved( indexOrder == IndexOrder.DESCENDING ? added.asReversed() : added, removed );
    }

    // PREFIX

    static AddedAndRemoved indexUpdatesForRangeSeekByPrefix( ReadableTransactionState txState,
                                                             IndexDescriptor descriptor,
                                                             Value[] equalityPrefix,
                                                             TextValue prefix,
                                                             IndexOrder indexOrder )
    {
        NavigableMap<ValueTuple,? extends LongDiffSets> sortedUpdates = txState.getSortedIndexUpdates( descriptor.schema() );
        if ( sortedUpdates == null )
        {
            return EMPTY_ADDED_AND_REMOVED;
        }
        int size = descriptor.schema().getPropertyIds().length;
        ValueTuple floor = getCompositeValueTuple( size, equalityPrefix, prefix, true );
        ValueTuple maxString = getCompositeValueTuple( size, equalityPrefix, Values.MAX_STRING, false );

        MutableLongList added = LongLists.mutable.empty();
        MutableLongSet removed = LongSets.mutable.empty();

        for ( Map.Entry<ValueTuple,? extends LongDiffSets> entry : sortedUpdates.subMap( floor, maxString ).entrySet() )
        {
            Value key = entry.getKey().valueAt( equalityPrefix.length );
            // Needs to check type since the subMap might include non-TextValue for composite index
            if ( key.valueGroup() == ValueGroup.TEXT && ((TextValue) key).startsWith( prefix ) )
            {
                LongDiffSets diffSets = entry.getValue();
                added.addAll( diffSets.getAdded() );
                removed.addAll( diffSets.getRemoved() );
            }
            else
            {
                break;
            }
        }
        return new AddedAndRemoved( indexOrder == IndexOrder.DESCENDING ? added.asReversed() : added, removed );
    }

    static AddedWithValuesAndRemoved indexUpdatesWithValuesForRangeSeekByPrefix( ReadableTransactionState txState,
                                                                                 IndexDescriptor descriptor,
                                                                                 Value[] equalityPrefix,
                                                                                 TextValue prefix,
                                                                                 IndexOrder indexOrder )
    {
        NavigableMap<ValueTuple,? extends LongDiffSets> sortedUpdates = txState.getSortedIndexUpdates( descriptor.schema() );
        if ( sortedUpdates == null )
        {
            return EMPTY_ADDED_AND_REMOVED_WITH_VALUES;
        }
        int keySize = descriptor.schema().getPropertyIds().length;
        ValueTuple floor = getCompositeValueTuple( keySize, equalityPrefix, prefix, true );
        ValueTuple maxString = getCompositeValueTuple( keySize, equalityPrefix, Values.MAX_STRING, false );

        MutableList<EntityWithPropertyValues> added = Lists.mutable.empty();
        MutableLongSet removed = LongSets.mutable.empty();

        for ( Map.Entry<ValueTuple,? extends LongDiffSets> entry : sortedUpdates.subMap( floor, maxString ).entrySet() )
        {
            ValueTuple key = entry.getKey();
            Value prefixKey = key.valueAt( equalityPrefix.length );
            // Needs to check type since the subMap might include non-TextValue for composite index
            if ( prefixKey.valueGroup() == ValueGroup.TEXT && ((TextValue) prefixKey).startsWith( prefix ) )
            {
                LongDiffSets diffSets = entry.getValue();
                Value[] values = key.getValues();
                diffSets.getAdded().each( nodeId -> added.add( new EntityWithPropertyValues( nodeId, values ) ) );
                removed.addAll( diffSets.getRemoved() );
            }
            else
            {
                break;
            }
        }
        return new AddedWithValuesAndRemoved( indexOrder == IndexOrder.DESCENDING ? added.asReversed() : added, removed );
    }

    // HELPERS

    private static AddedAndRemoved indexUpdatesForScanAndFilter( ReadableTransactionState txState,
                                                                 IndexDescriptor descriptor,
                                                                 IndexQuery filter,
                                                                 IndexOrder indexOrder )
    {
        Map<ValueTuple,? extends LongDiffSets> updates = getUpdates( txState, descriptor, indexOrder );

        if ( updates == null )
        {
            return EMPTY_ADDED_AND_REMOVED;
        }

        MutableLongList added = LongLists.mutable.empty();
        MutableLongSet removed = LongSets.mutable.empty();

        for ( Map.Entry<ValueTuple,? extends LongDiffSets> entry : updates.entrySet() )
        {
            ValueTuple key = entry.getKey();
            if ( filter == null || filter.acceptsValue( key.valueAt( 0 ) ) )
            {
                LongDiffSets diffSet = entry.getValue();
                added.addAll( diffSet.getAdded() );
                removed.addAll( diffSet.getRemoved() );
            }
        }
        return new AddedAndRemoved( indexOrder == IndexOrder.DESCENDING ? added.asReversed() : added, removed );
    }

    private static AddedWithValuesAndRemoved indexUpdatesWithValuesScanAndFilter( ReadableTransactionState txState,
                                                                                  IndexDescriptor descriptor,
                                                                                  IndexQuery filter,
                                                                                  IndexOrder indexOrder )
    {
        Map<ValueTuple,? extends LongDiffSets> updates = getUpdates( txState, descriptor, indexOrder );

        if ( updates == null )
        {
            return EMPTY_ADDED_AND_REMOVED_WITH_VALUES;
        }

        MutableList<EntityWithPropertyValues> added = Lists.mutable.empty();
        MutableLongSet removed = LongSets.mutable.empty();

        for ( Map.Entry<ValueTuple,? extends LongDiffSets> entry : updates.entrySet() )
        {
            ValueTuple key = entry.getKey();
            if ( filter == null || filter.acceptsValue( key.valueAt( 0 ) ) )
            {
                Value[] values = key.getValues();
                LongDiffSets diffSet = entry.getValue();
                diffSet.getAdded().each( nodeId -> added.add( new EntityWithPropertyValues( nodeId, values ) ) );
                removed.addAll( diffSet.getRemoved() );
            }
        }
        return new AddedWithValuesAndRemoved( indexOrder == IndexOrder.DESCENDING ? added.asReversed() : added, removed );
    }

    private static Map<ValueTuple,? extends LongDiffSets> getUpdates( ReadableTransactionState txState,
                                                                      IndexDescriptor descriptor,
                                                                      IndexOrder indexOrder )
    {
        return indexOrder == IndexOrder.NONE ?
               txState.getIndexUpdates( descriptor.schema() ) :
               txState.getSortedIndexUpdates( descriptor.schema() );
    }

    private static ValueTuple getCompositeValueTuple( int size, Value[] equalityValues, Value nextValue, boolean minValue )
    {
        Value[] values = new Value[size];
        Value restOfValues = minValue ? Values.MIN_GLOBAL : Values.MAX_GLOBAL;

        System.arraycopy( equalityValues, 0, values, 0, equalityValues.length );
        values[equalityValues.length] = nextValue == null ? restOfValues : nextValue;
        for ( int i = equalityValues.length + 1; i < size; i++ )
        {
            values[i] = restOfValues;
        }
        return ValueTuple.of( values );
    }

    public static class AddedAndRemoved
    {
        private final LongIterable added;
        private final LongSet removed;

        AddedAndRemoved( LongIterable added, LongSet removed )
        {
            this.added = added;
            this.removed = removed;
        }

        public boolean isEmpty()
        {
            return added.isEmpty() && removed.isEmpty();
        }

        public LongIterable getAdded()
        {
            return added;
        }

        public LongSet getRemoved()
        {
            return removed;
        }
    }

    public static class AddedWithValuesAndRemoved
    {
        private final Iterable<EntityWithPropertyValues> added;
        private final LongSet removed;

        AddedWithValuesAndRemoved( Iterable<EntityWithPropertyValues> added, LongSet removed )
        {
            this.added = added;
            this.removed = removed;
        }

        public boolean isEmpty()
        {
            return !added.iterator().hasNext() && removed.isEmpty();
        }

        public Iterable<EntityWithPropertyValues> getAdded()
        {
            return added;
        }

        public LongSet getRemoved()
        {
            return removed;
        }
    }

    private static class RangeFilterValues
    {
        ValueTuple lower;
        ValueTuple upper;
        boolean includeLower, includeUpper;

        private RangeFilterValues( ValueTuple lower, boolean includeLower, ValueTuple upper, boolean includeUpper )
        {
            this.lower = lower;
            this.upper = upper;
            this.includeLower = includeLower;
            this.includeUpper = includeUpper;
        }

        private static RangeFilterValues fromRange( int size, Value[] equalityValues, IndexQuery.RangePredicate<?> predicate )
        {
            Value lower = predicate.fromValue();
            Value upper = predicate.toValue();
            if ( lower == null || upper == null )
            {
                throw new IllegalStateException( "Use Values.NO_VALUE to encode the lack of a bound" );
            }

            ValueTuple selectedLower;
            boolean selectedIncludeLower;

            ValueTuple selectedUpper;
            boolean selectedIncludeUpper;

            if ( lower == NO_VALUE )
            {
                Value min = Values.minValue( predicate.valueGroup(), upper );
                selectedLower = getCompositeValueTuple( size, equalityValues, min, true );
                selectedIncludeLower = true;
            }
            else
            {
                selectedLower = getCompositeValueTuple( size, equalityValues, lower, true );
                selectedIncludeLower = predicate.fromInclusive();
            }

            if ( upper == NO_VALUE )
            {
                Value max = Values.maxValue( predicate.valueGroup(), lower );
                selectedUpper = getCompositeValueTuple( size, equalityValues, max, false );
                selectedIncludeUpper = false;
            }
            else
            {
                selectedUpper = getCompositeValueTuple( size, equalityValues, upper, false );
                selectedIncludeUpper = predicate.toInclusive();
            }

            return new RangeFilterValues( selectedLower, selectedIncludeLower, selectedUpper, selectedIncludeUpper );
        }

        private static RangeFilterValues fromExists( int size, Value[] equalityValues )
        {
            ValueTuple min = getCompositeValueTuple( size, equalityValues, null, true );
            ValueTuple max = getCompositeValueTuple( size, equalityValues, null, false );

            return new RangeFilterValues( min, true, max, true );
        }

        private boolean allowedEntry( Value entry, int length )
        {
            // Entry is already filtered on being between (or equal to) lower and upper
            // Here we just check if we are allowed to be equal to lower and/or upper
            if ( includeLower && includeUpper )
            {
                return true;
            }

            int compareLower = Values.COMPARATOR.compare( lower.valueAt( length ), entry ); // gets value 0 or -1
            int compareUpper = Values.COMPARATOR.compare( upper.valueAt( length ), entry ); // gets value 0 or 1

            if ( compareLower == compareUpper ) // only equal on 0
            {
                return false;
            }
            else
            {
                return (compareLower != 0 && compareUpper != 0) || (compareLower == 0 && includeLower) || (compareUpper == 0 && includeUpper);
            }
        }
    }
}
