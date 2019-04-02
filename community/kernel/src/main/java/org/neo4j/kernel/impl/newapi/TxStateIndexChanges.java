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

import org.eclipse.collections.api.LongIterable;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.UnmodifiableMap;
import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.eclipse.collections.impl.factory.primitive.LongSets;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.NavigableMap;

import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.schema.IndexDescriptor;
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

    // SUFFIX

    static AddedAndRemoved indexUpdatesForSuffixOrContains( ReadableTransactionState txState,
                                                            IndexDescriptor descriptor,
                                                            IndexQuery query,
                                                            IndexOrder indexOrder )
    {
        // TODO fix this when composite can handle suffix and contains
        if ( descriptor.schema().getPropertyIds().length != 1 )
        {
            throw new IllegalStateException( "Suffix and contains queries are only supported for single property queries" );
        }
        return indexUpdatesForScanAndFilter( txState, descriptor, query, indexOrder );
    }

    static AddedWithValuesAndRemoved indexUpdatesWithValuesForSuffixOrContains( ReadableTransactionState txState,
                                                                                IndexDescriptor descriptor,
                                                                                IndexQuery query,
                                                                                IndexOrder indexOrder )
    {
        // TODO fix this when composite can handle suffix and contains
        if ( descriptor.schema().getPropertyIds().length != 1 )
        {
            throw new IllegalStateException( "Suffix and contains queries are only supported for single property queries" );
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

    static AddedWithValuesAndRemoved indexUpdatesWithValuesForSeek( ReadableTransactionState txState, IndexDescriptor descriptor, IndexQuery[] predicates,
            IndexOrder indexOrder )
    {
        // Case: composite index starting with equality, but not containing only equality
        // equality+ exists+ || equality+ range exists* || equality+ prefix exists*
        // equality+ contains exists* || equality+ suffix exists* (not yet supported for composite)
        NavigableMap<ValueTuple,? extends LongDiffSets> sortedUpdates = txState.getSortedIndexUpdates( descriptor.schema() );
        if ( sortedUpdates == null )
        {
            return EMPTY_ADDED_AND_REMOVED_WITH_VALUES;
        }

        ArrayList<IndexQuery.ExactPredicate> exactPreds = new ArrayList<>( predicates.length - 1 );
        int i = 0;
        while ( predicates[i].getClass() == IndexQuery.ExactPredicate.class )
        {
            exactPreds.add( (IndexQuery.ExactPredicate) predicates[i] );
            i++;
        }
        IndexQuery nextPred = predicates[i];

        ArrayList<Map.Entry<ValueTuple,? extends LongDiffSets>> entriesFulfillingExact = new ArrayList<>( predicates.length - 1 );
        for ( Map.Entry<ValueTuple,? extends LongDiffSets> entry : sortedUpdates.entrySet() )
        {
            ValueTuple values = entry.getKey();
            boolean fulfilled = true;
            for (int j = 0; j < i && j < values.size(); j++)
            {
                if ( Values.COMPARATOR.compare( values.valueAt( j ), exactPreds.get( j ).value() ) != 0 )
                {
                    fulfilled = false;
                }
            }
            if ( fulfilled )
            {
                entriesFulfillingExact.add( entry );
            }
        }

        MutableList<NodeWithPropertyValues> added = Lists.mutable.empty();
        MutableLongSet removed = LongSets.mutable.empty();
        for ( Map.Entry<ValueTuple,? extends LongDiffSets> entry : entriesFulfillingExact )
        {
            ValueTuple values = entry.getKey();
            Value[] valuesArray = values.getValues();
            LongDiffSets diffForSpecificValue = entry.getValue();

            if ( fulfillPredicate( values.valueAt( i ), nextPred ) )
            {
                diffForSpecificValue.getAdded().each( nodeId -> added.add( new NodeWithPropertyValues( nodeId, valuesArray ) ) );
                removed.addAll( diffForSpecificValue.getRemoved() );
            }
        }

        return new AddedWithValuesAndRemoved( indexOrder == IndexOrder.DESCENDING ? added.asReversed() : added, removed );
    }

    // RANGE SEEK

    static AddedAndRemoved indexUpdatesForRangeSeek( ReadableTransactionState txState,
                                                     IndexDescriptor descriptor,
                                                     IndexQuery.RangePredicate<?> predicate,
                                                     IndexOrder indexOrder )
    {
        Value lower = predicate.fromValue();
        Value upper = predicate.toValue();
        if ( lower == null || upper == null )
        {
            throw new IllegalStateException( "Use Values.NO_VALUE to encode the lack of a bound" );
        }

        NavigableMap<ValueTuple,? extends LongDiffSets> sortedUpdates = txState.getSortedIndexUpdates( descriptor.schema() );
        if ( sortedUpdates == null )
        {
            return EMPTY_ADDED_AND_REMOVED;
        }

        ValueTuple selectedLower;
        boolean selectedIncludeLower;

        ValueTuple selectedUpper;
        boolean selectedIncludeUpper;

        if ( lower == NO_VALUE )
        {
            selectedLower = ValueTuple.of( Values.minValue( predicate.valueGroup(), upper ) );
            selectedIncludeLower = true;
        }
        else
        {
            selectedLower = ValueTuple.of( lower );
            selectedIncludeLower = predicate.fromInclusive();
        }

        if ( upper == NO_VALUE )
        {
            selectedUpper = ValueTuple.of( Values.maxValue( predicate.valueGroup(), lower ) );
            selectedIncludeUpper = false;
        }
        else
        {
            selectedUpper = ValueTuple.of( upper );
            selectedIncludeUpper = predicate.toInclusive();
        }

        MutableLongList added = LongLists.mutable.empty();
        MutableLongSet removed = LongSets.mutable.empty();

        for ( Map.Entry<ValueTuple,? extends LongDiffSets> entry : sortedUpdates.entrySet() )
        {
            ValueTuple values = entry.getKey();
            Value firstKey = values.valueAt( 0 );
            LongDiffSets diffForSpecificValue = entry.getValue();

            // Needs to manually filter since we can have composite index
            // and we only wants to compare the first value of the key not all of them
            boolean allowed = allowedEntry( firstKey, selectedLower, selectedIncludeLower, selectedUpper, selectedIncludeUpper );

            // The TreeMap cannot perfectly order multi-dimensional types (spatial) and need additional filtering out false positives
            if ( allowed && (predicate.isRegularOrder() || predicate.acceptsValue( firstKey )) )
            {
                added.addAll( diffForSpecificValue.getAdded() );
                removed.addAll( diffForSpecificValue.getRemoved() );
            }
        }
        return new AddedAndRemoved( indexOrder == IndexOrder.DESCENDING ? added.asReversed() : added, removed );
    }

    static AddedWithValuesAndRemoved indexUpdatesWithValuesForRangeSeek( ReadableTransactionState txState,
                                                                         IndexDescriptor descriptor,
                                                                         IndexQuery.RangePredicate<?> predicate,
                                                                         IndexOrder indexOrder )
    {
        Value lower = predicate.fromValue();
        Value upper = predicate.toValue();
        if ( lower == null || upper == null )
        {
            throw new IllegalStateException( "Use Values.NO_VALUE to encode the lack of a bound" );
        }

        NavigableMap<ValueTuple, ? extends LongDiffSets> sortedUpdates = txState.getSortedIndexUpdates( descriptor.schema() );
        if ( sortedUpdates == null )
        {
            return EMPTY_ADDED_AND_REMOVED_WITH_VALUES;
        }

        ValueTuple selectedLower;
        boolean selectedIncludeLower;

        ValueTuple selectedUpper;
        boolean selectedIncludeUpper;

        if ( lower == NO_VALUE )
        {
            selectedLower = ValueTuple.of( Values.minValue( predicate.valueGroup(), upper ) );
            selectedIncludeLower = true;
        }
        else
        {
            selectedLower = ValueTuple.of( lower );
            selectedIncludeLower = predicate.fromInclusive();
        }

        if ( upper == NO_VALUE )
        {
            selectedUpper = ValueTuple.of( Values.maxValue( predicate.valueGroup(), lower ) );
            selectedIncludeUpper = false;
        }
        else
        {
            selectedUpper = ValueTuple.of( upper );
            selectedIncludeUpper = predicate.toInclusive();
        }

        MutableList<NodeWithPropertyValues> added = Lists.mutable.empty();
        MutableLongSet removed = LongSets.mutable.empty();

        for ( Map.Entry<ValueTuple,? extends LongDiffSets> entry : sortedUpdates.entrySet() )
        {
            ValueTuple values = entry.getKey();
            Value firstKey = values.valueAt( 0 );
            Value[] valuesArray = values.getValues();
            LongDiffSets diffForSpecificValue = entry.getValue();

            // Needs to manually filter since we can have composite index
            // and we only wants to compare the first value of the key not all of them
            boolean allowed = allowedEntry( firstKey, selectedLower, selectedIncludeLower, selectedUpper, selectedIncludeUpper );

            // The TreeMap cannot perfectly order multi-dimensional types (spatial) and need additional filtering out false positives
            if ( allowed && (predicate.isRegularOrder() || predicate.acceptsValue( firstKey )) )
            {
                diffForSpecificValue.getAdded().each( nodeId -> added.add( new NodeWithPropertyValues( nodeId, valuesArray ) ) );
                removed.addAll( diffForSpecificValue.getRemoved() );
            }
        }
        return new AddedWithValuesAndRemoved( indexOrder == IndexOrder.DESCENDING ? added.asReversed() : added, removed );
    }

    // PREFIX

    static AddedAndRemoved indexUpdatesForRangeSeekByPrefix( ReadableTransactionState txState,
                                                             IndexDescriptor descriptor, TextValue prefix,
                                                             IndexOrder indexOrder )
    {
        NavigableMap<ValueTuple,? extends LongDiffSets> sortedUpdates = txState.getSortedIndexUpdates( descriptor.schema() );
        if ( sortedUpdates == null )
        {
            return EMPTY_ADDED_AND_REMOVED;
        }

        MutableLongList added = LongLists.mutable.empty();
        MutableLongSet removed = LongSets.mutable.empty();

        for ( Map.Entry<ValueTuple,? extends LongDiffSets> entry : sortedUpdates.entrySet() )
        {
            Value key = entry.getKey().valueAt( 0 );
            if ( key.valueGroup() == ValueGroup.TEXT && ((TextValue) key).startsWith( prefix ) )
            {
                LongDiffSets diffSets = entry.getValue();
                added.addAll( diffSets.getAdded() );
                removed.addAll( diffSets.getRemoved() );
            }
        }
        return new AddedAndRemoved( indexOrder == IndexOrder.DESCENDING ? added.asReversed() : added, removed );
    }

    static AddedWithValuesAndRemoved indexUpdatesWithValuesForRangeSeekByPrefix( ReadableTransactionState txState,
                                                                                 IndexDescriptor descriptor,
                                                                                 TextValue prefix,
                                                                                 IndexOrder indexOrder )
    {
        NavigableMap<ValueTuple,? extends LongDiffSets> sortedUpdates = txState.getSortedIndexUpdates( descriptor.schema() );
        if ( sortedUpdates == null )
        {
            return EMPTY_ADDED_AND_REMOVED_WITH_VALUES;
        }

        MutableList<NodeWithPropertyValues> added = Lists.mutable.empty();
        MutableLongSet removed = LongSets.mutable.empty();

        for ( Map.Entry<ValueTuple,? extends LongDiffSets> entry : sortedUpdates.entrySet() )
        {
            ValueTuple key = entry.getKey();
            Value firstKey = key.valueAt( 0 );
            if ( firstKey.valueGroup() == ValueGroup.TEXT && ((TextValue) firstKey).startsWith( prefix ) )
            {
                LongDiffSets diffSets = entry.getValue();
                Value[] values = key.getValues();
                diffSets.getAdded().each( nodeId -> added.add( new NodeWithPropertyValues( nodeId, values ) ) );
                removed.addAll( diffSets.getRemoved() );
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

        MutableList<NodeWithPropertyValues> added = Lists.mutable.empty();
        MutableLongSet removed = LongSets.mutable.empty();

        for ( Map.Entry<ValueTuple,? extends LongDiffSets> entry : updates.entrySet() )
        {
            ValueTuple key = entry.getKey();
            if ( filter == null || filter.acceptsValue( key.valueAt( 0 ) ) )
            {
                Value[] values = key.getValues();
                LongDiffSets diffSet = entry.getValue();
                diffSet.getAdded().each( nodeId -> added.add( new NodeWithPropertyValues( nodeId, values ) ) );
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

    private static boolean allowedEntry( Value entry, ValueTuple lower, boolean includeLower, ValueTuple upper, boolean includeUpper )
    {
        int compareLower = Values.COMPARATOR.compare( lower.valueAt( 0 ), entry );
        int compareUpper = Values.COMPARATOR.compare( upper.valueAt( 0 ), entry );
        return ((includeLower && compareLower <= 0) || compareLower < 0) && ((includeUpper && compareUpper >= 0) || compareUpper > 0);
    }

    private static boolean fulfillPredicate( Value value, IndexQuery predicate )
    {
        // don't check for exact since those should already be filtered out
        switch ( predicate.type() )
        {
        case exists:
            return true;

        case range:
            IndexQuery.RangePredicate predRange = (IndexQuery.RangePredicate) predicate;
            Value lower = predRange.fromValue();
            Value upper = predRange.toValue();
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
                selectedLower = ValueTuple.of( Values.minValue( predRange.valueGroup(), upper ) );
                selectedIncludeLower = true;
            }
            else
            {
                selectedLower = ValueTuple.of( lower );
                selectedIncludeLower = predRange.fromInclusive();
            }

            if ( upper == NO_VALUE )
            {
                selectedUpper = ValueTuple.of( Values.maxValue( predRange.valueGroup(), lower ) );
                selectedIncludeUpper = false;
            }
            else
            {
                selectedUpper = ValueTuple.of( upper );
                selectedIncludeUpper = predRange.toInclusive();
            }
            boolean allowed = allowedEntry( value, selectedLower, selectedIncludeLower, selectedUpper, selectedIncludeUpper );
            return allowed && (predRange.isRegularOrder() || predRange.acceptsValue( value ));

        case stringPrefix:
            TextValue prefix = ((IndexQuery.StringPrefixPredicate) predicate).prefix();
            return ((TextValue) value).startsWith( prefix );

        case stringSuffix:
        case stringContains:
            // TODO fix when composite can handle suffix and contains
            throw new IllegalStateException( "Suffix and contains queries are only supported for single property queries" );

        default:
            return false;
        }
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
        private final Iterable<NodeWithPropertyValues> added;
        private final LongSet removed;

        AddedWithValuesAndRemoved( Iterable<NodeWithPropertyValues> added, LongSet removed )
        {
            this.added = added;
            this.removed = removed;
        }

        public boolean isEmpty()
        {
            return !added.iterator().hasNext() && removed.isEmpty();
        }

        public Iterable<NodeWithPropertyValues> getAdded()
        {
            return added;
        }

        public LongSet getRemoved()
        {
            return removed;
        }
    }
}
