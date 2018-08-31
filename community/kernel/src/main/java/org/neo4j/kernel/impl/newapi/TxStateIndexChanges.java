/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.eclipse.collections.impl.UnmodifiableMap;

import java.util.Map;
import java.util.NavigableMap;

import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.kernel.impl.util.diffsets.MutableDiffSets;
import org.neo4j.kernel.impl.util.diffsets.MutableDiffSetsImpl;
import org.neo4j.kernel.impl.util.diffsets.MutableLongDiffSetsImpl;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.storageengine.api.txstate.DiffSets;
import org.neo4j.storageengine.api.txstate.LongDiffSets;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueTuple;
import org.neo4j.values.storable.Values;

import static org.neo4j.values.storable.Values.NO_VALUE;

/**
 * This class provides static utility methods that calculate relevant index updates from a transaction state for several index operations.
 */
class TxStateIndexChanges
{
    private static final ValueTuple MAX_STRING_TUPLE = ValueTuple.of( Values.MAX_STRING );

    static LongDiffSets indexUpdatesForScan( ReadableTransactionState txState, IndexDescriptor descriptor )
    {
        UnmodifiableMap<ValueTuple,? extends LongDiffSets> updates = txState.getIndexUpdates( descriptor.schema() );
        if ( updates == null )
        {
            return LongDiffSets.EMPTY;
        }
        MutableLongDiffSetsImpl diffs = new MutableLongDiffSetsImpl();
        for ( LongDiffSets diffSet : updates.values() )
        {
            diffs.addAll( diffSet.getAdded() );
            diffs.removeAll( diffSet.getRemoved() );
        }
        return diffs;
    }

    static DiffSets<NodeWithPropertyValues> indexUpdatesWithValuesForScan( ReadableTransactionState txState, IndexDescriptor descriptor )
    {
        UnmodifiableMap<ValueTuple,? extends LongDiffSets> updates = txState.getIndexUpdates( descriptor.schema() );
        if ( updates == null )
        {
            return DiffSets.Empty.instance();
        }
        MutableDiffSets<NodeWithPropertyValues> diffs = new MutableDiffSetsImpl<>();
        for ( Map.Entry<ValueTuple,? extends LongDiffSets> entry : updates.entrySet() )
        {
            Value[] values = entry.getKey().getValues();
            LongDiffSets diffSets = entry.getValue();

            diffSets.getAdded().each( nodeId -> diffs.add( new NodeWithPropertyValues( nodeId, values ) ) );
            diffSets.getRemoved().each( nodeId -> diffs.remove( new NodeWithPropertyValues( nodeId, values ) ) );
        }
        return diffs;
    }

    static LongDiffSets indexUpdatesForSuffixOrContains( ReadableTransactionState txState, IndexDescriptor descriptor, IndexQuery query )
    {
        if ( descriptor.schema().getPropertyIds().length != 1 )
        {
            throw new IllegalStateException( "Suffix and contains queries are only supported for single property queries" );
        }
        UnmodifiableMap<ValueTuple,? extends LongDiffSets> updates = txState.getIndexUpdates( descriptor.schema() );
        if ( updates == null )
        {
            return LongDiffSets.EMPTY;
        }

        MutableLongDiffSetsImpl diffs = new MutableLongDiffSetsImpl();
        for ( Map.Entry<ValueTuple,? extends LongDiffSets> entry : updates.entrySet() )
        {
            if ( query.acceptsValue( entry.getKey().getOnlyValue() ) )
            {
                LongDiffSets diffsets = entry.getValue();
                diffs.addAll( diffsets.getAdded() );
                diffs.removeAll( diffsets.getRemoved() );
            }
        }
        return diffs;
    }

    static DiffSets<NodeWithPropertyValues> indexUpdatesWithValuesForSuffixOrContains( ReadableTransactionState txState, IndexDescriptor descriptor,
            IndexQuery query )
    {
        if ( descriptor.schema().getPropertyIds().length != 1 )
        {
            throw new IllegalStateException( "Suffix and contains queries are only supported for single property queries" );
        }

        UnmodifiableMap<ValueTuple,? extends LongDiffSets> updates = txState.getIndexUpdates( descriptor.schema() );
        if ( updates == null )
        {
            return DiffSets.Empty.instance();
        }
        MutableDiffSets<NodeWithPropertyValues> diffs = new MutableDiffSetsImpl<>();
        for ( Map.Entry<ValueTuple,? extends LongDiffSets> entry : updates.entrySet() )
        {
            ValueTuple key = entry.getKey();
            if ( query.acceptsValue( key.getOnlyValue() ) )
            {
                Value[] values = key.getValues();
                LongDiffSets diffSets = entry.getValue();
                diffSets.getAdded().each( nodeId -> diffs.add( new NodeWithPropertyValues( nodeId, values ) ) );
                diffSets.getRemoved().each( nodeId -> diffs.remove( new NodeWithPropertyValues( nodeId, values ) ) );
            }
        }
        return diffs;
    }

    static LongDiffSets indexUpdatesForSeek( ReadableTransactionState txState, IndexDescriptor descriptor, ValueTuple values )
    {
        UnmodifiableMap<ValueTuple,? extends LongDiffSets> updates = txState.getIndexUpdates( descriptor.schema() );
        if ( updates != null )
        {
            LongDiffSets indexUpdatesForSeek = updates.get( values );
            return indexUpdatesForSeek == null ? LongDiffSets.EMPTY : indexUpdatesForSeek;
        }
        return LongDiffSets.EMPTY;
    }

    static LongDiffSets indexUpdatesForRangeSeek( ReadableTransactionState txState, IndexDescriptor descriptor, IndexQuery.RangePredicate<?> predicate )
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
            return LongDiffSets.EMPTY;
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

        MutableLongDiffSetsImpl diffs = new MutableLongDiffSetsImpl();

        Map<ValueTuple,? extends LongDiffSets> inRange = sortedUpdates.subMap( selectedLower, selectedIncludeLower, selectedUpper, selectedIncludeUpper );
        for ( Map.Entry<ValueTuple,? extends LongDiffSets> entry : inRange.entrySet() )
        {
            ValueTuple values = entry.getKey();
            LongDiffSets diffForSpecificValue = entry.getValue();

            // The TreeMap cannot perfectly order multi-dimensional types (spatial) and need additional filtering out false positives
            // TODO: If the composite index starts to be able to handle spatial types the line below needs enhancement
            if ( predicate.isRegularOrder() || predicate.acceptsValue( values.getOnlyValue() ) )
            {
                diffs.addAll( diffForSpecificValue.getAdded() );
                diffs.removeAll( diffForSpecificValue.getRemoved() );
            }
        }
        return diffs;
    }

    static DiffSets<NodeWithPropertyValues> indexUpdatesWithValuesForRangeSeek( ReadableTransactionState txState, IndexDescriptor descriptor,
            IndexQuery.RangePredicate<?> predicate )
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
            return DiffSets.Empty.instance();
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

        MutableDiffSets<NodeWithPropertyValues> diffs = new MutableDiffSetsImpl<>();

        Map<ValueTuple,? extends LongDiffSets> inRange = sortedUpdates.subMap( selectedLower, selectedIncludeLower, selectedUpper, selectedIncludeUpper );
        for ( Map.Entry<ValueTuple,? extends LongDiffSets> entry : inRange.entrySet() )
        {
            ValueTuple values = entry.getKey();
            Value[] valuesArray = values.getValues();
            LongDiffSets diffForSpecificValue = entry.getValue();

            // The TreeMap cannot perfectly order multi-dimensional types (spatial) and need additional filtering out false positives
            // TODO: If the composite index starts to be able to handle spatial types the line below needs enhancement
            if ( predicate.isRegularOrder() || predicate.acceptsValue( values.getOnlyValue() ) )
            {
                diffForSpecificValue.getAdded().each( nodeId -> diffs.add( new NodeWithPropertyValues( nodeId, valuesArray ) ) );
                diffForSpecificValue.getRemoved().each( nodeId -> diffs.remove( new NodeWithPropertyValues( nodeId, valuesArray ) ) );
            }
        }
        return diffs;
    }

    static LongDiffSets indexUpdatesForRangeSeekByPrefix( ReadableTransactionState txState, IndexDescriptor descriptor, String prefix )
    {
        NavigableMap<ValueTuple,? extends LongDiffSets> sortedUpdates = txState.getSortedIndexUpdates( descriptor.schema() );
        if ( sortedUpdates == null )
        {
            return LongDiffSets.EMPTY;
        }
        ValueTuple floor = ValueTuple.of( Values.stringValue( prefix ) );
        MutableLongDiffSetsImpl diffs = new MutableLongDiffSetsImpl();
        for ( Map.Entry<ValueTuple,? extends LongDiffSets> entry : sortedUpdates.subMap( floor, MAX_STRING_TUPLE ).entrySet() )
        {
            ValueTuple key = entry.getKey();
            if ( ((TextValue) key.getOnlyValue()).stringValue().startsWith( prefix ) )
            {
                LongDiffSets diffSets = entry.getValue();
                diffs.addAll( diffSets.getAdded() );
                diffs.removeAll( diffSets.getRemoved() );
            }
            else
            {
                break;
            }
        }
        return diffs;
    }

    static DiffSets<NodeWithPropertyValues> indexUpdatesWithValuesForRangeSeekByPrefix( ReadableTransactionState txState, IndexDescriptor descriptor,
            String prefix )
    {
        NavigableMap<ValueTuple,? extends LongDiffSets> sortedUpdates = txState.getSortedIndexUpdates( descriptor.schema() );
        if ( sortedUpdates == null )
        {
            return DiffSets.Empty.instance();
        }
        ValueTuple floor = ValueTuple.of( Values.stringValue( prefix ) );
        MutableDiffSets<NodeWithPropertyValues> diffs = new MutableDiffSetsImpl<>();
        for ( Map.Entry<ValueTuple,? extends LongDiffSets> entry : sortedUpdates.tailMap( floor ).entrySet() )
        {
            ValueTuple key = entry.getKey();
            if ( ((TextValue) key.getOnlyValue()).stringValue().startsWith( prefix ) )
            {
                LongDiffSets diffSets = entry.getValue();
                Value[] values = key.getValues();
                diffSets.getAdded().each( nodeId -> diffs.add( new NodeWithPropertyValues( nodeId, values ) ) );
                diffSets.getRemoved().each( nodeId -> diffs.remove( new NodeWithPropertyValues( nodeId, values ) ) );
            }
            else
            {
                break;
            }
        }
        return diffs;
    }
}
