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

import java.util.Collection;
import java.util.Map;
import java.util.NavigableMap;

import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.kernel.impl.util.diffsets.DiffSets;
import org.neo4j.kernel.impl.util.diffsets.MutableLongDiffSetsImpl;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.storageengine.api.txstate.LongDiffSets;
import org.neo4j.storageengine.api.txstate.ReadableDiffSets;
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

    static ReadableDiffSets<NodeWithPropertyValues> indexUpdatesWithValuesForScan( ReadableTransactionState txState, IndexDescriptor descriptor )
    {
        UnmodifiableMap<ValueTuple,? extends LongDiffSets> updates = txState.getIndexUpdates( descriptor.schema() );
        if ( updates == null )
        {
            return ReadableDiffSets.Empty.instance();
        }
        DiffSets<NodeWithPropertyValues> diffs = new DiffSets<>();
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

    static ReadableDiffSets<NodeWithPropertyValues> indexUpdatesWithValuesForSuffixOrContains( ReadableTransactionState txState, IndexDescriptor descriptor,
            IndexQuery query )
    {
        if ( descriptor.schema().getPropertyIds().length != 1 )
        {
            throw new IllegalStateException( "Suffix and contains queries are only supported for single property queries" );
        }

        UnmodifiableMap<ValueTuple,? extends LongDiffSets> updates = txState.getIndexUpdates( descriptor.schema() );
        if ( updates == null )
        {
            return ReadableDiffSets.Empty.instance();
        }
        DiffSets<NodeWithPropertyValues> diffs = new DiffSets<>();
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

    static LongDiffSets indexUpdatesForRangeSeek( ReadableTransactionState txState, IndexDescriptor descriptor, ValueGroup valueGroup, Value lower,
            boolean includeLower, Value upper, boolean includeUpper )
    {
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
            selectedLower = ValueTuple.of( Values.minValue( valueGroup, upper ) );
            selectedIncludeLower = true;
        }
        else
        {
            selectedLower = ValueTuple.of( lower );
            selectedIncludeLower = includeLower;
        }

        if ( upper == NO_VALUE )
        {
            selectedUpper = ValueTuple.of( Values.maxValue( valueGroup, lower ) );
            selectedIncludeUpper = false;
        }
        else
        {
            selectedUpper = ValueTuple.of( upper );
            selectedIncludeUpper = includeUpper;
        }

        return indexUpdatesForRangeSeek( sortedUpdates, selectedLower, selectedIncludeLower, selectedUpper, selectedIncludeUpper );
    }

    private static LongDiffSets indexUpdatesForRangeSeek( NavigableMap<ValueTuple,? extends LongDiffSets> sortedUpdates, ValueTuple lower, boolean includeLower,
            ValueTuple upper, boolean includeUpper )
    {
        MutableLongDiffSetsImpl diffs = new MutableLongDiffSetsImpl();

        Collection<? extends LongDiffSets> inRange = sortedUpdates.subMap( lower, includeLower, upper, includeUpper ).values();
        for ( LongDiffSets diffForSpecificValue : inRange )
        {
            diffs.addAll( diffForSpecificValue.getAdded() );
            diffs.removeAll( diffForSpecificValue.getRemoved() );
        }
        return diffs;
    }

    static ReadableDiffSets<NodeWithPropertyValues> indexUpdatesWithValuesForRangeSeek( ReadableTransactionState txState, IndexDescriptor descriptor,
            ValueGroup valueGroup, Value lower, boolean includeLower, Value upper, boolean includeUpper )
    {
        if ( lower == null || upper == null )
        {
            throw new IllegalStateException( "Use Values.NO_VALUE to encode the lack of a bound" );
        }

        NavigableMap<ValueTuple,? extends LongDiffSets> sortedUpdates = txState.getSortedIndexUpdates( descriptor.schema() );
        if ( sortedUpdates == null )
        {
            return ReadableDiffSets.Empty.instance();
        }

        ValueTuple selectedLower;
        boolean selectedIncludeLower;

        ValueTuple selectedUpper;
        boolean selectedIncludeUpper;

        if ( lower == NO_VALUE )
        {
            selectedLower = ValueTuple.of( Values.minValue( valueGroup, upper ) );
            selectedIncludeLower = true;
        }
        else
        {
            selectedLower = ValueTuple.of( lower );
            selectedIncludeLower = includeLower;
        }

        if ( upper == NO_VALUE )
        {
            selectedUpper = ValueTuple.of( Values.maxValue( valueGroup, lower ) );
            selectedIncludeUpper = false;
        }
        else
        {
            selectedUpper = ValueTuple.of( upper );
            selectedIncludeUpper = includeUpper;
        }

        return indexUpdatesWithValuesForRangeSeek( sortedUpdates, selectedLower, selectedIncludeLower, selectedUpper, selectedIncludeUpper );
    }

    private static ReadableDiffSets<NodeWithPropertyValues> indexUpdatesWithValuesForRangeSeek( NavigableMap<ValueTuple,? extends LongDiffSets> sortedUpdates,
            ValueTuple selectedLower, boolean selectedIncludeLower, ValueTuple selectedUpper, boolean selectedIncludeUpper )
    {
        DiffSets<NodeWithPropertyValues> diffs = new DiffSets<>();

        Map<ValueTuple,? extends LongDiffSets> inRange = sortedUpdates.subMap( selectedLower, selectedIncludeLower, selectedUpper, selectedIncludeUpper );
        for ( Map.Entry<ValueTuple,? extends LongDiffSets> entry : inRange.entrySet() )
        {
            ValueTuple values = entry.getKey();
            Value[] valuesArray = values.getValues();
            LongDiffSets diffForSpecificValue = entry.getValue();

            diffForSpecificValue.getAdded().each( nodeId -> diffs.add( new NodeWithPropertyValues( nodeId, valuesArray ) ) );
            diffForSpecificValue.getRemoved().each( nodeId -> diffs.remove( new NodeWithPropertyValues( nodeId, valuesArray ) ) );
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
        for ( Map.Entry<ValueTuple,? extends LongDiffSets> entry : sortedUpdates.tailMap( floor ).entrySet() )
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

    static ReadableDiffSets<NodeWithPropertyValues> indexUpdatesWithValuesForRangeSeekByPrefix( ReadableTransactionState txState, IndexDescriptor descriptor,
            String prefix )
    {
        NavigableMap<ValueTuple,? extends LongDiffSets> sortedUpdates = txState.getSortedIndexUpdates( descriptor.schema() );
        if ( sortedUpdates == null )
        {
            return ReadableDiffSets.Empty.instance();
        }
        ValueTuple floor = ValueTuple.of( Values.stringValue( prefix ) );
        DiffSets<NodeWithPropertyValues> diffs = new DiffSets<>();
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
