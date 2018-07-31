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
package org.neo4j.kernel.impl.index.schema;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexQuery.ExactPredicate;
import org.neo4j.internal.kernel.api.IndexQuery.RangePredicate;
import org.neo4j.internal.kernel.api.IndexQuery.StringPrefixPredicate;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.Values;

import static org.neo4j.kernel.impl.index.schema.NativeIndexKey.Inclusion.HIGH;
import static org.neo4j.kernel.impl.index.schema.NativeIndexKey.Inclusion.LOW;
import static org.neo4j.kernel.impl.index.schema.NativeIndexKey.Inclusion.NEUTRAL;

class GenericNativeIndexReader extends NativeIndexReader<CompositeGenericKey,NativeIndexValue>
{
    GenericNativeIndexReader( GBPTree<CompositeGenericKey,NativeIndexValue> tree, IndexLayout<CompositeGenericKey,NativeIndexValue> layout,
            IndexSamplingConfig samplingConfig, IndexDescriptor descriptor )
    {
        super( tree, layout, samplingConfig, descriptor );
    }

    @Override
    public boolean hasFullValuePrecision( IndexQuery... predicates )
    {
        // TODO except spatial tho
        return true;
    }

    @Override
    void validateQuery( IndexOrder indexOrder, IndexQuery[] predicates )
    {
        CapabilityValidator.validateQuery( GenericNativeIndexProvider.CAPABILITY, indexOrder, predicates );
    }

    @Override
    boolean initializeRangeForQuery( CompositeGenericKey treeKeyFrom, CompositeGenericKey treeKeyTo, IndexQuery[] predicates )
    {
        boolean needsFiltering = false;
        for ( int i = 0; i < predicates.length; i++ )
        {
            IndexQuery predicate = predicates[i];
            switch ( predicate.type() )
            {
            case exists:
                treeKeyFrom.initValueAsLowest( i, ValueGroup.UNKNOWN );
                treeKeyTo.initValueAsHighest( i, ValueGroup.UNKNOWN );
                break;
            case exact:
                ExactPredicate exactPredicate = (ExactPredicate) predicate;
                treeKeyFrom.initFromValue( i, exactPredicate.value(), NEUTRAL );
                treeKeyTo.initFromValue( i, exactPredicate.value(), NEUTRAL );
                break;
            case range:
                RangePredicate<?> rangePredicate = (RangePredicate<?>) predicate;
                initFromForRange( i, rangePredicate, treeKeyFrom );
                initToForRange( i, rangePredicate, treeKeyTo );
                break;
            case stringPrefix:
                StringPrefixPredicate prefixPredicate = (StringPrefixPredicate) predicate;
                treeKeyFrom.initAsPrefixLow( i, prefixPredicate.prefix() );
                treeKeyTo.initAsPrefixHigh( i, prefixPredicate.prefix() );
                break;
            case stringSuffix:
            case stringContains:
                treeKeyFrom.initValueAsLowest( i, ValueGroup.TEXT );
                treeKeyTo.initValueAsHighest( i, ValueGroup.TEXT );
                needsFiltering = true;
                break;
            default:
                throw new IllegalArgumentException( "IndexQuery of type " + predicate.type() + " is not supported." );
            }
        }
        return needsFiltering;
    }

    private static void initFromForRange( int stateSlot, RangePredicate<?> rangePredicate, CompositeGenericKey treeKeyFrom )
    {
        Value fromValue = rangePredicate.fromValue();
        if ( fromValue == Values.NO_VALUE )
        {
            treeKeyFrom.initValueAsLowest( stateSlot, ValueGroup.UNKNOWN );
        }
        else
        {
            treeKeyFrom.initFromValue( stateSlot, fromValue, rangePredicate.fromInclusive() ? LOW : HIGH );
            treeKeyFrom.setCompareId( true );
        }
    }

    private static void initToForRange( int stateSlot, RangePredicate<?> rangePredicate, CompositeGenericKey treeKeyTo )
    {
        Value toValue = rangePredicate.toValue();
        if ( toValue == Values.NO_VALUE )
        {
            treeKeyTo.initValueAsHighest( stateSlot, ValueGroup.UNKNOWN );
        }
        else
        {
            treeKeyTo.initFromValue( stateSlot, toValue, rangePredicate.toInclusive() ? HIGH : LOW );
            treeKeyTo.setCompareId( true );
        }
    }
}
