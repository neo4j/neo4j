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
package org.neo4j.kernel.impl.index.schema;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexQuery.ExactPredicate;
import org.neo4j.internal.kernel.api.IndexQuery.RangePredicate;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.Values;

class NumberIndexReader<VALUE extends NativeIndexValue> extends NativeIndexReader<NumberIndexKey,VALUE>
{
    NumberIndexReader( GBPTree<NumberIndexKey,VALUE> tree, IndexLayout<NumberIndexKey,VALUE> layout, IndexDescriptor descriptor )
    {
        super( tree, layout, descriptor );
    }

    @Override
    void validateQuery( IndexOrder indexOrder, IndexQuery[] predicates )
    {
        if ( predicates.length != 1 )
        {
            throw new UnsupportedOperationException();
        }

        CapabilityValidator.validateQuery( NumberIndexProvider.CAPABILITY, indexOrder, predicates );
    }

    @Override
    boolean initializeRangeForQuery( NumberIndexKey treeKeyFrom, NumberIndexKey treeKeyTo, IndexQuery[] predicates )
    {
        IndexQuery predicate = predicates[0];
        switch ( predicate.type() )
        {
        case exists:
            treeKeyFrom.initValueAsLowest( ValueGroup.NUMBER );
            treeKeyTo.initValueAsHighest( ValueGroup.NUMBER );
            break;
        case exact:
            ExactPredicate exactPredicate = (ExactPredicate) predicate;
            treeKeyFrom.from( exactPredicate.value() );
            treeKeyTo.from( exactPredicate.value() );
            break;
        case range:
            RangePredicate<?> rangePredicate = (RangePredicate<?>) predicate;
            initFromForRange( rangePredicate, treeKeyFrom );
            initToForRange( rangePredicate, treeKeyTo );
            break;
        default:
            throw new IllegalArgumentException( "IndexQuery of type " + predicate.type() + " is not supported." );
        }
        return false;
    }

    private static void initToForRange( RangePredicate<?> rangePredicate, NumberIndexKey treeKeyTo )
    {
        Value toValue = rangePredicate.toValue();
        if ( toValue == Values.NO_VALUE )
        {
            treeKeyTo.initValueAsHighest( ValueGroup.NUMBER );
        }
        else
        {
            treeKeyTo.initialize( rangePredicate.toInclusive() ? Long.MAX_VALUE : Long.MIN_VALUE );
            treeKeyTo.from( toValue );
            treeKeyTo.setCompareId( true );
        }
    }

    private static void initFromForRange( RangePredicate<?> rangePredicate, NumberIndexKey treeKeyFrom )
    {
        Value fromValue = rangePredicate.fromValue();
        if ( fromValue == Values.NO_VALUE )
        {
            treeKeyFrom.initValueAsLowest( ValueGroup.NUMBER );
        }
        else
        {
            treeKeyFrom.initialize( rangePredicate.fromInclusive() ? Long.MIN_VALUE : Long.MAX_VALUE );
            treeKeyFrom.from( fromValue );
            treeKeyFrom.setCompareId( true );
        }
    }

    @Override
    public boolean hasFullValuePrecision( IndexQuery... predicates )
    {
        return true;
    }
}
