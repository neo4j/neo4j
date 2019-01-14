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
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

class TemporalIndexPartReader<KEY extends NativeSchemaKey<KEY>> extends NativeSchemaIndexReader<KEY,NativeSchemaValue>
{
    TemporalIndexPartReader( GBPTree<KEY,NativeSchemaValue> tree,
                             Layout<KEY,NativeSchemaValue> layout,
                             IndexSamplingConfig samplingConfig,
                             SchemaIndexDescriptor descriptor )
    {
        super( tree, layout, samplingConfig, descriptor );
    }

    @Override
    protected void validateQuery( IndexOrder indexOrder, IndexQuery[] predicates )
    {
        if ( predicates.length != 1 )
        {
            throw new UnsupportedOperationException();
        }

        CapabilityValidator.validateQuery( TemporalIndexProvider.CAPABILITY, indexOrder, predicates );
    }

    @Override
    protected boolean initializeRangeForQuery( KEY treeKeyFrom, KEY treeKeyTo, IndexQuery[] predicates )
    {
        IndexQuery predicate = predicates[0];
        switch ( predicate.type() )
        {
        case exists:
            treeKeyFrom.initAsLowest();
            treeKeyTo.initAsHighest();
            break;

        case exact:
            IndexQuery.ExactPredicate exactPredicate = (IndexQuery.ExactPredicate) predicate;
            treeKeyFrom.from( Long.MIN_VALUE, exactPredicate.value() );
            treeKeyTo.from( Long.MAX_VALUE, exactPredicate.value() );
            break;

        case range:
            IndexQuery.RangePredicate<?> rangePredicate = (IndexQuery.RangePredicate<?>) predicate;
            initFromForRange( rangePredicate, treeKeyFrom );
            initToForRange( rangePredicate, treeKeyTo );
            break;

        default:
            throw new IllegalArgumentException( "IndexQuery of type " + predicate.type() + " is not supported." );
        }
        return false; // no filtering
    }

    private void initFromForRange( IndexQuery.RangePredicate<?> rangePredicate, KEY treeKeyFrom )
    {
        Value fromValue = rangePredicate.fromValue();
        if ( fromValue == Values.NO_VALUE )
        {
            treeKeyFrom.initAsLowest();
        }
        else
        {
            treeKeyFrom.from( rangePredicate.fromInclusive() ? Long.MIN_VALUE : Long.MAX_VALUE, fromValue );
            treeKeyFrom.setCompareId( true );
        }
    }

    private void initToForRange( IndexQuery.RangePredicate<?> rangePredicate, KEY treeKeyTo )
    {
        Value toValue = rangePredicate.toValue();
        if ( toValue == Values.NO_VALUE )
        {
            treeKeyTo.initAsHighest();
        }
        else
        {
            treeKeyTo.from( rangePredicate.toInclusive() ? Long.MAX_VALUE : Long.MIN_VALUE, toValue );
            treeKeyTo.setCompareId( true );
        }
    }

    @Override
    public boolean hasFullValuePrecision( IndexQuery... predicates )
    {
        return true;
    }
}
