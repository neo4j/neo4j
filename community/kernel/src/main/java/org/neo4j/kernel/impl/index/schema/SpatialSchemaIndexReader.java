/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import java.util.Arrays;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexQuery.ExactPredicate;
import org.neo4j.internal.kernel.api.IndexQuery.GeometryRangePredicate;
import org.neo4j.kernel.api.exceptions.index.IndexNotApplicableKernelException;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.storageengine.api.schema.IndexProgressor;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

import static java.lang.String.format;

public class SpatialSchemaIndexReader<KEY extends NativeSchemaKey, VALUE extends NativeSchemaValue> extends NativeSchemaIndexReader<KEY, VALUE>
{
    SpatialSchemaIndexReader( GBPTree<KEY,VALUE> tree, Layout<KEY,VALUE> layout, IndexSamplingConfig samplingConfig, IndexDescriptor descriptor )
    {
        super( tree, layout, samplingConfig, descriptor );
    }

    @Override
    void validateQuery( IndexOrder indexOrder, IndexQuery[] predicates )
    {
        if ( predicates.length != 1 )
        {
            throw new UnsupportedOperationException( "Spatial index doesn't handle composite queries" );
        }

        if ( indexOrder != IndexOrder.NONE )
        {
            throw new UnsupportedOperationException(
                    format( "Tried to query index with unsupported order %s. Supported orders for query %s are %s.", indexOrder, Arrays.toString( predicates ),
                            IndexOrder.NONE ) );
        }
    }

    @Override
    void initializeRangeForQuery( KEY treeKeyFrom, KEY treeKeyTo, IndexQuery[] predicates )
    {
        IndexQuery predicate = predicates[0];
        switch ( predicate.type() )
        {
        case exists:
            treeKeyFrom.initAsLowest();
            treeKeyTo.initAsHighest();
            break;
        case exact:
            ExactPredicate exactPredicate = (ExactPredicate) predicate;
            treeKeyFrom.from( Long.MIN_VALUE, exactPredicate.value() );
            treeKeyTo.from( Long.MAX_VALUE, exactPredicate.value() );
            break;
        case rangeGeometric:
            GeometryRangePredicate rangePredicate = (GeometryRangePredicate) predicate;
            initFromForRange( rangePredicate, treeKeyFrom );
            initToForRange( rangePredicate, treeKeyTo );
            break;
        default:
            throw new IllegalArgumentException( "IndexQuery of type " + predicate.type() + " is not supported." );
        }
    }

    private void initToForRange( GeometryRangePredicate rangePredicate, KEY treeKeyTo )
    {
        Value toValue = rangePredicate.toAsValue();
        if ( toValue.valueGroup() == ValueGroup.NO_VALUE )
        {
            treeKeyTo.initAsHighest();
        }
        else
        {
            treeKeyTo.from( rangePredicate.toInclusive() ? Long.MAX_VALUE : Long.MIN_VALUE, toValue );
            treeKeyTo.setEntityIdIsSpecialTieBreaker( true );
        }
    }

    private void initFromForRange( GeometryRangePredicate rangePredicate, KEY treeKeyFrom )
    {
        Value fromValue = rangePredicate.fromAsValue();
        if ( fromValue.valueGroup() == ValueGroup.NO_VALUE )
        {
            treeKeyFrom.initAsLowest();
        }
        else
        {
            treeKeyFrom.from( rangePredicate.fromInclusive() ? Long.MIN_VALUE : Long.MAX_VALUE, fromValue );
            treeKeyFrom.setEntityIdIsSpecialTieBreaker( true );
        }
    }

    @Override
    public boolean hasFullNumberPrecision( IndexQuery... predicates )
    {
        return false;
    }
}
