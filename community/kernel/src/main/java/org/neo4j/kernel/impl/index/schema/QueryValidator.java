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
package org.neo4j.kernel.impl.index.schema;

import java.util.Arrays;

import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.schema.IndexCapability;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.internal.schema.IndexOrderCapability;
import org.neo4j.values.storable.ValueCategory;

import static java.lang.String.format;

class QueryValidator
{
    static void validateOrder( IndexCapability capability, IndexOrder indexOrder, IndexQuery[] predicates )
    {
        if ( indexOrder != IndexOrder.NONE )
        {
            ValueCategory valueCategory = predicates[0].valueGroup().category();
            IndexOrderCapability orderCapability = capability.orderCapability( valueCategory );

            if ( indexOrder == IndexOrder.ASCENDING && !orderCapability.supportsAsc() ||
                 indexOrder == IndexOrder.DESCENDING && !orderCapability.supportsDesc() )
            {
                throw new UnsupportedOperationException(
                        format( "Tried to query index with unsupported order %s. For query %s supports ascending: %b, supports descending: %b.", indexOrder,
                                Arrays.toString( predicates ), orderCapability.supportsAsc(), orderCapability.supportsDesc() ) );
            }
        }
    }

    /**
     * Composite queries are somewhat restricted in what combination of predicates
     * that are allowed together and in what order.
     *
     * 1. Decreasing precision.
     * Composite queries must have decreasing precision on the slots, meaning
     * for example that a range predicate can not be followed by an exact
     * predicate.
     * The reason for this is that because the index is sorted in lexicographic
     * order with regards to the slots a predicate with increasing precision
     * does not narrow down the search space in the index.
     * It could of course be implemented by scanning the search space and do
     * post filtering, but this is not how the implementation currently works.
     *
     * 2. Contains and suffix.
     * Contains or suffix queries are not allowed in composite queries at all.
     * This is because they both demand a full scan of the search space, just like
     * "exist", and the post filtering of the result. This is how it works in the
     * non-composite case but it has not yet been implemented for the composite case.
     *
     * @param predicates The query for which we want to check the composite validity.
     */
    static void validateCompositeQuery( IndexQuery[] predicates )
    {
        IndexQuery prev = null;
        for ( IndexQuery current : predicates )
        {
            String illegalQueryMessage = "Tried to query index with illegal composite query.";
            if ( current instanceof IndexQuery.StringContainsPredicate ||
                    current instanceof IndexQuery.StringSuffixPredicate )
            {
                if ( predicates.length > 1 )
                {
                    throw new IllegalArgumentException( format( "%s Suffix or contains predicate are not allowed in composite query. Query was: %s ",
                            illegalQueryMessage, Arrays.toString( predicates ) ) );
                }
            }
            if ( prev instanceof IndexQuery.RangePredicate ||
                    prev instanceof IndexQuery.StringPrefixPredicate ||
                    prev instanceof IndexQuery.ExistsPredicate )
            {
                if ( !(current instanceof IndexQuery.ExistsPredicate) )
                {
                    throw new IllegalArgumentException( format( "%s Composite query must have decreasing precision. Query was: %s ",
                            illegalQueryMessage, Arrays.toString( predicates ) ) );
                }
            }
            prev = current;
        }
    }
}
