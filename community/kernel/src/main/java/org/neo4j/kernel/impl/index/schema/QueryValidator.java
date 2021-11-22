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
package org.neo4j.kernel.impl.index.schema;

import java.util.Arrays;

import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.schema.IndexCapability;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.internal.schema.IndexOrderCapability;
import org.neo4j.internal.schema.IndexQuery.IndexQueryType;
import org.neo4j.values.storable.ValueCategory;

import static java.lang.String.format;

class QueryValidator
{
    static void validateOrder( IndexCapability capability, IndexOrder indexOrder, PropertyIndexQuery... predicates )
    {
        if ( indexOrder == IndexOrder.NONE )
        {
            return;
        }

        final var valueCategories = new ValueCategory[predicates.length];
        for ( int i = 0; i < predicates.length; i++ )
        {
            valueCategories[i] = predicates[i].valueGroup().category();
        }

        final var orderCapability = capability.orderCapability( valueCategories );
        if ( indexOrder == IndexOrder.ASCENDING && !orderCapability.supportsAsc()
             || indexOrder == IndexOrder.DESCENDING && !orderCapability.supportsDesc() )
        {
            invalidOrder( indexOrder, orderCapability, predicates );
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
     * order in regard to the slots a predicate with increasing precision
     * does not narrow down the search space in the index.
     * It could of course be implemented by scanning the search space and do
     * post filtering, but this is not how the implementation currently works.
     *
     * 2. Contains and suffix.
     * Contains or suffix queries are not allowed in composite queries at all.
     * This is because they both demand a full scan of the search space, just like
     * "exist", and the post filtering of the result. This is how it works in the
     * non-composite case, but it has not yet been implemented for the composite case.
     *
     * 3. AllEntries
     * The allEntries query also is not allowed in composite queries, due to its semantic
     * meaning to return everything in the index; thus does a full scan of the search
     * space. This is similar to "exist", but is tied to the index, rather than a
     * particular property key.
     *
     * @param predicates The query for which we want to check the composite validity.
     */
    static void validateCompositeQuery( PropertyIndexQuery... predicates )
    {
        if ( predicates.length == 1 && predicates[0].type() == IndexQueryType.FULLTEXT_SEARCH )
        {
            invalidQuerySingular( IndexQueryType.FULLTEXT_SEARCH, predicates );
        }

        for ( int i = 1; i < predicates.length; i++ )
        {
            final var type = predicates[i].type();
            final var prevType = predicates[i - 1].type();

            switch ( type )
            {
                case EXISTS ->
                {
                    // all predicates that are supported in a composite query, can be followed by an EXISTS, as EXISTS has the least precision;
                    // thus, if current type is EXISTS, then previous type is allowed to be any type that is valid for composite queries
                    switch ( prevType )
                    {
                        case EXISTS, EXACT, RANGE, BOUNDING_BOX, STRING_PREFIX -> { }
                        default -> invalidQueryInComposite( prevType, predicates );
                    }
                }

                case EXACT, RANGE, BOUNDING_BOX, STRING_PREFIX ->
                {
                    // all other predicates that are supported in a composite query, can _only_ follow an EXACT;
                    // if the previous type is not an EXACT, then the precision is not decreasing
                    if ( prevType != IndexQueryType.EXACT )
                    {
                        invalidQueryPrecisionInComposite( predicates );
                    }
                }

                default -> invalidQueryInComposite( type, predicates );
            }
        }
    }

    private static void invalidOrder( IndexOrder indexOrder, IndexOrderCapability orderCapability, PropertyIndexQuery... predicates )
    {
        throw new UnsupportedOperationException(
                format( "Tried to query index with unsupported order %s. For query %s supports ascending: %b, supports descending: %b.",
                        indexOrder, Arrays.toString( predicates ), orderCapability.supportsAsc(), orderCapability.supportsDesc() ) );
    }

    private static void invalidQuerySingular( IndexQueryType type, PropertyIndexQuery... predicates )
    {
        throw new IllegalArgumentException(
                format( "Tried to query index with illegal composite query. %s queries are not allowed on this index. Query was: %s ",
                        type, Arrays.toString( predicates ) ) );
    }

    private static void invalidQueryInComposite( IndexQueryType type, PropertyIndexQuery... predicates )
    {
        throw new IllegalArgumentException(
                format( "Tried to query index with illegal composite query. %s queries are not allowed in composite query. Query was: %s ",
                        type, Arrays.toString( predicates ) ) );
    }

    private static void invalidQueryPrecisionInComposite( PropertyIndexQuery... predicates )
    {
        throw new IllegalArgumentException(
                format( "Tried to query index with illegal composite query. Composite query must have decreasing precision. Query was: %s ",
                        Arrays.toString( predicates ) ) );
    }
}
