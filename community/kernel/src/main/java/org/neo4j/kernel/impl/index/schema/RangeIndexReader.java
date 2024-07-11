/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.index.schema;

import static java.lang.String.format;
import static org.neo4j.kernel.impl.index.schema.NativeIndexKey.Inclusion.HIGH;
import static org.neo4j.kernel.impl.index.schema.NativeIndexKey.Inclusion.LOW;
import static org.neo4j.kernel.impl.index.schema.NativeIndexKey.Inclusion.NEUTRAL;
import static org.neo4j.kernel.impl.index.schema.RangeIndexProvider.CAPABILITY;

import java.util.Arrays;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.internal.schema.IndexQuery.IndexQueryType;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.Values;

public class RangeIndexReader extends NativeIndexReader<RangeKey> {
    RangeIndexReader(
            GBPTree<RangeKey, NullValue> tree,
            IndexLayout<RangeKey> layout,
            IndexDescriptor descriptor,
            IndexUsageTracking usageTracker) {
        super(tree, layout, descriptor, usageTracker);
    }

    @Override
    void validateQuery(IndexQueryConstraints constraints, PropertyIndexQuery... predicates) {
        validateNoUnsupportedPredicates(predicates);
        validateOrder(constraints.order(), predicates);
        validateCompositeQuery(predicates);
    }

    @Override
    boolean initializeRangeForQuery(RangeKey treeKeyFrom, RangeKey treeKeyTo, PropertyIndexQuery... predicates) {
        if (isAllQuery(predicates)) {
            initializeAllSlotsForFullRange(treeKeyFrom, treeKeyTo);
            return false;
        }

        for (int i = 0; i < predicates.length; i++) {
            final var predicate = predicates[i];
            switch (predicate.type()) {
                case EXISTS -> {
                    treeKeyFrom.initValueAsLowest(i, ValueGroup.UNKNOWN);
                    treeKeyTo.initValueAsHighest(i, ValueGroup.UNKNOWN);
                }

                case EXACT -> {
                    final var exactPredicate = (PropertyIndexQuery.ExactPredicate) predicate;
                    treeKeyFrom.initFromValue(i, exactPredicate.value(), NEUTRAL);
                    treeKeyTo.initFromValue(i, exactPredicate.value(), NEUTRAL);
                }

                case RANGE -> {
                    final var rangePredicate = (PropertyIndexQuery.RangePredicate<?>) predicate;
                    initFromForRange(i, rangePredicate, treeKeyFrom);
                    initToForRange(i, rangePredicate, treeKeyTo);
                }

                case STRING_PREFIX -> {
                    final var prefixPredicate = (PropertyIndexQuery.StringPrefixPredicate) predicate;
                    treeKeyFrom.stateSlot(i).initAsPrefixLow(prefixPredicate.prefix());
                    treeKeyTo.stateSlot(i).initAsPrefixHigh(prefixPredicate.prefix());
                }

                default -> throw new IllegalArgumentException(
                        "IndexQuery of type " + predicate.type() + " is not supported.");
            }
        }
        return false;
    }

    // all slots are required to be initialized such that the keys can be copied when scanning in parallel
    private static boolean isAllQuery(PropertyIndexQuery[] predicates) {
        return predicates.length == 1 && predicates[0].type() == IndexQueryType.ALL_ENTRIES;
    }

    private static void initializeAllSlotsForFullRange(RangeKey treeKeyFrom, RangeKey treeKeyTo) {
        assert treeKeyFrom.numberOfStateSlots() == treeKeyTo.numberOfStateSlots();
        for (int i = 0; i < treeKeyFrom.numberOfStateSlots(); i++) {
            treeKeyFrom.initValueAsLowest(i, ValueGroup.UNKNOWN);
            treeKeyTo.initValueAsHighest(i, ValueGroup.UNKNOWN);
        }
    }

    private static void initFromForRange(
            int stateSlot, PropertyIndexQuery.RangePredicate<?> rangePredicate, RangeKey treeKeyFrom) {
        Value fromValue = rangePredicate.fromValue();
        if (fromValue == Values.NO_VALUE) {
            treeKeyFrom.initValueAsLowest(stateSlot, rangePredicate.valueGroup());
        } else {
            treeKeyFrom.initFromValue(stateSlot, fromValue, fromInclusion(rangePredicate));
            treeKeyFrom.setCompareId(true);
        }
    }

    private static void initToForRange(
            int stateSlot, PropertyIndexQuery.RangePredicate<?> rangePredicate, RangeKey treeKeyTo) {
        Value toValue = rangePredicate.toValue();
        if (toValue == Values.NO_VALUE) {
            treeKeyTo.initValueAsHighest(stateSlot, rangePredicate.valueGroup());
        } else {
            treeKeyTo.initFromValue(stateSlot, toValue, toInclusion(rangePredicate));
            treeKeyTo.setCompareId(true);
        }
    }

    private static NativeIndexKey.Inclusion fromInclusion(PropertyIndexQuery.RangePredicate<?> rangePredicate) {
        return rangePredicate.fromInclusive() ? LOW : HIGH;
    }

    private static NativeIndexKey.Inclusion toInclusion(PropertyIndexQuery.RangePredicate<?> rangePredicate) {
        return rangePredicate.toInclusive() ? HIGH : LOW;
    }

    private static void validateNoUnsupportedPredicates(PropertyIndexQuery[] predicates) {
        for (final var predicate : predicates) {
            final var type = predicate.type();
            switch (type) {
                case TOKEN_LOOKUP,
                        BOUNDING_BOX,
                        STRING_CONTAINS,
                        STRING_SUFFIX,
                        FULLTEXT_SEARCH -> throw new IllegalArgumentException(format(
                        "Tried to query index with illegal query. A %s predicate is not allowed for a %s index. Query was :%s",
                        type, IndexType.RANGE, Arrays.toString(predicates)));
                default -> {}
            }
        }
    }

    static void validateOrder(IndexOrder indexOrder, PropertyIndexQuery... predicates) {
        if (indexOrder == IndexOrder.NONE) {
            return;
        }

        if (!CAPABILITY.supportsOrdering()) {
            invalidOrder(indexOrder, predicates);
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
     * Supported queries in order of precision:
     * - {@link IndexQueryType#EXACT exact}
     * - {@link IndexQueryType#RANGE range}, {@link IndexQueryType#STRING_PREFIX prefix}
     * - {@link IndexQueryType#EXISTS exists}
     *
     * 2. AllEntries
     * The {@link IndexQueryType#ALL_ENTRIES all entries} query is not allowed in composite queries, due to its semantic
     * meaning to return everything in the index; thus does a full scan of the search
     * space. This is similar to "exist", but is tied to the index, rather than a
     * particular property key.
     *
     * @param predicates The query for which we want to check the composite validity.
     */
    static void validateCompositeQuery(PropertyIndexQuery... predicates) {
        for (int i = 1; i < predicates.length; i++) {
            final var type = predicates[i].type();
            final var prevType = predicates[i - 1].type();
            if (prevType == IndexQueryType.ALL_ENTRIES) {
                invalidQueryInComposite(prevType, predicates);
            }

            switch (type) {
                case EXISTS -> {
                    // all predicates that are supported in a composite query, can be followed by an EXISTS, as EXISTS
                    // has the least precision; thus, if current type is EXISTS, then previous type is allowed to be any
                    // type that is valid for composite queries
                }
                case EXACT, RANGE, STRING_PREFIX -> {
                    // all other predicates that are supported in a composite query, can _only_ follow an EXACT;
                    // if the previous type is not an EXACT, then the precision is not decreasing
                    if (prevType != IndexQueryType.EXACT) {
                        invalidQueryPrecisionInComposite(predicates);
                    }
                }

                default -> invalidQueryInComposite(type, predicates);
            }
        }
    }

    private static void invalidOrder(IndexOrder indexOrder, PropertyIndexQuery... predicates) {
        throw new UnsupportedOperationException(format(
                "Tried to query index with unsupported order %s. For query %s supports ascending: false, supports descending: false.",
                indexOrder, Arrays.toString(predicates)));
    }

    private static void invalidQueryInComposite(IndexQueryType type, PropertyIndexQuery... predicates) {
        throw new IllegalArgumentException(format(
                "Tried to query index with illegal composite query. %s queries are not allowed in composite query. Query was: %s ",
                type, Arrays.toString(predicates)));
    }

    private static void invalidQueryPrecisionInComposite(PropertyIndexQuery... predicates) {
        throw new IllegalArgumentException(format(
                "Tried to query index with illegal composite query. Composite query must have decreasing precision. Query was: %s ",
                Arrays.toString(predicates)));
    }
}
