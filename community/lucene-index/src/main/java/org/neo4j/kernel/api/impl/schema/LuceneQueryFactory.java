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
package org.neo4j.kernel.api.impl.schema;

import java.util.Arrays;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.function.ThrowingPredicate;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.PropertyIndexQuery.EntityFilterPredicate;
import org.neo4j.internal.kernel.api.PropertyIndexQuery.NearestNeighborsPredicate;
import org.neo4j.internal.kernel.api.PropertyIndexQuery.StringContainsPredicate;
import org.neo4j.internal.kernel.api.PropertyIndexQuery.StringPrefixPredicate;
import org.neo4j.internal.kernel.api.PropertyIndexQuery.StringSuffixPredicate;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.impl.index.lucene.LuceneIndexSearcher;
import org.neo4j.kernel.api.impl.index.lucene.LuceneQueryContext;
import org.neo4j.kernel.api.impl.schema.vector.VectorDocumentStructure;
import org.neo4j.kernel.api.impl.schema.vector.VectorQuantizationType;

public abstract class LuceneQueryFactory {

    private static final PropertyIndexQuery[] NO_QUERIES = new PropertyIndexQuery[0];

    protected LuceneQueryFactory() {}

    public abstract LuceneQueryContext createQuery(
            LuceneIndexSearcher searcher,
            IndexQueryConstraints constraints,
            IndexDescriptor descriptor,
            PropertyIndexQuery... predicates);

    public static class TextQueryFactory extends LuceneQueryFactory {
        public static final LuceneQueryFactory INSTANCE = new TextQueryFactory();

        private TextQueryFactory() {}

        @Override
        public LuceneQueryContext createQuery(
                LuceneIndexSearcher searcher,
                IndexQueryConstraints constraints,
                IndexDescriptor descriptor,
                PropertyIndexQuery... predicates) {
            PropertyIndexQuery predicate = predicates[0];
            return switch (predicate.type()) {
                case ALL_ENTRIES -> searcher.newQueryContext().matchAll();
                case EXACT ->
                    TextDocumentStructure.newSeekQuery(
                            searcher, ((PropertyIndexQuery.ExactPredicate) predicate).value());
                case STRING_PREFIX -> {
                    StringPrefixPredicate spp = (PropertyIndexQuery.StringPrefixPredicate) predicate;
                    yield searcher.newQueryContext().stringPrefix(spp.prefix().stringValue());
                }
                case STRING_CONTAINS -> {
                    StringContainsPredicate scp = (PropertyIndexQuery.StringContainsPredicate) predicate;
                    yield searcher.newQueryContext()
                            .stringContains(scp.contains().stringValue());
                }
                case STRING_SUFFIX -> {
                    StringSuffixPredicate ssp = (PropertyIndexQuery.StringSuffixPredicate) predicate;
                    yield searcher.newQueryContext().stringSuffix(ssp.suffix().stringValue());
                }
                default -> throw invalidQuery(descriptor, predicate);
            };
        }
    }

    public static class TrigramQueryFactory extends LuceneQueryFactory {
        public static final LuceneQueryFactory INSTANCE = new TrigramQueryFactory();

        private TrigramQueryFactory() {}

        @Override
        public LuceneQueryContext createQuery(
                LuceneIndexSearcher searcher,
                IndexQueryConstraints constraints,
                IndexDescriptor descriptor,
                PropertyIndexQuery... predicates) {
            PropertyIndexQuery predicate = predicates[0];
            return switch (predicate.type()) {
                case ALL_ENTRIES -> searcher.newQueryContext().matchAll();
                case EXACT -> {
                    String value = ((PropertyIndexQuery.ExactPredicate) predicate)
                            .value()
                            .asObject()
                            .toString();
                    yield searcher.newQueryContext().trigramSearch(value);
                }
                case STRING_PREFIX -> {
                    StringPrefixPredicate spp = (PropertyIndexQuery.StringPrefixPredicate) predicate;
                    yield searcher.newQueryContext().trigramSearch(spp.prefix().stringValue());
                }
                case STRING_CONTAINS -> {
                    StringContainsPredicate scp = (PropertyIndexQuery.StringContainsPredicate) predicate;
                    yield searcher.newQueryContext()
                            .trigramSearch(scp.contains().stringValue());
                }
                case STRING_SUFFIX -> {
                    StringSuffixPredicate ssp = (PropertyIndexQuery.StringSuffixPredicate) predicate;
                    yield searcher.newQueryContext().trigramSearch(ssp.suffix().stringValue());
                }
                default -> throw invalidQuery(descriptor, predicate);
            };
        }
    }

    public static class VectorQueryFactory extends LuceneQueryFactory {
        private final VectorDocumentStructure documentStructure;
        private final VectorQuantizationType quantizationType;
        private final double defaultSearchExpansionFactor;
        private final int maxEfSearch;

        public VectorQueryFactory(
                VectorDocumentStructure documentStructure,
                VectorQuantizationType quantizationType,
                double defaultSearchExpansionFactor,
                int maxEfSearch) {
            this.documentStructure = documentStructure;
            this.quantizationType = quantizationType;
            this.defaultSearchExpansionFactor = defaultSearchExpansionFactor;
            this.maxEfSearch = maxEfSearch;
        }

        @Override
        public LuceneQueryContext createQuery(
                LuceneIndexSearcher searcher,
                IndexQueryConstraints constraints,
                IndexDescriptor descriptor,
                PropertyIndexQuery... predicates) {
            PropertyIndexQuery predicate = predicates[0];
            return switch (predicate.type()) {
                case ALL_ENTRIES -> searcher.newQueryContext().matchAll();
                case NEAREST_NEIGHBORS -> {
                    NearestNeighborsPredicate nearestNeighborsPredicate =
                            (PropertyIndexQuery.NearestNeighborsPredicate) predicate;
                    int k = nearestNeighborsPredicate.numberOfNeighbors(constraints, maxEfSearch);
                    double searchExpansionFactor =
                            nearestNeighborsPredicate.searchExpansionFactorOrElse(defaultSearchExpansionFactor);
                    int efSearch = Math.clamp((long) Math.ceil(searchExpansionFactor * k), k, maxEfSearch);
                    boolean rescore = quantizationType != VectorQuantizationType.NONE && efSearch > k;

                    if (predicates.length > 1) {
                        yield searcher.newQueryContext()
                                .approximateNearestNeighbors(
                                        documentStructure,
                                        nearestNeighborsPredicate.query(),
                                        k,
                                        efSearch,
                                        rescore,
                                        extractEntityFilter(predicates),
                                        extractPropertyFilters(predicates));
                    } else {
                        yield searcher.newQueryContext()
                                .approximateNearestNeighbors(
                                        documentStructure, nearestNeighborsPredicate.query(), k, efSearch, rescore);
                    }
                }
                default -> throw invalidQuery(descriptor, predicate);
            };
        }
    }

    static PropertyIndexQuery[] extractPropertyFilters(PropertyIndexQuery[] predicates) {
        if (hasEntityFilter(predicates)) {
            return predicates.length > 1
                    ? Arrays.copyOfRange(predicates, 2, predicates.length, PropertyIndexQuery[].class)
                    : NO_QUERIES;
        } else {
            return predicates.length > 0
                    ? Arrays.copyOfRange(predicates, 1, predicates.length, PropertyIndexQuery[].class)
                    : NO_QUERIES;
        }
    }

    static EntityFilterPredicate extractEntityFilter(PropertyIndexQuery[] predicates) {
        if (hasEntityFilter(predicates)) {
            return (EntityFilterPredicate) predicates[1];
        } else {
            return PropertyIndexQuery.matchAllEntityFilter();
        }
    }

    static boolean hasEntityFilter(PropertyIndexQuery[] predicates) {
        return predicates.length > 1 && predicates[1] instanceof EntityFilterPredicate;
    }

    public static <E extends Exception> boolean propertyFiltersForAll(
            PropertyIndexQuery[] predicates, ThrowingPredicate<PropertyIndexQuery, E> consumer) throws E {
        int i = hasEntityFilter(predicates) ? 2 : 1;
        for (; i < predicates.length; i++) {
            if (!consumer.test(predicates[i])) {
                return false;
            }
        }
        return true;
    }

    public static <E extends Exception> void propertyFiltersForEach(
            PropertyIndexQuery[] predicates, ThrowingConsumer<PropertyIndexQuery, E> consumer) throws E {
        int i = hasEntityFilter(predicates) ? 2 : 1;
        for (; i < predicates.length; i++) {
            consumer.accept(predicates[i]);
        }
    }

    protected static IllegalArgumentException invalidQuery(IndexDescriptor descriptor, PropertyIndexQuery predicate) {
        return new IllegalArgumentException(
                "Index query not supported for %s index. Query: %s".formatted(descriptor.getIndexType(), predicate));
    }
}
