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
package org.neo4j.kernel.api.impl.index.lucene.v9;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SequencedCollection;
import java.util.function.LongPredicate;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.kernel.api.impl.index.collector.ScoredEntityIterator;
import org.neo4j.kernel.api.impl.index.collector.ValuesIterator;
import org.neo4j.kernel.api.impl.index.lucene.LuceneIndexSearcher;
import org.neo4j.kernel.api.impl.index.lucene.LucenePartitionedSearch;
import org.neo4j.kernel.api.impl.index.lucene.LuceneQueryContext;
import org.neo4j.kernel.api.impl.schema.fulltext.LuceneFulltextDocumentStructure;
import org.neo4j.shaded.lucene9.index.Term;
import org.neo4j.shaded.lucene9.index.TermStates;
import org.neo4j.shaded.lucene9.search.CollectionStatistics;
import org.neo4j.shaded.lucene9.search.IndexSearcher;
import org.neo4j.shaded.lucene9.search.TermStatistics;
import org.neo4j.shaded.lucene9.search.Weight;
import org.neo4j.shaded.lucene9.util.BytesRef;

class Lucene9PartitionedSearch implements LucenePartitionedSearch {
    private final List<PreparedSearch> searches;

    Lucene9PartitionedSearch(int size) {
        searches = new ArrayList<>(size);
    }

    @Override
    public void addPartitionSearcher(LuceneIndexSearcher indexSearcher, LongPredicate filter) {
        searches.add(new PreparedSearch(((Lucene9IndexSearcher) indexSearcher).indexSearcher, filter));
    }

    @Override
    public ValuesIterator search(LuceneQueryContext queryContext, IndexQueryConstraints constraints)
            throws IOException {
        StatsCollector statsCollector = new StatsCollector(searches);
        List<ValuesIterator> results = new ArrayList<>(searches.size());
        for (PreparedSearch preparedSearch : searches) {
            FulltextResultCollector collector = new FulltextResultCollector(constraints, preparedSearch.filter);

            // Weights are bonded with the top IndexReaderContext of the index searcher that they are created for.
            // That's why we have to create a new StatsCachingIndexSearcher, and a new weight, for every index
            // partition.
            // However, the important thing is that we re-use the statsCollector.
            StatsCachingIndexSearcher statsCachingIndexSearcher =
                    new StatsCachingIndexSearcher(preparedSearch.indexSearcher, statsCollector);
            Weight weight = statsCachingIndexSearcher.createWeight(
                    ((Lucene9QueryContext) queryContext).build(), collector.scoreMode(), 1);

            ((Lucene9Neo4jIndexSearcher) preparedSearch.indexSearcher).search(weight, collector);

            results.add(collector.iterator());
        }

        return ScoredEntityIterator.mergeIterators(results);
    }

    private record PreparedSearch(IndexSearcher indexSearcher, LongPredicate filter) {}

    /**
     * Collect, aggregate and cache Lucene index statistics that span multiple index searchers.
     */
    private static class StatsCollector {
        private final List<PreparedSearch> searches;
        private final Map<Term, Optional<TermStatistics>> termStatisticsCache;
        private final Map<String, Optional<CollectionStatistics>> collStatisticsCache;

        StatsCollector(List<PreparedSearch> searches) {
            this.searches = searches;
            termStatisticsCache = new HashMap<>();
            collStatisticsCache = new HashMap<>();
        }

        TermStatistics termStatistics(Term term) {
            return termStatisticsCache
                    .computeIfAbsent(term, this::computeTermStatistics)
                    .orElse(null);
        }

        private Optional<TermStatistics> computeTermStatistics(Term term) {
            TermStatistics result;
            SequencedCollection<TermStatistics> statistics = new ArrayList<>(searches.size());
            for (PreparedSearch preparedSearch : searches) {
                IndexSearcher searcher = preparedSearch.indexSearcher;
                try {
                    TermStates context = TermStates.build(searcher, term, true);
                    if (context.docFreq() > 0) {
                        TermStatistics statistic =
                                searcher.termStatistics(term, context.docFreq(), context.totalTermFreq());
                        statistics.add(statistic);
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            long docFreq = 0;
            long totalTermFreq = 0;
            for (TermStatistics statistic : statistics) {
                docFreq += statistic.docFreq();
                totalTermFreq += statistic.totalTermFreq();
            }
            if (docFreq == 0) {
                return Optional.empty();
            }
            BytesRef bytesTerm = statistics.getFirst().term();
            result = new TermStatistics(bytesTerm, docFreq, totalTermFreq);
            return Optional.of(result);
        }

        CollectionStatistics collectionStatistics(String field) {
            return collStatisticsCache
                    .computeIfAbsent(field, this::computeCollStatistics)
                    .orElse(null);
        }

        private Optional<CollectionStatistics> computeCollStatistics(String field) {
            try {
                long maxDoc = 0;
                long docCount = 0;
                long sumTotalTermFreq = 0;
                long sumDocFreq = 0;
                for (PreparedSearch preparedSearch : searches) {
                    CollectionStatistics statistic = preparedSearch.indexSearcher.collectionStatistics(field);
                    if (statistic != null) {
                        maxDoc += statistic.maxDoc();
                        docCount += statistic.docCount();
                        sumTotalTermFreq += statistic.sumTotalTermFreq();
                        sumDocFreq += statistic.sumDocFreq();
                    }
                }
                if (docCount == 0) {
                    return Optional.empty();
                }
                return Optional.of(new CollectionStatistics(field, maxDoc, docCount, sumTotalTermFreq, sumDocFreq));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    /**
     * An index searcher implementation delegates to the given {@link StatsCollector} for computing its term and collection statistics.
     * This makes it possible for this index searcher to create weights and scorers that are calibrated to the aggregate statistics of multiple indexes.
     * Aggregating the statistics is useful when a full-text index spans multiple partitions, or when transaction state needs to be taken into account as well.
     * Without the aggregate statistics, the scores computed from each search in the individual partitions, will not be comparable.
     */
    private static class StatsCachingIndexSearcher extends IndexSearcher {
        private final StatsCollector collector;

        StatsCachingIndexSearcher(IndexSearcher searcher, StatsCollector collector) {
            super(searcher.getTopReaderContext(), searcher.getExecutor());
            this.collector = collector;
        }

        @Override
        public TermStatistics termStatistics(Term term, int docFreq, long totalTermFreq) {
            return collector.termStatistics(term);
        }

        @Override
        public CollectionStatistics collectionStatistics(String field) {
            return collector.collectionStatistics(field);
        }
    }

    private static class FulltextResultCollector extends Lucene9ScoredEntityResultCollector {
        private FulltextResultCollector(IndexQueryConstraints constraints, LongPredicate exclusionFilter) {
            super(constraints, exclusionFilter);
        }

        @Override
        protected String entityIdFieldKey() {
            return LuceneFulltextDocumentStructure.FIELD_ENTITY_ID;
        }
    }
}
