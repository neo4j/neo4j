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
package org.neo4j.kernel.api.impl.fulltext;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermStates;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.util.BytesRef;
import org.neo4j.kernel.api.impl.index.partition.Neo4jIndexSearcher;

/**
 * Collect, aggregate and cache Lucene index statistics that span multiple index searchers.
 */
class StatsCollector {
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
        List<TermStatistics> statistics = new ArrayList<>(searches.size());
        for (PreparedSearch search : searches) {
            Neo4jIndexSearcher searcher = search.searcher();
            try {
                TermStates context = TermStates.build(searcher, term, true);
                if (context.docFreq() > 0) {
                    var statistic = searcher.termStatistics(term, context.docFreq(), context.totalTermFreq());
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
        BytesRef bytesTerm = statistics.get(0).term();
        result = new TermStatistics(bytesTerm, docFreq, totalTermFreq);
        return Optional.of(result);
    }

    CollectionStatistics collectionStatistics(String field) {
        return collStatisticsCache
                .computeIfAbsent(field, this::computeCollStatistics)
                .orElse(null);
    }

    private Optional<CollectionStatistics> computeCollStatistics(String field) {
        List<CollectionStatistics> statistics = new ArrayList<>(searches.size());
        for (PreparedSearch search : searches) {
            try {
                CollectionStatistics statistic = search.searcher().collectionStatistics(field);
                if (statistic != null) {
                    statistics.add(statistic);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        long maxDoc = 0;
        long docCount = 0;
        long sumTotalTermFreq = 0;
        long sumDocFreq = 0;
        for (CollectionStatistics statistic : statistics) {
            maxDoc += statistic.maxDoc();
            docCount += statistic.docCount();
            sumTotalTermFreq += statistic.sumTotalTermFreq();
            sumDocFreq += statistic.sumDocFreq();
        }
        if (docCount == 0) {
            return Optional.empty();
        }
        return Optional.of(new CollectionStatistics(field, maxDoc, docCount, sumTotalTermFreq, sumDocFreq));
    }
}
