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
import java.util.function.LongPredicate;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Weight;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.kernel.api.impl.index.collector.ValuesIterator;
import org.neo4j.kernel.api.impl.index.partition.Neo4jIndexSearcher;

class PreparedSearch {
    private final Neo4jIndexSearcher searcher;
    private final LongPredicate filter;

    PreparedSearch(Neo4jIndexSearcher searcher, LongPredicate filter) {
        this.searcher = searcher;
        this.filter = filter;
    }

    Neo4jIndexSearcher searcher() {
        return searcher;
    }

    ValuesIterator search(Query query, IndexQueryConstraints constraints, StatsCollector statsCollector)
            throws IOException {
        FulltextResultCollector collector = new FulltextResultCollector(constraints, filter);

        // Weights are bonded with the top IndexReaderContext of the index searcher that they are created for.
        // That's why we have to create a new StatsCachingIndexSearcher, and a new weight, for every index partition.
        // However, the important thing is that we re-use the statsCollector.
        StatsCachingIndexSearcher statsCachingIndexSearcher = new StatsCachingIndexSearcher(this, statsCollector);
        Weight weight = statsCachingIndexSearcher.createWeight(query, collector.scoreMode(), 1);

        searcher.search(weight, collector);
        return collector.iterator();
    }
}
