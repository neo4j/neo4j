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
package org.neo4j.kernel.api.impl.fulltext;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermStatistics;

import java.io.IOException;

/**
 * An index searcher implementation delegates to the given {@link StatsCollector} for computing its term and collection statistics.
 * This makes it possible for this index searcher to create weights and scorers that are calibrated to the aggregate statistics of multiple indexes.
 * Aggregating the statistics is useful when a full-text index spans multiple partitions, or when transaction state needs to be taken into account as well.
 * Without the aggregate statistics, the scores computed from each search in the individual partitions, will not be comparable.
 */
class StatsCachingIndexSearcher extends IndexSearcher
{
    private final StatsCollector collector;

    StatsCachingIndexSearcher( PreparedSearch search, StatsCollector collector )
    {
        super( search.searcher().getTopReaderContext(), search.searcher().getExecutor() );
        this.collector = collector;
    }

    @Override
    public TermStatistics termStatistics( Term term, int docFreq, long totalTermFreq ) throws IOException
    {
        return collector.termStatistics( term );
    }

    @Override
    public CollectionStatistics collectionStatistics( String field )
    {
        return collector.collectionStatistics( field );
    }
}
