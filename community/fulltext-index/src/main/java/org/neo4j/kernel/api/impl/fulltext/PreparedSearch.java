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

import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.function.LongPredicate;

import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.kernel.api.impl.index.collector.ValuesIterator;
import org.neo4j.kernel.api.impl.index.partition.Neo4jIndexSearcher;

class PreparedSearch
{
    private final Neo4jIndexSearcher searcher;
    private final LongPredicate filter;

    PreparedSearch( Neo4jIndexSearcher searcher, LongPredicate filter )
    {
        this.searcher = searcher;
        this.filter = filter;
    }

    Neo4jIndexSearcher searcher()
    {
        return searcher;
    }

    ValuesIterator search( Weight weight, IndexQueryConstraints constraints ) throws IOException
    {
        FulltextResultCollector collector = new FulltextResultCollector( constraints, filter );
        searcher.search( weight, collector );
        return collector.iterator();
    }
}
