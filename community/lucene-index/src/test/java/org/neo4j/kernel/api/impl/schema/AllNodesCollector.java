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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.store.Directory;
import org.neo4j.kernel.api.impl.index.partition.Neo4jSearcherFactory;
import org.neo4j.values.storable.Value;

public class AllNodesCollector extends SimpleCollector {
    public static List<Long> getAllNodes(Directory directory, Value propertyValue) throws IOException {
        try (SearcherManager manager = new SearcherManager(directory, new Neo4jSearcherFactory())) {
            IndexSearcher searcher = manager.acquire();
            Query query = TextDocumentStructure.newSeekQuery(propertyValue);
            AllNodesCollector collector = new AllNodesCollector();
            searcher.search(query, collector);
            return collector.nodeIds;
        }
    }

    private final List<Long> nodeIds = new ArrayList<>();
    private LeafReader reader;

    @Override
    public void collect(int doc) throws IOException {
        nodeIds.add(TextDocumentStructure.getNodeId(reader.document(doc)));
    }

    @Override
    public ScoreMode scoreMode() {
        return ScoreMode.COMPLETE_NO_SCORES;
    }

    @Override
    protected void doSetNextReader(LeafReaderContext context) {
        this.reader = context.reader();
    }
}
