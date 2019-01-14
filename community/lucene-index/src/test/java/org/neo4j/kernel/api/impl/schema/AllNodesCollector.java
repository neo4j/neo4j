/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.api.impl.schema;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.store.Directory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.values.storable.Value;

public class AllNodesCollector extends SimpleCollector
{
    public static List<Long> getAllNodes( Directory directory, Value propertyValue ) throws IOException
    {
        try ( SearcherManager manager = new SearcherManager( directory, new SearcherFactory() ) )
        {
            IndexSearcher searcher = manager.acquire();
            Query query = LuceneDocumentStructure.newSeekQuery( propertyValue );
            AllNodesCollector collector = new AllNodesCollector();
            searcher.search( query, collector );
            return collector.nodeIds;
        }
    }

    private final List<Long> nodeIds = new ArrayList<>();
    private LeafReader reader;

    @Override
    public void collect( int doc ) throws IOException
    {
        nodeIds.add( LuceneDocumentStructure.getNodeId( reader.document( doc ) ) );
    }

    @Override
    public boolean needsScores()
    {
        return false;
    }

    @Override
    protected void doSetNextReader( LeafReaderContext context )
    {
        this.reader = context.reader();
    }
}
