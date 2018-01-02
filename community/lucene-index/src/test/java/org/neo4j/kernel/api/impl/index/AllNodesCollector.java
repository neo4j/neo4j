/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.api.impl.index;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.Directory;

class AllNodesCollector extends Collector
{
    static List<Long> getAllNodes( DirectoryFactory dirFactory, File indexDir, Object propertyValue ) throws IOException
    {
        try ( Directory directory = dirFactory.open( indexDir );
              SearcherManager manager = new SearcherManager( directory, new SearcherFactory() );
              IndexSearcher searcher = manager.acquire() )
        {
            List<Long> nodes = new ArrayList<>();
            LuceneDocumentStructure documentStructure = new LuceneDocumentStructure();
            Query query = documentStructure.newSeekQuery( propertyValue );
            searcher.search( query, new AllNodesCollector( documentStructure, nodes ) );
            return nodes;
        }
    }

    private final List<Long> nodeIds;
    private final LuceneDocumentStructure documentLogic;
    private IndexReader reader;

    AllNodesCollector( LuceneDocumentStructure documentLogic, List<Long> nodeIds )
    {
        this.documentLogic = documentLogic;
        this.nodeIds = nodeIds;
    }

    @Override
    public void setScorer( Scorer scorer ) throws IOException
    {
    }

    @Override
    public void collect( int doc ) throws IOException
    {
        nodeIds.add( documentLogic.getNodeId( reader.document( doc ) ) );
    }

    @Override
    public void setNextReader( IndexReader reader, int docBase ) throws IOException
    {
        this.reader = reader;
    }

    @Override
    public boolean acceptsDocsOutOfOrder()
    {
        return true;
    }
}
