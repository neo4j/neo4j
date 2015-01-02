/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.io.IOException;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TermQuery;

import org.neo4j.index.impl.lucene.Hits;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.impl.util.PrimitiveLongIterator;

class LuceneIndexAccessorReader implements IndexReader
{
    private final IndexSearcher searcher;
    private final LuceneDocumentStructure documentLogic;
    private final SearcherManager searcherManager;

    LuceneIndexAccessorReader( SearcherManager searcherManager, LuceneDocumentStructure documentLogic )
    {
        this.searcherManager = searcherManager;
        this.searcher = searcherManager.acquire();
        this.documentLogic = documentLogic;
    }

    @Override
    public PrimitiveLongIterator lookup( Object value )
    {
        try
        {
            Hits hits = new Hits( searcher, documentLogic.newQuery( value ), null );
            return new HitsPrimitiveLongIterator( hits, documentLogic );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public boolean hasIndexed( long nodeId, Object propertyValue )
    {
        Query nodeIdQuery = new TermQuery( documentLogic.newQueryForChangeOrRemove( nodeId ) );
        Query valueQuery = documentLogic.newQuery( propertyValue );
        BooleanQuery nodeIdAndValueQuery = new BooleanQuery( true );
        nodeIdAndValueQuery.add( nodeIdQuery, BooleanClause.Occur.MUST );
        nodeIdAndValueQuery.add( valueQuery, BooleanClause.Occur.MUST );
        try
        {
            Hits hits = new Hits( searcher, nodeIdAndValueQuery, null );
            // A <label,propertyKeyId,nodeId> tuple should only match at most a single propertyValue
            return hits.length() == 1;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public void close()
    {
        try
        {
            searcherManager.release( searcher );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
}
