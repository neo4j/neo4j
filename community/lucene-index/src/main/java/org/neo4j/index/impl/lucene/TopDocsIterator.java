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
package org.neo4j.index.impl.lucene;

import java.io.IOException;
import java.util.Iterator;

import org.apache.lucene.document.Document;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldCollector;

import org.neo4j.helpers.collection.ArrayIterator;
import org.neo4j.index.lucene.QueryContext;

class TopDocsIterator extends AbstractIndexHits<Document>
{
    private final Iterator<ScoreDoc> iterator;
    private ScoreDoc currentDoc;
    private final int size;
    private final IndexSearcher searcher;

    TopDocsIterator( Query query, QueryContext context, IndexSearcher searcher ) throws IOException
    {
        TopDocs docs = toTopDocs( query, context, searcher );
        this.size = docs.scoreDocs.length;
        this.iterator = new ArrayIterator<ScoreDoc>( docs.scoreDocs );
        this.searcher = searcher;
    }

    private TopDocs toTopDocs( Query query, QueryContext context, IndexSearcher searcher ) throws IOException
    {
        Sort sorting = context != null ? context.getSorting() : null;
        TopDocs topDocs;
        if ( sorting == null && context != null )
        {
            topDocs = searcher.search( query, context.getTop() );
        }
        else
        {
            if ( context == null || !context.getTradeCorrectnessForSpeed() )
            {
                TopFieldCollector collector = LuceneDataSource.scoringCollector( sorting, context.getTop() );
                searcher.search( query, collector );
                topDocs = collector.topDocs();
            }
            else
            {
                topDocs = searcher.search( query, null, context.getTop(), sorting );
            }
        }
        return topDocs;
    }

    @Override
    protected Document fetchNextOrNull()
    {
        if ( !iterator.hasNext() )
        {
            return null;
        }
        currentDoc = iterator.next();
        try
        {
            return searcher.doc( currentDoc.doc );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public float currentScore()
    {
        return currentDoc.score;
    }

    @Override
    public int size()
    {
        return this.size;
    }
}
