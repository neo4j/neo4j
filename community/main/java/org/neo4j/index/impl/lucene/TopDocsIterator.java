/**
 * Copyright (c) 2002-2010 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package org.neo4j.index.impl.lucene;

import java.io.IOException;
import java.util.Iterator;

import org.apache.lucene.document.Document;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.neo4j.helpers.collection.ArrayIterator;
import org.neo4j.helpers.collection.PrefetchingIterator;

class TopDocsIterator extends PrefetchingIterator<Document>
{
    private final Iterator<ScoreDoc> iterator;
    private final IndexSearcherRef searcher;
    
    TopDocsIterator( TopDocs docs, IndexSearcherRef searcher )
    {
        this.iterator = new ArrayIterator<ScoreDoc>( docs.scoreDocs );
        this.searcher = searcher;
    }

    @Override
    protected Document fetchNextOrNull()
    {
        if ( !iterator.hasNext() )
        {
            return null;
        }
        ScoreDoc doc = iterator.next();
        try
        {
            return searcher.getSearcher().doc( doc.doc );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
}
