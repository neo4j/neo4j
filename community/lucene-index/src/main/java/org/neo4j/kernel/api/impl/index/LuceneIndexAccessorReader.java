/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherManager;

import org.neo4j.index.impl.lucene.Hits;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.impl.api.AbstractPrimitiveLongIterator;
import org.neo4j.kernel.impl.api.PrimitiveLongIterator;

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
    public PrimitiveLongIterator lookup( final Object value )
    {
        try
        {
            final Hits hits = new Hits( searcher, documentLogic.newQuery( value ), null );
            return new AbstractPrimitiveLongIterator()
            {
                int size = hits.length(), index;
                
                {
                    computeNext();
                }
                
                @Override
                protected void computeNext()
                {
                    if ( index < size )
                    {
                        try
                        {
                            nextValue = documentLogic.getNodeId( hits.doc( index++ ) );
                            hasNext = true;
                        }
                        catch ( IOException e )
                        {
                            throw new RuntimeException( e );
                        }
                    }
                    else
                    {
                        hasNext = false;
                    }
                }
            };
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
