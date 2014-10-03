/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import static org.neo4j.kernel.api.impl.index.LuceneDocumentStructure.NODE_ID_KEY;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.index.impl.lucene.Hits;
import org.neo4j.kernel.api.index.IndexReader;

class LuceneIndexAccessorReader implements IndexReader
{

    private final IndexSearcher searcher;
    private final LuceneDocumentStructure documentLogic;
    private final Closeable onClose;

    LuceneIndexAccessorReader( IndexSearcher searcher, LuceneDocumentStructure documentLogic, Closeable onClose )
    {
        this.searcher = searcher;
        this.documentLogic = documentLogic;
        this.onClose = onClose;
    }

    @Override
    public double uniqueValuesFrequencyInSample( final long sampleSize, final int frequency )
    {
        long remainingSamples = sampleSize;
        final SkipOracle oracle = frequencySkipOracle( frequency );
        final Set<Object> values = new HashSet<>();
        try ( TermEnum terms = searcher.getIndexReader().terms() )
        {
            while ( remainingSamples > 0 && terms.next() )
            {
                Term term = terms.term();
                if ( !NODE_ID_KEY.equals( term.field() ))
                {
                    values.add( term.text() );
                    remainingSamples--;
                }

                for ( int toSkip = oracle.skip(); toSkip > 0 && terms.next(); )
                {
                    if ( !NODE_ID_KEY.equals( terms.term().field() ) )
                    {
                        toSkip--;
                    }
                }
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }

        return ((double) values.size()) / ((double) sampleSize);
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
    public int getIndexedCount( long nodeId, Object propertyValue )
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
            return hits.length();
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
            onClose.close();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    static interface SkipOracle
    {
        int skip();
    }

    static SkipOracle frequencySkipOracle( final int frequency )
    {
        return frequency < 2 ? FULL_SCAN_SKIP_ORACLE : randomSkipOracle( 2 * frequency );
    }

    static SkipOracle FULL_SCAN_SKIP_ORACLE = new SkipOracle()
    {
        @Override
        public int skip()
        {
            return 0;
        }
    };

    static SkipOracle randomSkipOracle( final int maxValue )
    {
        return new SkipOracle()
        {
            private final Random random = ThreadLocalRandom.current();

            @Override
            public int skip()
            {
                return random.nextInt( maxValue );
            }
        };
    }
}
