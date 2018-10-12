/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TotalHitCountCollector;

import java.io.IOException;

import org.neo4j.kernel.api.impl.index.collector.DocValuesCollector;
import org.neo4j.kernel.api.impl.index.collector.ValuesIterator;
import org.neo4j.kernel.api.impl.schema.reader.IndexReaderCloseException;
import org.neo4j.kernel.impl.core.TokenHolder;
import org.neo4j.kernel.impl.core.TokenNotFoundException;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.values.storable.Value;

/**
 * Lucene index reader that is able to read/sample a single partition of a partitioned Lucene index.
 *
 * @see PartitionedFulltextIndexReader
 */
class SimpleFulltextIndexReader extends FulltextIndexReader
{
    private final SearcherReference searcherRef;
    private final TokenHolder propertyKeyTokenHolder;
    private final FulltextIndexDescriptor descriptor;

    SimpleFulltextIndexReader( SearcherReference searcherRef, TokenHolder propertyKeyTokenHolder, FulltextIndexDescriptor descriptor )
    {
        this.searcherRef = searcherRef;
        this.propertyKeyTokenHolder = propertyKeyTokenHolder;
        this.descriptor = descriptor;
    }

    @Override
    public void close()
    {
        try
        {
            searcherRef.close();
        }
        catch ( IOException e )
        {
            throw new IndexReaderCloseException( e );
        }
    }

    @Override
    protected String getPropertyKeyName( int propertyKey ) throws TokenNotFoundException
    {
        return propertyKeyTokenHolder.getTokenById( propertyKey ).name();
    }

    @Override
    protected FulltextIndexDescriptor getDescriptor()
    {
        return descriptor;
    }

    @Override
    protected ScoreEntityIterator indexQuery( Query query )
    {
        try
        {
            DocValuesCollector docValuesCollector = new DocValuesCollector( true );
            getIndexSearcher().search( query, docValuesCollector );
            ValuesIterator sortedValuesIterator =
                    docValuesCollector.getSortedValuesIterator( LuceneFulltextDocumentStructure.FIELD_ENTITY_ID, Sort.RELEVANCE );
            return new ScoreEntityIterator( sortedValuesIterator );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private IndexSearcher getIndexSearcher()
    {
        return searcherRef.getIndexSearcher();
    }

    @Override
    public long countIndexedNodes( long nodeId, int[] propertyKeyIds, Value... propertyValues )
    {
        try
        {
            String[] propertyKeys = new String[propertyKeyIds.length];
            for ( int i = 0; i < propertyKeyIds.length; i++ )
            {
                propertyKeys[i] = getPropertyKeyName( propertyKeyIds[i] );
            }
            Query query = LuceneFulltextDocumentStructure.newCountNodeEntriesQuery( nodeId, propertyKeys, propertyValues );
            TotalHitCountCollector collector = new TotalHitCountCollector();
            getIndexSearcher().search( query, collector );
            return collector.getTotalHits();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }
}
