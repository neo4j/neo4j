/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.fulltext;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;

import java.io.IOException;
import java.util.Collection;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.kernel.api.impl.index.collector.DocValuesCollector;
import org.neo4j.kernel.api.impl.index.partition.PartitionSearcher;
import org.neo4j.kernel.api.impl.schema.reader.IndexReaderCloseException;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.joining;
import static org.neo4j.kernel.api.impl.fulltext.FulltextProvider.FIELD_ENTITY_ID;

/**
 * Lucene index reader that is able to read/sample a single partition of a partitioned Lucene index.
 *
 * @see PartitionedFulltextReader
 */
class SimpleFulltextReader implements ReadOnlyFulltext
{
    private final PartitionSearcher partitionSearcher;
    private final Analyzer analyzer;
    private final String[] properties;

    SimpleFulltextReader( PartitionSearcher partitionSearcher, String[] properties, Analyzer analyzer )
    {
        this.partitionSearcher = partitionSearcher;
        this.properties = properties;
        this.analyzer = analyzer;
    }

    @Override
    public PrimitiveLongIterator query( Collection<String> terms, boolean matchAll )
    {
        String query = terms.stream().map( QueryParser::escape ).collect( joining( " " ) );
        return innerQuery( query, matchAll );
    }

    @Override
    public PrimitiveLongIterator fuzzyQuery( Collection<String> terms, boolean matchAll )
    {
        String query = terms.stream().map( QueryParser::escape ).collect( joining( "~ ", "", "~" ) );
        return innerQuery( query, matchAll );
    }

    @Override
    public void close()
    {
        try
        {
            partitionSearcher.close();
        }
        catch ( IOException e )
        {
            throw new IndexReaderCloseException( e );
        }
    }

    @Override
    public FulltextIndexConfiguration getConfigurationDocument() throws IOException
    {
        IndexSearcher indexSearcher = getIndexSearcher();
        TopDocs docs = indexSearcher.search( new TermQuery( FulltextIndexConfiguration.TERM ), 1 );
        if ( docs.scoreDocs.length < 1 )
        {
            return null;
        }
        return new FulltextIndexConfiguration( indexSearcher.doc( docs.scoreDocs[0].doc ) );
    }

    private PrimitiveLongIterator innerQuery( String queryString, boolean matchAll )
    {
        MultiFieldQueryParser multiFieldQueryParser = new MultiFieldQueryParser( properties, analyzer );
        if ( matchAll )
        {
            multiFieldQueryParser.setDefaultOperator( QueryParser.Operator.AND );
        }
        else
        {
            multiFieldQueryParser.setDefaultOperator( QueryParser.Operator.OR );
        }
        Query query;
        try
        {
            query = multiFieldQueryParser.parse( queryString );
        }
        catch ( ParseException e )
        {
            assert false;
            return PrimitiveLongCollections.emptyIterator();
        }
        return indexQuery( query );
    }

    private PrimitiveLongIterator indexQuery( Query query )
    {
        try
        {
            DocValuesCollector docValuesCollector = new DocValuesCollector( true );
            getIndexSearcher().search( query, docValuesCollector );
            return docValuesCollector.getSortedValuesIterator( FIELD_ENTITY_ID, Sort.RELEVANCE );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private IndexSearcher getIndexSearcher()
    {
        return partitionSearcher.getIndexSearcher();
    }
}
