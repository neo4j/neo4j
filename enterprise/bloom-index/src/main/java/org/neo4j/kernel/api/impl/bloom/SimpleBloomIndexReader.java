/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.bloom;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;

import java.io.IOException;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.kernel.api.impl.index.collector.DocValuesCollector;
import org.neo4j.kernel.api.impl.index.partition.PartitionSearcher;
import org.neo4j.kernel.api.impl.schema.reader.IndexReaderCloseException;

import static org.neo4j.kernel.api.impl.schema.LuceneDocumentStructure.NODE_ID_KEY;

/**
 * Schema index reader that is able to read/sample a single partition of a partitioned Lucene index.
 *
 * @see PartitionedBloomIndexReader
 */
class SimpleBloomIndexReader implements BloomIndexReader
{
    private final PartitionSearcher partitionSearcher;
    private final Analyzer analyzer;
    private String[] properties;

    SimpleBloomIndexReader( PartitionSearcher partitionSearcher, String[] properties, Analyzer analyzer )
    {
        this.partitionSearcher = partitionSearcher;
        this.properties = properties;
        this.analyzer = analyzer;
    }

    public PrimitiveLongIterator query( String... query )
    {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        QueryParser multiFieldQueryParser = new MultiFieldQueryParser( properties, analyzer );
        multiFieldQueryParser.setDefaultOperator( QueryParser.Operator.OR );
        for ( String s : query )
        {
            for ( String property : properties )
            {
                builder.add( multiFieldQueryParser.createBooleanQuery( property, s ), BooleanClause.Occur.SHOULD );
            }
        }
        return query( builder.build() );
    }

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

    protected PrimitiveLongIterator query( Query query )
    {
        try
        {
            DocValuesCollector docValuesCollector = new DocValuesCollector( true );
            getIndexSearcher().search( query, docValuesCollector );
            return docValuesCollector.getSortedValuesIterator( NODE_ID_KEY, Sort.RELEVANCE );
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
