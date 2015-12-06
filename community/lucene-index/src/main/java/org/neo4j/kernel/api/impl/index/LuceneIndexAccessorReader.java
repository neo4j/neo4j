/*
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

import org.apache.lucene.index.Fields;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.util.BytesRef;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.helpers.CancellationRequest;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.impl.index.LuceneDocumentStructure.ValueEncoding;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.impl.api.index.sampling.NonUniqueIndexSampler;
import org.neo4j.register.Register.DoubleLong;

import static org.neo4j.kernel.api.impl.index.LuceneDocumentStructure.NODE_ID_KEY;


class LuceneIndexAccessorReader implements IndexReader
{
    private final IndexSearcher searcher;
    private final LuceneDocumentStructure documentLogic;
    private final Closeable onClose;
    private final CancellationRequest cancellation;
    private final int bufferSizeLimit;

    LuceneIndexAccessorReader( IndexSearcher searcher, LuceneDocumentStructure documentLogic, Closeable onClose,
                               CancellationRequest cancellation, int bufferSizeLimit )
    {
        this.searcher = searcher;
        this.documentLogic = documentLogic;
        this.onClose = onClose;
        this.cancellation = cancellation;
        this.bufferSizeLimit = bufferSizeLimit;
    }

    @Override
    public long sampleIndex( DoubleLong.Out result ) throws IndexNotFoundKernelException
    {
        NonUniqueIndexSampler sampler = new NonUniqueIndexSampler( bufferSizeLimit );

        for ( LeafReaderContext readerContext : luceneIndexReader().leaves() )
        {
            try
            {
                Set<String> fieldNames = getFieldNamesToSample( readerContext );
                for ( String fieldName : fieldNames )
                {
                    Terms terms = readerContext.reader().terms( fieldName );
                    if ( terms != null )
                    {
                        TermsEnum termsEnum = terms.iterator();
                        BytesRef termsRef;
                        while ( (termsRef = termsEnum.next()) != null )
                        {
                            sampler.include( termsRef.utf8ToString(), termsEnum.docFreq());
                            checkCancellation();
                        }
                    }
                }
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }

        return sampler.result( result );
    }

    private Set<String> getFieldNamesToSample( LeafReaderContext readerContext ) throws IOException
    {
        Fields fields = readerContext.reader().fields();
        Set<String> fieldNames = Iterables.toSet( fields );
        assert fieldNames.remove( NODE_ID_KEY );
        return fieldNames;
    }

    @Override
    public PrimitiveLongIterator seek( Object value )
    {
        return query( documentLogic.newSeekQuery( value ) );
    }

    @Override
    public PrimitiveLongIterator rangeSeekByNumberInclusive( Number lower, Number upper )
    {
        return query( documentLogic.newInclusiveNumericRangeSeekQuery( lower, upper ) );
    }

    @Override
    public PrimitiveLongIterator rangeSeekByString( String lower, boolean includeLower,
                                                    String upper, boolean includeUpper )
    {
        return query( documentLogic.newRangeSeekByStringQuery( lower, includeLower, upper, includeUpper ) );
    }

    @Override
    public PrimitiveLongIterator rangeSeekByPrefix( String prefix )
    {
        return query( documentLogic.newRangeSeekByPrefixQuery( prefix ) );
    }

    @Override
    public PrimitiveLongIterator scan()
    {
        return query( documentLogic.newScanQuery() );
    }

    @Override
    public int countIndexedNodes( long nodeId, Object propertyValue )
    {
        Query nodeIdQuery = new TermQuery( documentLogic.newTermForChangeOrRemove( nodeId ) );
        Query valueQuery = documentLogic.newSeekQuery( propertyValue );
        BooleanQuery.Builder nodeIdAndValueQuery = new BooleanQuery.Builder().setDisableCoord( true );
        nodeIdAndValueQuery.add( nodeIdQuery, BooleanClause.Occur.MUST );
        nodeIdAndValueQuery.add( valueQuery, BooleanClause.Occur.MUST );
        try
        {
            TotalHitCountCollector collector = new TotalHitCountCollector();
            searcher.search( nodeIdAndValueQuery.build(), collector );
            // A <label,propertyKeyId,nodeId> tuple should only match at most a single propertyValue
            return collector.getTotalHits();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public Set<Class> valueTypesInIndex()
    {
        Set<Class> types = new HashSet<>();
        for ( LeafReaderContext readerContext : luceneIndexReader().leaves() )
        {
            try
            {
                Fields fields = readerContext.reader().fields();
                for ( String field : fields )
                {
                    if ( !NODE_ID_KEY.equals( field ) )
                    {
                        switch ( ValueEncoding.fromKey( field ) )
                        {
                        case Number:
                            types.add( Number.class );
                            break;
                        case String:
                            types.add( String.class );
                            break;
                        case Array:
                            types.add( Array.class );
                            break;
                        case Bool:
                            types.add( Boolean.class );
                            break;
                        }
                    }
                }
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }
        return types;
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

    protected void checkCancellation() throws IndexNotFoundKernelException
    {
        if ( cancellation.cancellationRequested() )
        {
            throw new IndexNotFoundKernelException( "Index dropped while sampling." );
        }
    }

    protected org.apache.lucene.index.IndexReader luceneIndexReader()
    {
        return searcher.getIndexReader();
    }


    protected PrimitiveLongIterator query( Query query )
    {
        try
        {
            DocValuesCollector docValuesCollector = new DocValuesCollector();
            searcher.search( query, docValuesCollector );
            return docValuesCollector.getValuesIterator( NODE_ID_KEY );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
}
