/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.api.impl.schema.reader;

import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.ReaderSlice;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.StreamSupport;

import org.neo4j.internal.helpers.TaskControl;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexQuery.IndexQueryType;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.kernel.api.impl.index.SearcherReference;
import org.neo4j.kernel.api.impl.index.collector.DocValuesCollector;
import org.neo4j.kernel.api.impl.schema.LuceneDocumentStructure;
import org.neo4j.kernel.api.impl.schema.TaskCoordinator;
import org.neo4j.kernel.api.impl.schema.ValueEncoding;
import org.neo4j.kernel.api.impl.schema.sampler.NonUniqueLuceneIndexSampler;
import org.neo4j.kernel.api.impl.schema.sampler.UniqueLuceneIndexSampler;
import org.neo4j.kernel.api.index.AbstractIndexReader;
import org.neo4j.kernel.api.index.BridgingIndexProgressor;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.api.index.IndexSampler;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.Values;

import static java.lang.String.format;
import static org.neo4j.internal.kernel.api.IndexQuery.IndexQueryType.exact;
import static org.neo4j.kernel.api.impl.schema.LuceneDocumentStructure.NODE_ID_KEY;

/**
 * Schema index reader that is able to read/sample a single partition of a partitioned Lucene index.
 *
 * @see PartitionedIndexReader
 */
public class SimpleIndexReader extends AbstractIndexReader
{
    private final SearcherReference searcherReference;
    private final IndexDescriptor descriptor;
    private final IndexSamplingConfig samplingConfig;
    private final TaskCoordinator taskCoordinator;

    public SimpleIndexReader( SearcherReference searcherReference,
            IndexDescriptor descriptor,
            IndexSamplingConfig samplingConfig,
            TaskCoordinator taskCoordinator )
    {
        super( descriptor );
        this.searcherReference = searcherReference;
        this.descriptor = descriptor;
        this.samplingConfig = samplingConfig;
        this.taskCoordinator = taskCoordinator;
    }

    @Override
    public IndexSampler createSampler()
    {
        TaskControl taskControl = taskCoordinator.newInstance();
        if ( descriptor.isUnique() )
        {
            return new UniqueLuceneIndexSampler( getIndexSearcher(), taskControl );
        }
        else
        {
            return new NonUniqueLuceneIndexSampler( getIndexSearcher(), taskControl, samplingConfig );
        }
    }

    @Override
    public void query( QueryContext context, IndexProgressor.EntityValueClient client, IndexOrder indexOrder, boolean needsValues,
            IndexQuery... predicates ) throws IndexNotApplicableKernelException
    {
        Query query = toLuceneQuery( predicates );
        client.initialize( descriptor, search( query ).getIndexProgressor( NODE_ID_KEY, client ), predicates, indexOrder, needsValues, false );
    }

    private DocValuesCollector search( Query query )
    {
        try
        {
            DocValuesCollector docValuesCollector = new DocValuesCollector();
            getIndexSearcher().search( query, docValuesCollector );
            return docValuesCollector;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private Query toLuceneQuery( IndexQuery... predicates ) throws IndexNotApplicableKernelException
    {
        IndexQuery predicate = predicates[0];
        switch ( predicate.type() )
        {
        case exact:
            Value[] values = new Value[predicates.length];
            for ( int i = 0; i < predicates.length; i++ )
            {
                assert predicates[i].type() == exact :
                        "Exact followed by another query predicate type is not supported at this moment.";
                values[i] = ((IndexQuery.ExactPredicate) predicates[i]).value();
            }
            return LuceneDocumentStructure.newSeekQuery( values );
        case exists:
            for ( IndexQuery p : predicates )
            {
                if ( p.type() != IndexQueryType.exists )
                {
                    throw new IndexNotApplicableKernelException(
                            "Exists followed by another query predicate type is not supported." );
                }
            }
            return LuceneDocumentStructure.newScanQuery();
        case range:
            assertNotComposite( predicates );
            if ( predicate.valueGroup() == ValueGroup.TEXT )
            {
                IndexQuery.TextRangePredicate sp = (IndexQuery.TextRangePredicate) predicate;
                return LuceneDocumentStructure.newRangeSeekByStringQuery( sp.from(), sp.fromInclusive(), sp.to(), sp.toInclusive() );
            }
            throw new UnsupportedOperationException( format( "Range scans of value group %s are not supported", predicate.valueGroup() ) );

        case stringPrefix:
            assertNotComposite( predicates );
            IndexQuery.StringPrefixPredicate spp = (IndexQuery.StringPrefixPredicate) predicate;
            return LuceneDocumentStructure.newRangeSeekByPrefixQuery( spp.prefix().stringValue() );
        case stringContains:
            assertNotComposite( predicates );
            IndexQuery.StringContainsPredicate scp = (IndexQuery.StringContainsPredicate) predicate;
            return LuceneDocumentStructure.newWildCardStringQuery( scp.contains().stringValue() );
        case stringSuffix:
            assertNotComposite( predicates );
            IndexQuery.StringSuffixPredicate ssp = (IndexQuery.StringSuffixPredicate) predicate;
            return LuceneDocumentStructure.newSuffixStringQuery( ssp.suffix().stringValue() );
        default:
            // todo figure out a more specific exception
            throw new RuntimeException( "Index query not supported: " + Arrays.toString( predicates ) );
        }
    }

    @Override
    public boolean hasFullValuePrecision( IndexQuery... predicates )
    {
        return false;
    }

    /**
     * OBS this implementation can only provide values for properties of type {@link String}.
     * Other property types will still be counted as distinct, but {@code client} won't receive {@link Value}
     * instances for those.
     * @param client {@link IndexProgressor.EntityValueClient} to get initialized with this progression.
     * @param propertyAccessor {@link NodePropertyAccessor} for reading property values.
     * @param needsValues whether or not to load string values.
     */
    @Override
    public void distinctValues( IndexProgressor.EntityValueClient client, NodePropertyAccessor propertyAccessor, boolean needsValues )
    {
        try
        {
            IndexQuery[] noQueries = new IndexQuery[0];
            BridgingIndexProgressor multiProgressor = new BridgingIndexProgressor( client, descriptor.schema().getPropertyIds() );
            IndexReader reader = getIndexSearcher().getIndexReader();
            List<LeafReaderContext> leaves = reader.leaves();
            Fields[] subFields = new Fields[leaves.size()];
            ReaderSlice[] readerSlices = new ReaderSlice[leaves.size()];
            for ( int i = 0; i < leaves.size(); i++ )
            {
                LeafReaderContext context = leaves.get( i );
                LeafReader leafReader = context.reader();
                subFields[i] = fieldsOf( leafReader );
                readerSlices[i] = new ReaderSlice( context.docBase, reader.maxDoc(), i );
            }
            Fields fields = new MultiFields( subFields, readerSlices );
            for ( ValueEncoding valueEncoding : ValueEncoding.values() )
            {

                Terms terms = fields.terms( valueEncoding.key() );
                if ( terms != null )
                {
                    Function<BytesRef,Value> valueMaterializer = valueEncoding == ValueEncoding.String && needsValues
                                                                 ? term -> Values.stringValue( term.utf8ToString() )
                                                                 : term -> null;
                    TermsEnum termsIterator = terms.iterator();
                    multiProgressor.initialize( descriptor, new LuceneDistinctValuesProgressor( termsIterator, client, valueMaterializer ), noQueries,
                            IndexOrder.NONE, needsValues, false );
                }
            }
            client.initialize( descriptor, multiProgressor, noQueries, IndexOrder.NONE, needsValues, false );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private Fields fieldsOf( LeafReader leafReader )
    {
        return new Fields()
        {
            @Override
            public Iterator<String> iterator()
            {
                return StreamSupport.stream( leafReader.getFieldInfos().spliterator(), false ).map( info -> info.name ).iterator();
            }

            @Override
            public Terms terms( String field ) throws IOException
            {
                return leafReader.terms( field );
            }

            @Override
            public int size()
            {
                return leafReader.getFieldInfos().size();
            }
        };
    }

    private void assertNotComposite( IndexQuery[] predicates )
    {
        assert predicates.length == 1 : "composite indexes not yet supported for this operation";
    }

    @Override
    public long countIndexedNodes( long nodeId, int[] propertyKeyIds, Value... propertyValues )
    {
        Query nodeIdQuery = new TermQuery( LuceneDocumentStructure.newTermForChangeOrRemove( nodeId ) );
        Query valueQuery = LuceneDocumentStructure.newSeekQuery( propertyValues );
        BooleanQuery.Builder nodeIdAndValueQuery = new BooleanQuery.Builder();
        nodeIdAndValueQuery.add( nodeIdQuery, BooleanClause.Occur.MUST );
        nodeIdAndValueQuery.add( valueQuery, BooleanClause.Occur.MUST );
        try
        {
            TotalHitCountCollector collector = new TotalHitCountCollector();
            getIndexSearcher().search( nodeIdAndValueQuery.build(), collector );
            // A <label,propertyKeyId,nodeId> tuple should only match at most a single propertyValue
            return collector.getTotalHits();
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
            searcherReference.close();
        }
        catch ( IOException e )
        {
            throw new IndexReaderCloseException( e );
        }
    }

    private IndexSearcher getIndexSearcher()
    {
        return searcherReference.getIndexSearcher();
    }
}
