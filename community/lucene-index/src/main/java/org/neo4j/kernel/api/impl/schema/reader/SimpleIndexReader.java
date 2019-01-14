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
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.function.Function;

import org.neo4j.collection.primitive.PrimitiveLongResourceIterator;
import org.neo4j.helpers.TaskControl;
import org.neo4j.helpers.TaskCoordinator;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexQuery.IndexQueryType;
import org.neo4j.kernel.api.exceptions.index.IndexNotApplicableKernelException;
import org.neo4j.kernel.api.impl.index.collector.DocValuesCollector;
import org.neo4j.kernel.api.impl.index.partition.PartitionSearcher;
import org.neo4j.kernel.api.impl.schema.LuceneDocumentStructure;
import org.neo4j.kernel.api.impl.schema.ValueEncoding;
import org.neo4j.kernel.api.impl.schema.sampler.NonUniqueLuceneIndexSampler;
import org.neo4j.kernel.api.impl.schema.sampler.UniqueLuceneIndexSampler;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.index.schema.fusion.BridgingIndexProgressor;
import org.neo4j.storageengine.api.schema.AbstractIndexReader;
import org.neo4j.storageengine.api.schema.IndexProgressor;
import org.neo4j.storageengine.api.schema.IndexSampler;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.lang.String.format;
import static org.neo4j.internal.kernel.api.IndexQuery.IndexQueryType.exact;
import static org.neo4j.kernel.api.impl.schema.LuceneDocumentStructure.NODE_ID_KEY;
import static org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor.Type.UNIQUE;

/**
 * Schema index reader that is able to read/sample a single partition of a partitioned Lucene index.
 *
 * @see PartitionedIndexReader
 */
public class SimpleIndexReader extends AbstractIndexReader
{
    private final PartitionSearcher partitionSearcher;
    private final SchemaIndexDescriptor descriptor;
    private final IndexSamplingConfig samplingConfig;
    private final TaskCoordinator taskCoordinator;

    public SimpleIndexReader( PartitionSearcher partitionSearcher,
            SchemaIndexDescriptor descriptor,
            IndexSamplingConfig samplingConfig,
            TaskCoordinator taskCoordinator )
    {
        super( descriptor );
        this.partitionSearcher = partitionSearcher;
        this.descriptor = descriptor;
        this.samplingConfig = samplingConfig;
        this.taskCoordinator = taskCoordinator;
    }

    @Override
    public IndexSampler createSampler()
    {
        TaskControl taskControl = taskCoordinator.newInstance();
        if ( descriptor.type() == UNIQUE )
        {
            return new UniqueLuceneIndexSampler( getIndexSearcher(), taskControl );
        }
        else
        {
            return new NonUniqueLuceneIndexSampler( getIndexSearcher(), taskControl, samplingConfig );
        }
    }

    @Override
    public void query( IndexProgressor.NodeValueClient client, IndexOrder indexOrder, IndexQuery... predicates ) throws IndexNotApplicableKernelException
    {
        Query query = toLuceneQuery( predicates );
        client.initialize( descriptor, search( query ).getIndexProgressor( NODE_ID_KEY, client ), predicates );
    }

    @Override
    public PrimitiveLongResourceIterator query( IndexQuery... predicates ) throws IndexNotApplicableKernelException
    {
        Query query = toLuceneQuery( predicates );
        return search( query ).getValuesIterator( NODE_ID_KEY );
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
            switch ( predicate.valueGroup() )
            {
            case NUMBER:
                IndexQuery.NumberRangePredicate np = (IndexQuery.NumberRangePredicate) predicate;
                return LuceneDocumentStructure.newInclusiveNumericRangeSeekQuery( np.from(),
                                                                                  np.to() );

            case TEXT:
                IndexQuery.TextRangePredicate sp = (IndexQuery.TextRangePredicate) predicate;
                return LuceneDocumentStructure.newRangeSeekByStringQuery( sp.from(), sp.fromInclusive(),
                                                                          sp.to(), sp.toInclusive() );

            default:
                throw new UnsupportedOperationException(
                        format( "Range scans of value group %s are not supported", predicate.valueGroup() ) );
            }

        case stringPrefix:
            assertNotComposite( predicates );
            IndexQuery.StringPrefixPredicate spp = (IndexQuery.StringPrefixPredicate) predicate;
            return LuceneDocumentStructure.newRangeSeekByPrefixQuery( spp.prefix() );
        case stringContains:
            assertNotComposite( predicates );
            IndexQuery.StringContainsPredicate scp = (IndexQuery.StringContainsPredicate) predicate;
            return LuceneDocumentStructure.newWildCardStringQuery( scp.contains() );
        case stringSuffix:
            assertNotComposite( predicates );
            IndexQuery.StringSuffixPredicate ssp = (IndexQuery.StringSuffixPredicate) predicate;
            return LuceneDocumentStructure.newSuffixStringQuery( ssp.suffix() );
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
     *
     * @param client {@link IndexProgressor.NodeValueClient} to get initialized with this progression.
     * @param propertyAccessor {@link PropertyAccessor} for reading property values.
     */
    @Override
    public void distinctValues( IndexProgressor.NodeValueClient client, PropertyAccessor propertyAccessor )
    {
        try
        {
            IndexQuery[] noQueries = new IndexQuery[0];
            BridgingIndexProgressor multiProgressor = new BridgingIndexProgressor( client, descriptor.schema().getPropertyIds() );
            Fields fields = MultiFields.getFields( getIndexSearcher().getIndexReader() );
            for ( ValueEncoding valueEncoding : ValueEncoding.values() )
            {
                Terms terms = fields.terms( valueEncoding.key() );
                if ( terms != null )
                {
                    Function<BytesRef,Value> valueMaterializer = valueEncoding == ValueEncoding.String && client.needsValues()
                                                                 ? term -> Values.stringValue( term.utf8ToString() )
                                                                 : term -> null;
                    TermsEnum termsIterator = terms.iterator();
                    if ( valueEncoding == ValueEncoding.Number )
                    {
                        termsIterator = NumericUtils.filterPrefixCodedLongs( termsIterator );
                    }
                    multiProgressor.initialize( descriptor, new LuceneDistinctValuesProgressor( termsIterator, client, valueMaterializer ), noQueries );
                }
            }
            client.initialize( descriptor, multiProgressor, noQueries );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private void assertNotComposite( IndexQuery[] predicates )
    {
        assert predicates.length == 1 : "composite indexes not yet supported for this operation";
    }

    @Override
    public long countIndexedNodes( long nodeId, Value... propertyValues )
    {
        Query nodeIdQuery = new TermQuery( LuceneDocumentStructure.newTermForChangeOrRemove( nodeId ) );
        Query valueQuery = LuceneDocumentStructure.newSeekQuery( propertyValues );
        BooleanQuery.Builder nodeIdAndValueQuery = new BooleanQuery.Builder().setDisableCoord( true );
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
            partitionSearcher.close();
        }
        catch ( IOException e )
        {
            throw new IndexReaderCloseException( e );
        }
    }

    private IndexSearcher getIndexSearcher()
    {
        return partitionSearcher.getIndexSearcher();
    }
}
