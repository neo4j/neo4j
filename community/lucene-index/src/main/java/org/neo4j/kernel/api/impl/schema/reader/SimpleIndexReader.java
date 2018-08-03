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
package org.neo4j.kernel.api.impl.schema.reader;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TotalHitCountCollector;

import java.io.IOException;
import java.util.Arrays;

import org.neo4j.collection.PrimitiveLongResourceIterator;
import org.neo4j.helpers.TaskControl;
import org.neo4j.helpers.TaskCoordinator;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexQuery.IndexQueryType;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.kernel.api.impl.index.collector.DocValuesCollector;
import org.neo4j.kernel.api.impl.index.partition.PartitionSearcher;
import org.neo4j.kernel.api.impl.schema.LuceneDocumentStructure;
import org.neo4j.kernel.api.impl.schema.sampler.NonUniqueLuceneIndexSampler;
import org.neo4j.kernel.api.impl.schema.sampler.UniqueLuceneIndexSampler;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.storageengine.api.schema.AbstractIndexReader;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.storageengine.api.schema.IndexProgressor;
import org.neo4j.storageengine.api.schema.IndexSampler;
import org.neo4j.values.storable.Value;

import static java.lang.String.format;
import static org.neo4j.internal.kernel.api.IndexQuery.IndexQueryType.exact;
import static org.neo4j.kernel.api.impl.schema.LuceneDocumentStructure.NODE_ID_KEY;
import static org.neo4j.storageengine.api.schema.IndexDescriptor.Type.UNIQUE;

/**
 * Schema index reader that is able to read/sample a single partition of a partitioned Lucene index.
 *
 * @see PartitionedIndexReader
 */
public class SimpleIndexReader extends AbstractIndexReader
{
    private final PartitionSearcher partitionSearcher;
    private final IndexDescriptor descriptor;
    private final IndexSamplingConfig samplingConfig;
    private final TaskCoordinator taskCoordinator;

    public SimpleIndexReader( PartitionSearcher partitionSearcher,
            IndexDescriptor descriptor,
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
    public void query( IndexProgressor.NodeValueClient client, IndexOrder indexOrder, boolean needsValues, IndexQuery... predicates )
            throws IndexNotApplicableKernelException
    {
        Query query = toLuceneQuery( predicates );
        client.initialize( descriptor, search( query ).getIndexProgressor( NODE_ID_KEY, client ), predicates, needsValues );
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
