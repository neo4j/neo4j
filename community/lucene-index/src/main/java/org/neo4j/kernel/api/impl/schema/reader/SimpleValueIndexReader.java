/*
 * Copyright (c) "Neo4j"
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

import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexQuery;
import org.neo4j.internal.schema.IndexQuery.IndexQueryType;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.impl.index.SearcherReference;
import org.neo4j.kernel.api.impl.index.collector.DocValuesCollector;
import org.neo4j.kernel.api.impl.schema.LuceneDocumentStructure;
import org.neo4j.kernel.api.impl.schema.TaskCoordinator;
import org.neo4j.kernel.api.impl.schema.sampler.NonUniqueLuceneIndexSampler;
import org.neo4j.kernel.api.impl.schema.sampler.UniqueLuceneIndexSampler;
import org.neo4j.kernel.api.index.AbstractValueIndexReader;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.api.index.IndexSampler;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.kernel.impl.index.schema.PartitionedValueSeek;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

import static java.lang.String.format;
import static org.neo4j.kernel.api.impl.schema.LuceneDocumentStructure.NODE_ID_KEY;

/**
 * Schema index reader that is able to read/sample a single partition of a partitioned Lucene index.
 *
 * @see PartitionedValueIndexReader
 */
public class SimpleValueIndexReader extends AbstractValueIndexReader
{
    private final SearcherReference searcherReference;
    private final IndexDescriptor descriptor;
    private final IndexSamplingConfig samplingConfig;
    private final TaskCoordinator taskCoordinator;

    public SimpleValueIndexReader( SearcherReference searcherReference,
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
        if ( descriptor.isUnique() )
        {
            return new UniqueLuceneIndexSampler( getIndexSearcher(), taskCoordinator );
        }
        else
        {
            return new NonUniqueLuceneIndexSampler( getIndexSearcher(), taskCoordinator, samplingConfig );
        }
    }

    @Override
    public void query( IndexProgressor.EntityValueClient client, QueryContext context, AccessMode accessMode,
                       IndexQueryConstraints constraints, PropertyIndexQuery... predicates ) throws IndexNotApplicableKernelException
    {
        context.monitor().queried( descriptor );
        Query query = toLuceneQuery( predicates );
        client.initialize( descriptor, search( query ).getIndexProgressor( NODE_ID_KEY, client ), accessMode, false, constraints, predicates );
    }

    @Override
    public PartitionedValueSeek valueSeek( int desiredNumberOfPartitions, QueryContext context, PropertyIndexQuery... query )
    {
        throw new UnsupportedOperationException();
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

    private static Query toLuceneQuery( PropertyIndexQuery... predicates ) throws IndexNotApplicableKernelException
    {
        PropertyIndexQuery predicate = predicates[0];
        switch ( predicate.type() )
        {
        case EXACT:
            Value[] values = new Value[predicates.length];
            for ( int i = 0; i < predicates.length; i++ )
            {
                assert predicates[i].type() == IndexQueryType.EXACT :
                        "Exact followed by another query predicate type is not supported at this moment.";
                values[i] = ((PropertyIndexQuery.ExactPredicate) predicates[i]).value();
            }
            return LuceneDocumentStructure.newSeekQuery( values );
        case EXISTS:
            for ( PropertyIndexQuery p : predicates )
            {
                if ( p.type() != IndexQueryType.EXISTS )
                {
                    throw new IndexNotApplicableKernelException(
                            "Exists followed by another query predicate type is not supported." );
                }
            }
            return LuceneDocumentStructure.newScanQuery();
        case RANGE:
            assertNotComposite( predicates );
            if ( predicate.valueGroup() == ValueGroup.TEXT )
            {
                PropertyIndexQuery.TextRangePredicate sp = (PropertyIndexQuery.TextRangePredicate) predicate;
                return LuceneDocumentStructure.newRangeSeekByStringQuery( sp.from(), sp.fromInclusive(), sp.to(), sp.toInclusive() );
            }
            throw new UnsupportedOperationException( format( "Range scans of value group %s are not supported", predicate.valueGroup() ) );

        case STRING_PREFIX:
            assertNotComposite( predicates );
            PropertyIndexQuery.StringPrefixPredicate spp = (PropertyIndexQuery.StringPrefixPredicate) predicate;
            return LuceneDocumentStructure.newRangeSeekByPrefixQuery( spp.prefix().stringValue() );
        case STRING_CONTAINS:
            assertNotComposite( predicates );
            PropertyIndexQuery.StringContainsPredicate scp = (PropertyIndexQuery.StringContainsPredicate) predicate;
            return LuceneDocumentStructure.newWildCardStringQuery( scp.contains().stringValue() );
        case STRING_SUFFIX:
            assertNotComposite( predicates );
            PropertyIndexQuery.StringSuffixPredicate ssp = (PropertyIndexQuery.StringSuffixPredicate) predicate;
            return LuceneDocumentStructure.newSuffixStringQuery( ssp.suffix().stringValue() );
        default:
            // todo figure out a more specific exception
            throw new RuntimeException( "Index query not supported: " + Arrays.toString( predicates ) );
        }
    }

    private static void assertNotComposite( PropertyIndexQuery[] predicates )
    {
        assert predicates.length == 1 : "composite indexes not yet supported for this operation";
    }

    @Override
    public long countIndexedEntities( long entityId, CursorContext cursorContext, int[] propertyKeyIds, Value... propertyValues )
    {
        Query entityIdQuery = new TermQuery( LuceneDocumentStructure.newTermForChangeOrRemove( entityId ) );
        Query valueQuery = LuceneDocumentStructure.newSeekQuery( propertyValues );
        BooleanQuery.Builder entityIdAndValueQuery = new BooleanQuery.Builder();
        entityIdAndValueQuery.add( entityIdQuery, BooleanClause.Occur.MUST );
        entityIdAndValueQuery.add( valueQuery, BooleanClause.Occur.MUST );
        try
        {
            TotalHitCountCollector collector = new TotalHitCountCollector();
            getIndexSearcher().search( entityIdAndValueQuery.build(), collector );
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
