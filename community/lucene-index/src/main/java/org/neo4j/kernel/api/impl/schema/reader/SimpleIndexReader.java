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
package org.neo4j.kernel.api.impl.schema.reader;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TotalHitCountCollector;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.helpers.TaskControl;
import org.neo4j.helpers.TaskCoordinator;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.api.exceptions.index.IndexNotApplicableKernelException;
import org.neo4j.kernel.api.impl.LuceneErrorDetails;
import org.neo4j.kernel.api.impl.index.collector.DocValuesCollector;
import org.neo4j.kernel.api.impl.index.partition.PartitionSearcher;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.kernel.api.impl.schema.LuceneDocumentStructure;
import org.neo4j.kernel.api.impl.schema.LuceneSchemaIndex;
import org.neo4j.kernel.api.impl.schema.sampler.NonUniqueLuceneIndexSampler;
import org.neo4j.kernel.api.impl.schema.sampler.UniqueLuceneIndexSampler;
import org.neo4j.kernel.api.schema.IndexQuery;
import org.neo4j.kernel.api.schema.IndexQuery.IndexQueryType;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.IndexSampler;

import static org.neo4j.kernel.api.impl.schema.LuceneDocumentStructure.NODE_ID_KEY;
import static org.neo4j.kernel.api.schema.IndexQuery.IndexQueryType.exact;
import static org.neo4j.kernel.api.schema.index.IndexDescriptor.Type.UNIQUE;

/**
 * Schema index reader that is able to read/sample a single partition of a partitioned Lucene index.
 *
 * @see PartitionedIndexReader
 */
public class SimpleIndexReader implements IndexReader
{
    private PartitionSearcher partitionSearcher;
    private IndexDescriptor descriptor;
    private final IndexSamplingConfig samplingConfig;
    private TaskCoordinator taskCoordinator;
    private final LuceneSchemaIndex luceneSchemaIndex;

    public SimpleIndexReader( PartitionSearcher partitionSearcher,
            IndexDescriptor descriptor,
            IndexSamplingConfig samplingConfig,
            TaskCoordinator taskCoordinator,
            LuceneSchemaIndex luceneSchemaIndex )
    {
        this.partitionSearcher = partitionSearcher;
        this.descriptor = descriptor;
        this.samplingConfig = samplingConfig;
        this.taskCoordinator = taskCoordinator;
        this.luceneSchemaIndex = luceneSchemaIndex;
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
    public PrimitiveLongIterator query( IndexQuery... predicates ) throws IndexNotApplicableKernelException
    {
        IndexQuery predicate = predicates[0];
        switch ( predicate.type() )
        {
        case exact:
            Object[] values = new Object[predicates.length];
            for ( int i = 0; i < predicates.length; i++ )
            {
                assert predicates[i].type() == exact :
                        "Exact followed by another query predicate type is not supported at this moment.";
                values[i] = ((IndexQuery.ExactPredicate) predicates[i]).value();
            }
            return seek( values );
        case exists:
            for ( IndexQuery p : predicates )
            {
                if ( p.type() != IndexQueryType.exists )
                {
                    throw new IndexNotApplicableKernelException(
                            "Exists followed by another query predicate type is not supported." );
                }
            }
            return scan();
        case rangeNumeric:
            assertNotComposite( predicates );
            IndexQuery.NumberRangePredicate np = (IndexQuery.NumberRangePredicate) predicate;
            return rangeSeekByNumberInclusive( np.from(), np.to() );
        case rangeString:
            assertNotComposite( predicates );
            IndexQuery.StringRangePredicate sp = (IndexQuery.StringRangePredicate) predicate;
            return rangeSeekByString( sp.from(), sp.fromInclusive(), sp.to(), sp.toInclusive() );
        case stringPrefix:
            assertNotComposite( predicates );
            IndexQuery.StringPrefixPredicate spp = (IndexQuery.StringPrefixPredicate) predicate;
            return rangeSeekByPrefix( spp.prefix() );
        case stringContains:
            assertNotComposite( predicates );
            IndexQuery.StringContainsPredicate scp = (IndexQuery.StringContainsPredicate) predicate;
            return containsString( scp.contains() );
        case stringSuffix:
            assertNotComposite( predicates );
            IndexQuery.StringSuffixPredicate ssp = (IndexQuery.StringSuffixPredicate) predicate;
            return endsWith( ssp.suffix() );
        case fail:
            return query( (Query) null );
        default:
            // todo figure out a more specific exception
            throw new RuntimeException( "Index query not supported: " + Arrays.toString( predicates ) );
        }
    }

    private void assertNotComposite( IndexQuery[] predicates )
    {
        assert predicates.length == 1 : "composite indexes not yet supported for this operation";
    }

    private PrimitiveLongIterator seek( Object... values )
    {
        return query( LuceneDocumentStructure.newSeekQuery( values ) );
    }

    private PrimitiveLongIterator rangeSeekByNumberInclusive( Number lower, Number upper )
    {
        return query( LuceneDocumentStructure.newInclusiveNumericRangeSeekQuery( lower, upper ) );
    }

    private PrimitiveLongIterator rangeSeekByString( String lower, boolean includeLower,
            String upper, boolean includeUpper )
    {
        return query( LuceneDocumentStructure.newRangeSeekByStringQuery( lower, includeLower, upper, includeUpper ) );
    }

    private PrimitiveLongIterator rangeSeekByPrefix( String prefix )
    {
        return query( LuceneDocumentStructure.newRangeSeekByPrefixQuery( prefix ) );
    }

    private PrimitiveLongIterator scan()
    {
        return query( LuceneDocumentStructure.newScanQuery() );
    }

    private PrimitiveLongIterator containsString( String exactTerm )
    {
        return query( LuceneDocumentStructure.newWildCardStringQuery( exactTerm ) );
    }

    private PrimitiveLongIterator endsWith( String suffix )
    {
        return query( LuceneDocumentStructure.newSuffixStringQuery( suffix ) );
    }

    @Override
    public long countIndexedNodes( long nodeId, Object... propertyValues )
    {
        Query nodeIdQuery = new TermQuery( LuceneDocumentStructure.newTermForChangeOrRemove( nodeId ) );
        Query valueQuery = LuceneDocumentStructure.newSeekQuery( propertyValues );
        BooleanQuery.Builder nodeIdAndValueQuery = new BooleanQuery.Builder().setDisableCoord( true );
        nodeIdAndValueQuery.add( nodeIdQuery, BooleanClause.Occur.MUST );
        nodeIdAndValueQuery.add( valueQuery, BooleanClause.Occur.MUST );
        BooleanQuery query = null;
        try
        {
            TotalHitCountCollector collector = new TotalHitCountCollector();
            query = nodeIdAndValueQuery.build();
            getIndexSearcher().search( query, collector );
            // A <label,propertyKeyId,nodeId> tuple should only match at most a single propertyValue
            return collector.getTotalHits();
        }
        catch ( Throwable t )
        {
            throw new RuntimeException( LuceneErrorDetails.searchError( descriptor.toString(), query ), t );
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

    public PrimitiveLongIterator query( Query query )
    {
        try
        {
            DocValuesCollector docValuesCollector = new DocValuesCollector();
            getIndexSearcher().search( query, docValuesCollector );
            return docValuesCollector.getValuesIterator( NODE_ID_KEY );
        }
        catch ( Throwable t )
        {
            // Completely ignore the stack trace. Only keep index and query information.
            RuntimeException exceptionToThrow = new RuntimeException( LuceneErrorDetails.searchError( descriptor.toString(), query ) )
            {
                @Override
                public synchronized Throwable fillInStackTrace()
                {
                    return this;
                }
            };

            // Hack to not pass in real LuceneSchemaIndex to constructor everywhere
            if ( luceneSchemaIndex != null )
            {
                try
                {
                    luceneSchemaIndex.flush( false );

                    PartitionedIndexStorage indexStorage = luceneSchemaIndex.indexStorage();
                    File storeDir = indexStorage.getIndexFolder() /* /db/schema/index/lucene/1 */
                            .getParentFile() /* /db/schema/index/lucene */
                            .getParentFile() /* /db/schema/index */
                            .getParentFile() /* /db/schema */
                            .getParentFile();/* /db */
                    File indexFolder = indexStorage.getIndexFolder();
                    File dumpDir = new File( storeDir, "dump_" + indexFolder.getName() );
                    if ( dumpDir.exists() )
                    {
                        // We have already created a dump
                    }
                    else
                    {
                        dumpDir.mkdir();

                        // dump index snapshot
                        File indexDir = indexStorage.getPartitionFolder( 1 );
                        File indexDumpDir = new File( dumpDir, "index" );
                        FileUtils.copyRecursively( indexDir, indexDumpDir );

                        // snapshot debug.log
                        File logs = new File( storeDir, "logs" );
                        File logsDumpDir = new File( dumpDir, "logs" );
                        FileUtils.copyRecursively( logs, logsDumpDir );
                    }
                }
                catch ( Exception e )
                {
                    RuntimeException snapshotException = new RuntimeException( "Could not create snapshot of lucene files", e );
                    snapshotException.addSuppressed( exceptionToThrow );
                    exceptionToThrow = snapshotException;
                }

                try
                {
                    luceneSchemaIndex.close();
                    luceneSchemaIndex.open();
                }
                catch ( IOException e )
                {
                    RuntimeException closeOpenException = new RuntimeException( "Could not close and open index.", e );
                    closeOpenException.addSuppressed( exceptionToThrow );
                    exceptionToThrow = closeOpenException;
                }
            }
            throw exceptionToThrow;
        }
    }

    private void printExist( int propertyKeyId ) throws IOException, IndexNotApplicableKernelException
    {
        IndexReader indexReader = luceneSchemaIndex.getIndexReader();
        PrimitiveLongIterator result = indexReader.query( IndexQuery.exists( propertyKeyId ) );
        while ( result.hasNext() )
        {
            System.out.println( result.next() );
        }
    }

    private File parent( File storeDir )
    {
        File parent = storeDir.getParentFile();
        System.out.println( parent );
        return parent;
    }

    private void sleep()
    {
        try
        {
            Thread.sleep( 100 );
        }
        catch ( InterruptedException e )
        {
            e.printStackTrace();
        }
    }

    private IndexSearcher getIndexSearcher()
    {
        return partitionSearcher.getIndexSearcher();
    }
}
