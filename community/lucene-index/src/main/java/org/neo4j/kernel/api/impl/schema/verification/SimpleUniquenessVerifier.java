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
package org.neo4j.kernel.api.impl.schema.verification;

import org.apache.lucene.index.Fields;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.List;

import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.LuceneErrorDetails;
import org.neo4j.kernel.api.impl.index.partition.PartitionSearcher;
import org.neo4j.kernel.api.impl.schema.LuceneDocumentStructure;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;

/**
 * A {@link UniquenessVerifier} that is able to verify value uniqueness inside a single index partition using
 * it's {@link PartitionSearcher}.
 * <p>
 * This verifier reads all terms, checks document frequency for each term and verifies uniqueness of values from the
 * property store if document frequency is greater than 1.
 *
 * @see PartitionSearcher
 * @see DuplicateCheckingCollector
 */
public class SimpleUniquenessVerifier implements UniquenessVerifier
{
    private final PartitionSearcher partitionSearcher;
    private final IndexDescriptor descriptor;

    public SimpleUniquenessVerifier( PartitionSearcher partitionSearcher, IndexDescriptor descriptor )
    {
        this.partitionSearcher = partitionSearcher;
        this.descriptor = descriptor;
    }

    @Override
    public void verify( PropertyAccessor accessor, int[] propKeyIds ) throws IndexEntryConflictException, IOException
    {
        TermQuery query = null;
        try
        {
            DuplicateCheckingCollector collector = DuplicateCheckingCollector.forProperties( accessor, propKeyIds );
            IndexSearcher searcher = indexSearcher();
            for ( LeafReaderContext leafReaderContext : searcher.getIndexReader().leaves() )
            {
                Fields fields = leafReaderContext.reader().fields();
                for ( String field : fields )
                {
                    if ( LuceneDocumentStructure.useFieldForUniquenessVerification( field ) )
                    {
                        TermsEnum terms = LuceneDocumentStructure.originalTerms( fields.terms( field ), field );
                        BytesRef termsRef;
                        while ( (termsRef = terms.next()) != null )
                        {
                            if ( terms.docFreq() > 1 )
                            {
                                collector.init( terms.docFreq() );
                                query = new TermQuery( new Term( field, termsRef ) );
                                searcher.search( query, collector );
                            }
                        }
                    }
                }
            }
        }
        catch ( IOException e )
        {
            Throwable cause = e.getCause();
            if ( cause instanceof IndexEntryConflictException )
            {
                throw (IndexEntryConflictException) cause;
            }
            throw e;
        }
        catch ( Throwable t )
        {
            throw new RuntimeException( LuceneErrorDetails.searchError( descriptor.toString(), query ), t );
        }
    }

    @Override
    public void verify( PropertyAccessor accessor, int[] propKeyIds, List<Object> updatedPropertyValues )
            throws IndexEntryConflictException, IOException
    {
        Query query = null;
        try
        {
            DuplicateCheckingCollector collector = DuplicateCheckingCollector.forProperties( accessor, propKeyIds );
            for ( Object propertyValue : updatedPropertyValues )
            {
                collector.init();
                query = LuceneDocumentStructure.newSeekQuery( propertyValue );
                indexSearcher().search( query, collector );
            }
        }
        catch ( IOException e )
        {
            Throwable cause = e.getCause();
            if ( cause instanceof IndexEntryConflictException )
            {
                throw (IndexEntryConflictException) cause;
            }
            throw e;
        }
        catch ( Throwable t )
        {
            throw new RuntimeException( LuceneErrorDetails.searchError( descriptor.toString(), query ), t );
        }
    }

    @Override
    public void close() throws IOException
    {
        partitionSearcher.close();
    }

    private IndexSearcher indexSearcher()
    {
        return partitionSearcher.getIndexSearcher();
    }
}
