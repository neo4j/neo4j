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
package org.neo4j.kernel.api.impl.schema.sampler;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.impl.schema.LuceneDocumentStructure;
import org.neo4j.kernel.api.impl.schema.TaskCoordinator;
import org.neo4j.kernel.api.impl.schema.populator.DefaultNonUniqueIndexSampler;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.api.index.NonUniqueIndexSampler;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;

/**
 * Sampler for non-unique Lucene schema index.
 * Internally uses terms and their document frequencies for sampling.
 */
public class NonUniqueLuceneIndexSampler extends LuceneIndexSampler
{
    private final IndexSearcher indexSearcher;
    private final IndexSamplingConfig indexSamplingConfig;

    public NonUniqueLuceneIndexSampler( IndexSearcher indexSearcher, TaskCoordinator taskCoordinator,
            IndexSamplingConfig indexSamplingConfig )
    {
        super( taskCoordinator );
        this.indexSearcher = indexSearcher;
        this.indexSamplingConfig = indexSamplingConfig;
    }

    @Override
    public IndexSample sampleIndex( CursorContext cursorContext, AtomicBoolean stopped ) throws IndexNotFoundKernelException
    {
        try ( TaskCoordinator.Task task = newTask() )
        {
            NonUniqueIndexSampler sampler = new DefaultNonUniqueIndexSampler( indexSamplingConfig.sampleSizeLimit() );
            IndexReader indexReader = indexSearcher.getIndexReader();
            for ( LeafReaderContext readerContext : indexReader.leaves() )
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
                               // Note from Lucene docs:
                               // "Once a document is deleted it will not appear in search results.
                               // The presence of this document may still be reflected in the docFreq statistics, and thus alter search scores,
                               // though this will be corrected eventually as segments containing deletions are merged."
                                sampler.include( termsRef.utf8ToString(), termsEnum.docFreq() );
                                checkCancellation( task );
                                if ( stopped.get() )
                                {
                                    return new IndexSample();
                                }
                            }
                        }
                    }
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }
            }

            return sampler.sample( indexReader.numDocs(), cursorContext );
        }
    }

    private static Set<String> getFieldNamesToSample( LeafReaderContext readerContext )
    {
        Set<String> fieldNames = new HashSet<>();
        LeafReader reader = readerContext.reader();
        reader.getFieldInfos().forEach( info ->
        {
            String name = info.name;
            if ( !LuceneDocumentStructure.NODE_ID_KEY.equals( name ) )
            {
                fieldNames.add( name );
            }
        });
        return fieldNames;
    }
}
