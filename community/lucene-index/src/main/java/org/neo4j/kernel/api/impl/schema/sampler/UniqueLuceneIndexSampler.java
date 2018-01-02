/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.schema.sampler;

import org.apache.lucene.search.IndexSearcher;

import org.neo4j.helpers.TaskControl;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.impl.api.index.sampling.UniqueIndexSampler;
import org.neo4j.storageengine.api.schema.IndexSample;

/**
 * Sampler for unique Lucene schema index.
 * Internally uses number of documents in the index for sampling.
 */
public class UniqueLuceneIndexSampler extends LuceneIndexSampler
{
    private final IndexSearcher indexSearcher;

    public UniqueLuceneIndexSampler( IndexSearcher indexSearcher, TaskControl taskControl )
    {
        super( taskControl );
        this.indexSearcher = indexSearcher;
    }

    @Override
    protected IndexSample performSampling() throws IndexNotFoundKernelException
    {
        UniqueIndexSampler sampler = new UniqueIndexSampler();
        sampler.increment( indexSearcher.getIndexReader().numDocs() );
        checkCancellation();
        return sampler.result();
    }
}
