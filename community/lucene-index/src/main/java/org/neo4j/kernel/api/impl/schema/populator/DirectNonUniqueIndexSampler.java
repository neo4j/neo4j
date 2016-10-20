/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.schema.populator;

import java.io.IOException;

import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.impl.schema.SchemaIndex;
import org.neo4j.kernel.impl.api.index.sampling.NonUniqueIndexSampler;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.IndexSample;
import org.neo4j.storageengine.api.schema.IndexSampler;

/**
 * Non unique index sampler that ignores all include/exclude calls and builds
 * sample based on values obtained directly from targeted index.
 */
public class DirectNonUniqueIndexSampler implements NonUniqueIndexSampler
{

    private SchemaIndex luceneIndex;

    public DirectNonUniqueIndexSampler( SchemaIndex luceneIndex )
    {
        this.luceneIndex = luceneIndex;
    }

    @Override
    public void include( String value )
    {
        // no-op
    }

    @Override
    public void include( String value, long increment )
    {
        // no-op
    }

    @Override
    public void exclude( String value )
    {
        // no-op
    }

    @Override
    public void exclude( String value, long decrement )
    {
        // no-op
    }

    @Override
    public IndexSample result()
    {
        try
        {
            // lucene index needs to be flushed to be sure that reader will see all the data :(
            luceneIndex.flush();
            luceneIndex.maybeRefreshBlocking();

            try (IndexReader indexReader = luceneIndex.getIndexReader())
            {
                IndexSampler sampler = indexReader.createSampler();
                return sampler.sampleIndex();
            }
            catch ( IOException | IndexNotFoundKernelException e )
            {
                throw new RuntimeException( e );
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public IndexSample result( int numDocs )
    {
        throw new UnsupportedOperationException( "Not implemented." );
    }
}
