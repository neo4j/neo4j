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
package org.neo4j.kernel.api.impl.schema.populator;

import java.io.IOException;

import java.util.concurrent.atomic.AtomicBoolean;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.impl.schema.SchemaIndex;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.index.schema.IndexUpdateIgnoreStrategy;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.NodePropertyAccessor;

/**
 * A {@link LuceneIndexPopulator} used for non-unique Lucene schema indexes, Performs index sampling.
 */
public class NonUniqueLuceneIndexPopulator extends LuceneIndexPopulator<SchemaIndex>
{

    public NonUniqueLuceneIndexPopulator( SchemaIndex luceneIndex, IndexUpdateIgnoreStrategy ignoreStrategy )
    {
        super( luceneIndex, ignoreStrategy );
    }

    @Override
    public void verifyDeferredConstraints( NodePropertyAccessor accessor )
    {
        // no constraints to verify so do nothing
    }

    @Override
    public IndexUpdater newPopulatingUpdater( NodePropertyAccessor nodePropertyAccessor, CursorContext cursorContext )
    {
        return new NonUniqueLuceneIndexPopulatingUpdater( writer, ignoreStrategy );
    }

    @Override
    public void includeSample( IndexEntryUpdate<?> update )
    {
        // Samples are built by scanning the index
    }

    @Override
    public IndexSample sample( CursorContext cursorContext )
    {
        try
        {
            luceneIndex.maybeRefreshBlocking();
            try ( var reader = luceneIndex.getIndexReader();
                  var sampler = reader.createSampler() )
            {
                return sampler.sampleIndex( cursorContext, new AtomicBoolean() );
            }
        }
        catch ( IOException | IndexNotFoundKernelException e )
        {
            throw new RuntimeException( e );
        }
    }
}
