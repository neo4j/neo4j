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
package org.neo4j.kernel.impl.index.schema.fusion;

import java.util.ArrayList;
import java.util.List;

import java.util.concurrent.atomic.AtomicBoolean;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.api.index.IndexSampler;

import static org.neo4j.internal.helpers.Exceptions.throwIfInstanceOf;
import static org.neo4j.internal.helpers.Exceptions.throwIfUnchecked;
import static org.neo4j.internal.helpers.collection.Iterables.asCollection;
import static org.neo4j.io.IOUtils.closeAllSilently;

public class FusionIndexSampler implements IndexSampler
{
    private final Iterable<IndexSampler> samplers;

    public FusionIndexSampler( Iterable<IndexSampler> samplers )
    {
        this.samplers = samplers;
    }

    @Override
    public IndexSample sampleIndex( CursorContext cursorContext, AtomicBoolean stopped ) throws IndexNotFoundKernelException
    {
        List<IndexSample> samples = new ArrayList<>();
        Exception exception = null;
        for ( IndexSampler sampler : samplers )
        {
            try
            {
                samples.add( sampler.sampleIndex( cursorContext, stopped ) );
            }
            catch ( IndexNotFoundKernelException | RuntimeException e )
            {
                exception = Exceptions.chain( exception, e );
            }
        }
        if ( exception != null )
        {
            throwIfUnchecked( exception );
            throwIfInstanceOf( exception, IndexNotFoundKernelException.class );
            throw new RuntimeException( exception );
        }
        return combineSamples( samples );
    }

    public static IndexSample combineSamples( Iterable<IndexSample> samples )
    {
        long indexSize = 0;
        long uniqueValues = 0;
        long sampleSize = 0;
        for ( IndexSample sample : samples )
        {
            indexSize += sample.indexSize();
            uniqueValues += sample.uniqueValues();
            sampleSize += sample.sampleSize();
        }
        return new IndexSample( indexSize, uniqueValues, sampleSize );
    }

    @Override
    public void close()
    {
        closeAllSilently( asCollection( samplers ) );
    }
}
