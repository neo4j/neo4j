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
package org.neo4j.kernel.impl.api.index.sampling;

import java.util.concurrent.atomic.AtomicBoolean;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.api.index.IndexSampler;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.util.DurationLogger;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;
import static org.neo4j.internal.kernel.api.InternalIndexState.ONLINE;

class OnlineIndexSamplingJob implements IndexSamplingJob
{
    private static final String INDEX_SAMPLER_TAG = "indexSampler";
    private final long indexId;
    private final IndexProxy indexProxy;
    private final IndexStatisticsStore indexStatisticsStore;
    private final Log log;
    private final String indexUserDescription;
    private final String indexName;
    private final PageCacheTracer pageCacheTracer;

    OnlineIndexSamplingJob( long indexId, IndexProxy indexProxy, IndexStatisticsStore indexStatisticsStore, String indexUserDescription, String indexName,
            LogProvider logProvider, PageCacheTracer pageCacheTracer )
    {
        this.indexId = indexId;
        this.indexProxy = indexProxy;
        this.indexStatisticsStore = indexStatisticsStore;
        this.log = logProvider.getLog( getClass() );
        this.indexUserDescription = indexUserDescription;
        this.indexName = indexName;
        this.pageCacheTracer = pageCacheTracer;
    }

    @Override
    public long indexId()
    {
        return indexId;
    }

    @Override
    public String indexName()
    {
        return indexName;
    }

    @Override
    public void run( AtomicBoolean stopped )
    {
        try ( DurationLogger durationLogger = new DurationLogger( log, "Sampling index " + indexUserDescription ) )
        {
            try
            {
                try ( var reader = indexProxy.newValueReader();
                      var cursorContext = new CursorContext( pageCacheTracer.createPageCursorTracer( INDEX_SAMPLER_TAG ) );
                      IndexSampler sampler = reader.createSampler() )
                {
                    IndexSample sample = sampler.sampleIndex( cursorContext, stopped );

                    // check again if the index is online before saving the counts in the store
                    if ( indexProxy.getState() == ONLINE )
                    {
                        indexStatisticsStore.replaceStats( indexId, sample );
                        durationLogger.markAsFinished();
                        log.debug(
                                format( "Sampled index %s with %d unique values in sample of avg size %d taken from " +
                                        "index containing %d entries",
                                        indexUserDescription, sample.uniqueValues(), sample.sampleSize(),
                                        sample.indexSize() ) );
                    }
                    else
                    {
                        durationLogger.markAsAborted( "Index no longer ONLINE" );
                    }
                }
            }
            catch ( IndexNotFoundKernelException e )
            {
                durationLogger.markAsAborted(
                        "Attempted to sample missing/already deleted index " + indexUserDescription );
            }
        }
    }
}
