/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.internal.batchimport;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

import org.neo4j.internal.batchimport.staging.BatchSender;
import org.neo4j.internal.batchimport.staging.ProcessorStep;
import org.neo4j.internal.batchimport.staging.StageControl;
import org.neo4j.internal.batchimport.staging.Step;
import org.neo4j.internal.batchimport.stats.StatsProvider;
import org.neo4j.io.IOUtils;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;

/**
 * {@link RecordProcessor} in {@link Step Step-form}.
 */
public class RecordProcessorStep<T extends AbstractBaseRecord> extends ProcessorStep<T[]>
{
    private final Supplier<RecordProcessor<T>> processorFactory;
    private final boolean endOfLine;
    private final List<RecordProcessor<T>> allProcessors = Collections.synchronizedList( new ArrayList<>() );
    private final ThreadLocal<RecordProcessor<T>> threadProcessors = new ThreadLocal<>()
    {
        @Override
        protected RecordProcessor<T> initialValue()
        {
            RecordProcessor<T> processor = processorFactory.get();
            allProcessors.add( processor );
            return processor;
        }
    };

    public RecordProcessorStep( StageControl control, String name, Configuration config,
            Supplier<RecordProcessor<T>> processorFactory, boolean endOfLine, int maxProcessors, PageCacheTracer pageCacheTracer,
            StatsProvider... additionalStatsProviders )
    {
        super( control, name, config, maxProcessors, pageCacheTracer, additionalStatsProviders );
        this.processorFactory = processorFactory;
        this.endOfLine = endOfLine;
    }

    @Override
    protected void process( T[] batch, BatchSender sender, PageCursorTracer cursorTracer )
    {
        RecordProcessor<T> processor = threadProcessors.get();
        for ( T item : batch )
        {
            if ( item != null && item.inUse() )
            {
                if ( !processor.process( item, cursorTracer ) )
                {
                    // No change for this record
                    item.setInUse( false );
                }
            }
        }

        // This step can be used in different stage settings, possible as the last step,
        // where nothing should be emitted
        if ( !endOfLine )
        {
            sender.send( batch );
        }
    }

    @Override
    protected void done()
    {
        super.done();
        Iterator<RecordProcessor<T>> processors = allProcessors.iterator();
        if ( processors.hasNext() )
        {
            RecordProcessor<T> first = processors.next();
            while ( processors.hasNext() )
            {
                first.mergeResultsFrom( processors.next() );
            }
            first.done();
        }
    }

    @Override
    public void close() throws Exception
    {
        super.close();
        IOUtils.closeAll( allProcessors );
    }
}
