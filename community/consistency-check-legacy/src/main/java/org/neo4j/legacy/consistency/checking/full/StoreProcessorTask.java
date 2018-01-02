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
package org.neo4j.legacy.consistency.checking.full;

import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;

import static java.lang.String.format;

class StoreProcessorTask<R extends AbstractBaseRecord> implements StoppableRunnable
{
    private final RecordStore<R> store;
    private final StoreProcessor[] processors;
    private final ProgressListener[] progressListeners;


    StoreProcessorTask( RecordStore<R> store,
                        ProgressMonitorFactory.MultiPartBuilder builder,
                        TaskExecutionOrder order, StoreProcessor singlePassProcessor,
                        StoreProcessor... multiPassProcessors )
    {
        this( store, "", builder, order, singlePassProcessor, multiPassProcessors );
    }

    StoreProcessorTask( RecordStore<R> store, String builderPrefix,
                        ProgressMonitorFactory.MultiPartBuilder builder,
                        TaskExecutionOrder order, StoreProcessor singlePassProcessor,
                        StoreProcessor... multiPassProcessors )
    {
        this.store = store;
        String storeFileName = store.getStorageFileName().getName();

        String sanitizedBuilderPrefix = builderPrefix == null ? "" : builderPrefix;

        if ( order == TaskExecutionOrder.MULTI_PASS )
        {
            this.processors = multiPassProcessors;
            this.progressListeners = new ProgressListener[multiPassProcessors.length];
            for ( int i = 0; i < multiPassProcessors.length; i++ )
            {
                String partName = indexedPartName( storeFileName, sanitizedBuilderPrefix, i );
                progressListeners[i] = builder.progressForPart( partName, store.getHighId() );
            }
        }
        else
        {
            this.processors = new StoreProcessor[]{singlePassProcessor};
            String partName = partName( storeFileName, sanitizedBuilderPrefix );
            this.progressListeners = new ProgressListener[]{
                    builder.progressForPart( partName, store.getHighId() )};
        }
    }

    private String partName( String storeFileName, String builderPrefix )
    {
        return builderPrefix.length() == 0 ? storeFileName : format("%s_run_%s", storeFileName, builderPrefix );
    }

    private String indexedPartName( String storeFileName, String prefix, int i )
    {
        if ( prefix.length() != 0 )
        {
            prefix += "_";
        }
        return format( "%s_pass_%s%d", storeFileName, prefix, i );
    }

    @SuppressWarnings("unchecked")
    @Override
    public void run()
    {
        for ( int i = 0; i < processors.length; i++ )
        {
            StoreProcessor processor = processors[i];
            beforeProcessing(processor);
            try
            {
                processor.applyFiltered( store, progressListeners[i] );
            }
            catch ( Throwable e )
            {
                progressListeners[i].failed( e );
            }
            finally
            {
                afterProcessing(processor);
            }
        }
    }

    protected void beforeProcessing( StoreProcessor processor )
    {
        // intentionally empty
    }

    protected void afterProcessing( StoreProcessor processor )
    {
        // intentionally empty
    }

    @Override
    public void stopScanning()
    {
        processors[0].stop();
    }

}
