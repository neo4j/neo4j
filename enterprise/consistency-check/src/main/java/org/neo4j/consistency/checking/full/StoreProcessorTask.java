/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.consistency.checking.full;

import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;
import org.neo4j.kernel.impl.nioneo.store.RecordStore;

class StoreProcessorTask<R extends AbstractBaseRecord> implements Runnable
{
    private final RecordStore<R> store;
    private final StoreProcessor[] processors;
    private final ProgressListener[] progressListeners;

    StoreProcessorTask( RecordStore<R> store, ProgressMonitorFactory.MultiPartBuilder builder,
                        TaskExecutionOrder order, StoreProcessor singlePassProcessor,
                        StoreProcessor... multiPassProcessors )
    {
        this.store = store;
        String storeFileName = store.getStorageFileName().substring(
                store.getStorageFileName().lastIndexOf( '/' ) + 1 );

        if ( order == TaskExecutionOrder.MULTI_PASS )
        {
            this.processors = multiPassProcessors;
            this.progressListeners = new ProgressListener[multiPassProcessors.length];
            for ( int i = 0; i < multiPassProcessors.length; i++ )
            {
                progressListeners[i] = builder.progressForPart( storeFileName + "_pass_" + i, store.getHighId() );
            }
        }
        else
        {
            this.processors = new StoreProcessor[]{singlePassProcessor};
            this.progressListeners = new ProgressListener[]{
                    builder.progressForPart( storeFileName, store.getHighId() )};
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void run()
    {
        for ( int i = 0; i < processors.length; i++ )
        {
            try
            {
                processors[i].applyFiltered( store, progressListeners[i] );
            }
            catch ( Throwable e )
            {
                progressListeners[i].failed( e );
            }
        }
    }

    public void stopScanning()
    {
        processors[0].stopScanning();
    }

}
