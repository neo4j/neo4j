/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.consistency.checking.full;

import org.neo4j.consistency.checking.cache.CacheAccess;
import org.neo4j.consistency.checking.cache.CacheAction;
import org.neo4j.consistency.checking.cache.CacheProcessor;
import org.neo4j.consistency.statistics.Statistics;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

import static java.lang.String.format;

public class StoreProcessorTask<R extends AbstractBaseRecord> extends ConsistencyCheckerTask
{
    private final RecordStore<R> store;
    private final StoreProcessor processor;
    private final ProgressListener progressListener;
    private final StoreAccess storeAccess;
    private final CacheAccess cacheAccess;

    StoreProcessorTask( String name, Statistics statistics, int threads, RecordStore<R> store, StoreAccess storeAccess,
            String builderPrefix, ProgressMonitorFactory.MultiPartBuilder builder, CacheAccess cacheAccess,
            StoreProcessor processor )
    {
        super( name, statistics, threads );
        this.store = store;
        this.storeAccess = storeAccess;
        this.cacheAccess = cacheAccess;
        this.processor = processor;
        this.progressListener = builder.progressForPart( name +
                indexedPartName( store.getStorageFileName().getName(), builderPrefix ), store.getHighId() );
    }

    private String indexedPartName( String storeFileName, String prefix )
    {
        return prefix.length() != 0 ? "_" : format( "%s_pass_%s", storeFileName, prefix );
    }

    @Override
    @SuppressWarnings( "unchecked" )
    public void run()
    {
        statistics.reset();
        beforeProcessing( processor );
        try
        {
            if ( processor.getStage().getCacheSlotSizes().length > 0 )
            {
                cacheAccess.setCacheSlotSizes( processor.getStage().getCacheSlotSizes() );
            }
            cacheAccess.setForward( processor.getStage().isForward() );

            if ( processor instanceof CacheProcessor )
            {
                CacheAction action = ((CacheProcessor)processor).action();
                action.processCache();
            }
            else
            {
                if ( processor.getStage().isParallel() )
                {
                    Distributor distributor = null;
                    if ( processor.getStage() == CheckStage.Stage1_NS_PropsLabels )
                    {
                        distributor = new Distributor<NodeRecord>( storeAccess.getNodeStore().getHighId(), numberOfThreads );
                    }
                    else if ( processor.getStage() == CheckStage.Stage8_PS_Props )
                    {
                        distributor = new Distributor<PropertyRecord>(
                                storeAccess.getPropertyStore().getHighId(), numberOfThreads );
                    }
                    else
                    {
                        distributor = new Distributor<RelationshipRecord>(
                                storeAccess.getRelationshipStore().getHighId(), numberOfThreads );
                    }
                    processor.applyFilteredParallel( store, distributor, progressListener );
                }
                else
                {
                    processor.applyFiltered( store, progressListener );
                }
            }
            cacheAccess.setForward( true );
        }
        catch ( Throwable e )
        {
            progressListener.failed( e );
            throw Exceptions.launderedException( e );
        }
        finally
        {
            afterProcessing( processor );
        }
        statistics.print( name );
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
    public String toString()
    {
        return getClass().getSimpleName() + "[" + name + " @ " + processor.getStage() + ", " +
                store + ":" + store.getHighId() + "]";
    }
}
