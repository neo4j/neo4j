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
package org.neo4j.kernel.impl.api.index.sampling;

import java.util.function.Predicate;

import org.neo4j.internal.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.index.IndexMapSnapshotProvider;
import org.neo4j.kernel.impl.api.index.IndexStoreView;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.register.Register.DoubleLongRegister;

import static org.neo4j.register.Registers.newDoubleLongRegister;

public class IndexSamplingControllerFactory
{
    private final IndexSamplingConfig config;
    private final IndexStoreView storeView;
    private final JobScheduler scheduler;
    private final TokenNameLookup tokenNameLookup;
    private final LogProvider logProvider;

    public IndexSamplingControllerFactory( IndexSamplingConfig config, IndexStoreView storeView,
                                           JobScheduler scheduler, TokenNameLookup tokenNameLookup,
                                           LogProvider logProvider )
    {
        this.config = config;
        this.storeView = storeView;
        this.scheduler = scheduler;
        this.tokenNameLookup = tokenNameLookup;
        this.logProvider = logProvider;
    }

    public IndexSamplingController create( IndexMapSnapshotProvider snapshotProvider )
    {
        OnlineIndexSamplingJobFactory jobFactory =
                new OnlineIndexSamplingJobFactory( storeView, tokenNameLookup, logProvider );
        Predicate<Long> samplingUpdatePredicate = createSamplingPredicate();
        IndexSamplingJobQueue<Long> jobQueue = new IndexSamplingJobQueue<>( samplingUpdatePredicate );
        IndexSamplingJobTracker jobTracker = new IndexSamplingJobTracker( config, scheduler );
        IndexSamplingController.RecoveryCondition
                indexRecoveryCondition = createIndexRecoveryCondition( logProvider, tokenNameLookup );
        return new IndexSamplingController(
                config, jobFactory, jobQueue, jobTracker, snapshotProvider, scheduler, indexRecoveryCondition
        );
    }

    private Predicate<Long> createSamplingPredicate()
    {
        return new Predicate<Long>()
        {
            private final DoubleLongRegister output = newDoubleLongRegister();

            @Override
            public boolean test( Long indexId )
            {
                storeView.indexUpdatesAndSize( indexId, output );
                long updates = output.readFirst();
                long size = output.readSecond();
                long threshold = Math.round( config.updateRatio() * size );
                return updates > threshold;
            }
        };
    }

    private IndexSamplingController.RecoveryCondition createIndexRecoveryCondition( final LogProvider logProvider,
                                                                     final TokenNameLookup tokenNameLookup )
    {
        return new IndexSamplingController.RecoveryCondition()
        {
            private final Log log = logProvider.getLog( IndexSamplingController.class );
            private final DoubleLongRegister register = newDoubleLongRegister();

            @Override
            public boolean test( long indexId, IndexDescriptor descriptor )
            {
                boolean result = storeView.indexSample( indexId, register ).readSecond() == 0;
                if ( result )
                {
                    log.debug( "Recovering index sampling for index %s",
                            descriptor.schema().userDescription( tokenNameLookup ) );
                }
                return result;
            }
        };
    }
}
