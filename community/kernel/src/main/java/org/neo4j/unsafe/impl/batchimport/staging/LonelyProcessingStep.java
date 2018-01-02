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
package org.neo4j.unsafe.impl.batchimport.staging;

import org.neo4j.unsafe.impl.batchimport.stats.StatsProvider;

import static java.lang.System.currentTimeMillis;

/**
 * {@link Step} that doesn't receive batches, doesn't send batches downstream; just processes data.
 */
public abstract class LonelyProcessingStep extends AbstractStep<Void>
{
    private final int batchSize;
    private int batch;
    private long lastProcessingTimestamp;

    public LonelyProcessingStep( StageControl control, String name, Configuration config,
            StatsProvider... additionalStatsProviders )
    {
        super( control, name, config, additionalStatsProviders );
        this.batchSize = config.batchSize();
    }

    @Override
    public long receive( long ticket, Void nothing )
    {
        new Thread()
        {
            @Override
            public void run()
            {
                assertHealthy();
                try
                {
                    try
                    {
                        lastProcessingTimestamp = currentTimeMillis();
                        process();
                    }
                    catch ( Throwable e )
                    {
                        // we need to update panic state before ending upstream and notifying executor that we completed
                        issuePanic( e );
                    }
                    finally
                    {
                        endOfUpstream();
                    }
                }
                catch ( Throwable e )
                {
                    // to avoid cases when we hide original panic problem
                    // check first if we already in panic state and if so - rethrow original panic cause
                    if ( !isPanic() )
                    {
                        issuePanic( e );
                    }
                    else
                    {
                        throw e;
                    }
                }
            }
        }.start();
        return 0;
    }

    /**
     * Called once and signals the start of this step. Responsible for calling {@link #progress(long)}
     * at least now and then.
     */
    protected abstract void process();

    /**
     * Called from {@link #process()}, reports progress so that statistics are updated appropriately.
     *
     * @param amount number of items processed since last call to this method.
     */
    protected void progress( long amount )
    {
        batch += amount;
        if ( batch >= batchSize )
        {
            int batches = batch / batchSize;
            batch %= batchSize;
            doneBatches.addAndGet( batches );
            long time = currentTimeMillis();
            totalProcessingTime.add( time - lastProcessingTimestamp );
            lastProcessingTimestamp = time;
        }
    }
}
