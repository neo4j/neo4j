/**
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
package org.neo4j.unsafe.impl.batchimport.staging;

import static java.lang.System.currentTimeMillis;

/**
 * {@link Step} that doesn't receive batches, doesn't send batches downstream; just processes data.
 */
public abstract class LonelyProcessingStep extends AbstractStep<Void>
{
    private final int batchSize;
    private int batch;
    private long lastProcessingTimestamp;

    public LonelyProcessingStep( StageControl control, String name, int batchSize )
    {
        super( control, name );
        this.batchSize = batchSize;
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
                    lastProcessingTimestamp = currentTimeMillis();
                    process();
                }
                catch ( Throwable e )
                {
                    issuePanic( e );
                }
                finally
                {
                    endOfUpstream();
                }
            }
        }.start();
        return 0;
    }

    protected abstract void process();

    protected void itemProcessed()
    {
        if ( ++batch == batchSize )
        {
            doneBatches.incrementAndGet();
            batch = 0;
            long time = currentTimeMillis();
            totalProcessingTime.addAndGet( time - lastProcessingTimestamp );
            lastProcessingTimestamp = time;
        }
    }
}
