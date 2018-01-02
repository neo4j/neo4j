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

import static java.lang.System.currentTimeMillis;

/**
 * Step that generally sits first in a {@link Stage} and produces batches that will flow downstream
 * to other {@link Step steps}.
 */
public abstract class ProducerStep extends AbstractStep<Void>
{
    protected final int batchSize;

    public ProducerStep( StageControl control, String name, Configuration config )
    {
        super( control, name, config );
        this.batchSize = config.batchSize();
    }

    /**
     * Merely receives one call, like a start signal from the staging framework.
     */
    @Override
    public long receive( long ticket, Void batch )
    {
        // It's fone to not store a reference to this thread here because either it completes and exits
        // normally, notices a panic and exits via an exception.
        new Thread( "PRODUCER" )
        {
            @Override
            public void run()
            {
                assertHealthy();
                try
                {
                    process();
                    endOfUpstream();
                }
                catch ( Throwable e )
                {
                    issuePanic( e, false );
                }
            }
        }.start();
        return 0;
    }

    /**
     * Forms batches out of some sort of data stream and sends these batches downstream.
     */
    @SuppressWarnings( "unchecked" )
    protected void process()
    {
        Object batch = null;
        long startTime = currentTimeMillis();
        while ( (batch = nextBatchOrNull( doneBatches.get(), batchSize )) != null )
        {
            totalProcessingTime.add( currentTimeMillis()-startTime );
            downstreamIdleTime.addAndGet( downstream.receive( doneBatches.getAndIncrement(), batch ) );
            assertHealthy();
            startTime = currentTimeMillis();
        }
    }

    /**
     * Generates next batch object with a target size of {@code batchSize} items from its data stream in it.
     * @param batchSize number of items to grab from its data stream (whatever a subclass defines as a data stream).
     * @return the batch object to send downstream, or null if the data stream came to an end.
     */
    protected abstract Object nextBatchOrNull( long ticket, int batchSize );
}
