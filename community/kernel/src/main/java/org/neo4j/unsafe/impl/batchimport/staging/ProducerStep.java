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

import java.util.ArrayList;
import java.util.List;

import static java.lang.System.currentTimeMillis;

/**
 * {@link Step} that is the start of the line of steps in a {@link Stage}. It produces batches of data
 * and sends downstream.
 */
public abstract class ProducerStep<T> extends AbstractStep<Void>
{
    private final int batchSize;

    public ProducerStep( StageControl control, String name, int batchSize )
    {
        super( control, name );
        this.batchSize = batchSize;
    }

    protected abstract T nextOrNull();

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
                    process();
                    endOfUpstream();
                }
                catch ( Throwable e )
                {
                    issuePanic( e );
                }
            }
        }.start();
        return 0;
    }

    protected void process()
    {
        List<T> batch = new ArrayList<>( batchSize );
        int size = 0;
        long startTime = currentTimeMillis();
        T next = null;
        while ( (next = nextOrNull()) != null )
        {
            batch.add( next );
            if ( ++size == batchSize )
            {   // Full batch
                totalProcessingTime.addAndGet( currentTimeMillis()-startTime );

                // Increment both received and done batch count
                sendDownstream( nextTicket(), batch );

                batch = new ArrayList<>( batchSize );
                size = 0;
                assertHealthy();
                startTime = currentTimeMillis();
            }
        }

        if ( size > 0 )
        {   // Last batch
            totalProcessingTime.addAndGet( currentTimeMillis()-startTime );
            sendDownstream( nextTicket(), batch );
        }
    }

    private long nextTicket()
    {
        return doneBatches.incrementAndGet();
    }
}
