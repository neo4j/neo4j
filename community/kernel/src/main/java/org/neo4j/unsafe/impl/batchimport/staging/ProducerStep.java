/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
import java.util.Iterator;
import java.util.List;

import static java.lang.System.currentTimeMillis;

public class ProducerStep<T> extends AbstractStep<Iterator<T>>
{
    private final int batchSize;

    public ProducerStep( StageControl control, String name, int batchSize )
    {
        super( control, name );
        this.batchSize = batchSize;
    }

    @Override
    public long receive( long ticket, final Iterator<T> input )
    {
        new Thread()
        {
            @Override
            public void run()
            {
                assertHealthy();
                try
                {
                    process( input );
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

    protected void process( Iterator<T> input )
    {
        List<T> batch = new ArrayList<>( batchSize );
        int size = 0;
        long startTime = currentTimeMillis();
        while ( input.hasNext() )
        {
            batch.add( input.next() );
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
        // Increment both done and received count to have stillWorking() evaluate properly
        long ticket = doneBatches.incrementAndGet();
        receivedBatches.incrementAndGet();
        return ticket;
    }
}
