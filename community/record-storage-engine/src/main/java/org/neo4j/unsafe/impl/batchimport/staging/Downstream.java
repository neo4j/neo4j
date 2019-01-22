/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.unsafe.impl.batchimport.staging;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;

class Downstream
{
    private static final java.util.Comparator<TicketedBatch> TICKETED_BATCH_COMPARATOR =
            ( a, b ) -> Long.compare( b.ticket, a.ticket );

    private final Step<Object> downstream;
    private final AtomicLong doneBatches;
    private final ArrayList<TicketedBatch> batches;
    private long lastSendTicket = -1;

    Downstream( Step<Object> downstream, AtomicLong doneBatches )
    {
        this.downstream = downstream;
        this.doneBatches = doneBatches;
        batches = new ArrayList<>();
    }

    long send()
    {
        // Sort in reverse, so the elements we want to send first are at the end.
        batches.sort( TICKETED_BATCH_COMPARATOR );
        long idleTimeSum = 0;
        long batchesDone = 0;

        for ( int i = batches.size() - 1; i >= 0 ; i-- )
        {
            TicketedBatch batch = batches.get( i );
            if ( batch.ticket == lastSendTicket + 1 )
            {
                batches.remove( i );
                lastSendTicket = batch.ticket;
                idleTimeSum += downstream.receive( batch.ticket, batch.batch );
                batchesDone++;
            }
            else
            {
                break;
            }
        }

        doneBatches.getAndAdd( batchesDone );
        return idleTimeSum;
    }

    void queue( TicketedBatch batch )
    {
        // Check that this is not a marker to flush the downstream.
        if ( batch.ticket != -1 && batch.batch != null )
        {
            batches.add( batch );
        }
    }
}
