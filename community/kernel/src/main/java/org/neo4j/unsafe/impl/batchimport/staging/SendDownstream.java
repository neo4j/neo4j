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

import java.util.concurrent.atomic.LongAdder;

import org.neo4j.concurrent.Work;

class SendDownstream implements Work<Downstream,SendDownstream>
{
    private final LongAdder downstreamIdleTime;
    private TicketedBatch head;
    private TicketedBatch tail;

    SendDownstream( long ticket, Object batch, LongAdder downstreamIdleTime )
    {
        this.downstreamIdleTime = downstreamIdleTime;
        TicketedBatch b = new TicketedBatch( ticket, batch );
        head = b;
        tail = b;
    }

    @Override
    public SendDownstream combine( SendDownstream work )
    {
        tail.next = work.head;
        tail = work.tail;
        return this;
    }

    @Override
    public void apply( Downstream downstream )
    {
        TicketedBatch next = head;
        do
        {
            downstream.queue( next );
            next = next.next;
        }
        while ( next != null );
        downstreamIdleTime.add( downstream.send() );
    }
}
