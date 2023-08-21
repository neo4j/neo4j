/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.dbms.database;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

class TicketMachine {
    private final AtomicLong currentTicketId = new AtomicLong();

    Ticket newTicket() {
        return new Ticket(currentTicketId::get);
    }

    Barrier nextBarrier() {
        return new Barrier(currentTicketId.incrementAndGet());
    }

    static class Ticket {
        private final LongSupplier ticketIdSupplier;
        private volatile long ticketId;

        private Ticket(LongSupplier ticketIdSupplier) {
            this.ticketIdSupplier = ticketIdSupplier;
        }

        void use() {
            this.ticketId = ticketIdSupplier.getAsLong();
        }
    }

    record Barrier(long barrier) {
        static final Barrier NO_BARRIER = new Barrier(Long.MAX_VALUE);

        boolean canPass(Ticket ticket) {
            return ticket.ticketId < barrier;
        }
    }
}
