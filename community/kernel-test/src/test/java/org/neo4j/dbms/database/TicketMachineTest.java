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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class TicketMachineTest {
    private final TicketMachine ticketMachine = new TicketMachine();

    @Test
    void ticketsShouldBeValidUntilUsed() {
        // Given
        var barrier = ticketMachine.nextBarrier();
        // When
        var ticket = ticketMachine.newTicket();
        // Then
        assertThat(barrier.canPass(ticket)).isTrue();
        // When
        ticket.use();
        // Then
        assertThat(barrier.canPass(ticket)).isFalse();
    }

    @Test
    void ticketsShouldBeValidForNewBarriersUntilUsed() {
        // Given
        var ticket = ticketMachine.newTicket();
        ticket.use();
        // When
        var barrier = ticketMachine.nextBarrier();
        // Then
        assertThat(barrier.canPass(ticket)).isTrue();
        // When
        ticket.use();
        // Then
        assertThat(barrier.canPass(ticket)).isFalse();
    }

    @Test
    void multipleBarriersCanExistSimultaneously() {
        // Given
        var ticket = ticketMachine.newTicket();
        var barrier1 = ticketMachine.nextBarrier();
        // When
        ticket.use();
        var barrier2 = ticketMachine.nextBarrier();
        // Then
        assertThat(barrier1.canPass(ticket)).isFalse();
        assertThat(barrier2.canPass(ticket)).isTrue();
    }
}
