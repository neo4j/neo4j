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
package org.neo4j.storageengine.util;

import static java.lang.Math.max;
import static java.lang.Runtime.getRuntime;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.KernelVersion.DEFAULT_BOOTSTRAP_VERSION;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.test.Race;

class HighestTransactionIdTest {
    @Test
    void shouldHardSetHighest() {
        // GIVEN
        HighestTransactionId highest =
                new HighestTransactionId(new TransactionId(10, DEFAULT_BOOTSTRAP_VERSION, 10, 10, 11));

        // WHEN
        highest.set(8, DEFAULT_BOOTSTRAP_VERSION, 1299128, 42, 43);

        // THEN
        assertEquals(new TransactionId(8, DEFAULT_BOOTSTRAP_VERSION, 1299128, 42, 43), highest.get());
    }

    @Test
    void shouldOnlyKeepTheHighestOffered() {
        // GIVEN
        HighestTransactionId highest =
                new HighestTransactionId(new TransactionId(-1, DEFAULT_BOOTSTRAP_VERSION, -1, -1, -1));

        // WHEN/THEN
        assertAccepted(highest, 2);
        assertAccepted(highest, 5);
        assertRejected(highest, 3);
        assertRejected(highest, 4);
        assertAccepted(highest, 10);
    }

    @Test
    void shouldKeepHighestDuringConcurrentOfferings() throws Throwable {
        // GIVEN
        final HighestTransactionId highest =
                new HighestTransactionId(new TransactionId(-1, DEFAULT_BOOTSTRAP_VERSION, -1, -1, -1));
        Race race = new Race();
        int updaters = max(2, getRuntime().availableProcessors());
        final AtomicInteger accepted = new AtomicInteger();
        for (int i = 0; i < updaters; i++) {
            final long id = i + 1;
            race.addContestant(() -> {
                if (highest.offer(id, DEFAULT_BOOTSTRAP_VERSION, (int) id, id, id)) {
                    accepted.incrementAndGet();
                }
            });
        }

        // WHEN
        race.go();

        // THEN
        assertTrue(accepted.get() > 0);
        assertEquals(updaters, highest.get().id());
    }

    private static void assertAccepted(HighestTransactionId highest, long txId) {
        TransactionId current = highest.get();
        assertTrue(highest.offer(txId, DEFAULT_BOOTSTRAP_VERSION, -1, -1, -1));
        assertTrue(txId > current.id());
    }

    private static void assertRejected(HighestTransactionId highest, long txId) {
        TransactionId current = highest.get();
        assertFalse(highest.offer(txId, DEFAULT_BOOTSTRAP_VERSION, -1, -1, -1));
        assertEquals(current, highest.get());
    }
}
