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
package org.neo4j.kernel.impl.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.NoSuchTransactionException;
import org.neo4j.kernel.impl.transaction.log.TransactionLogVersionLocator;

class LogVersionLocatorTest {
    private static final long firstTxIdInLog = 3;
    private static final long lastTxIdInLog = 67;

    @Test
    void shouldFindLogPosition() throws NoSuchTransactionException {
        // given
        final long txId = 42L;

        final TransactionLogVersionLocator locator = new TransactionLogVersionLocator(txId);

        final LogPosition position = new LogPosition(1, 128);

        // when
        final boolean result = locator.visit(null, position, firstTxIdInLog, lastTxIdInLog);

        // then
        assertFalse(result);
        assertEquals(position, locator.getLogPositionOrThrow());
    }

    @Test
    void shouldNotFindLogPosition() {
        // given
        final long txId = 1L;

        final TransactionLogVersionLocator locator = new TransactionLogVersionLocator(txId);

        final LogPosition position = new LogPosition(1, 128);

        // when
        final boolean result = locator.visit(null, position, firstTxIdInLog, lastTxIdInLog);

        // then
        assertTrue(result);

        var e = assertThrows(NoSuchTransactionException.class, locator::getLogPositionOrThrow);
        assertEquals(
                "Unable to find transaction " + txId + " in any of my logical logs: "
                        + "Couldn't find any log containing " + txId,
                e.getMessage());
    }

    @Test
    void shouldAlwaysThrowIfVisitIsNotCalled() {
        // given
        final long txId = 1L;

        final TransactionLogVersionLocator locator = new TransactionLogVersionLocator(txId);

        var e = assertThrows(NoSuchTransactionException.class, locator::getLogPositionOrThrow);
        assertEquals(
                "Unable to find transaction " + txId + " in any of my logical logs: "
                        + "Couldn't find any log containing " + txId,
                e.getMessage());
    }
}
