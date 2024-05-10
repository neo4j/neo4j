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
import org.neo4j.kernel.impl.transaction.log.AppendedChunkLogVersionLocator;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.NoSuchLogEntryException;

class LogVersionLocatorTest {
    private static final long firstAppendIndexInLog = 3;
    private static final long lastAppendIndexInLog = 67;

    @Test
    void findLogPosition() throws NoSuchLogEntryException {
        final long appendIndex = 42L;
        var locator = new AppendedChunkLogVersionLocator(appendIndex);
        var position = new LogPosition(1, 128);

        final boolean result = locator.visit(null, position, firstAppendIndexInLog, lastAppendIndexInLog);

        assertFalse(result);
        assertEquals(position, locator.getLogPositionOrThrow());
    }

    @Test
    void doNotFindLogPosition() {
        final long appendIndex = 1L;
        var locator = new AppendedChunkLogVersionLocator(appendIndex);
        var position = new LogPosition(1, 128);

        final boolean result = locator.visit(null, position, firstAppendIndexInLog, lastAppendIndexInLog);

        assertTrue(result);
        var e = assertThrows(NoSuchLogEntryException.class, locator::getLogPositionOrThrow);
        assertEquals(
                "Unable to find transaction or chunk with append index " + appendIndex
                        + " in any of available logical logs.",
                e.getMessage());
    }

    @Test
    void alwaysThrowIfVisitIsNotCalled() {
        final long appendIndex = 1L;
        var locator = new AppendedChunkLogVersionLocator(appendIndex);

        var e = assertThrows(NoSuchLogEntryException.class, locator::getLogPositionOrThrow);
        assertEquals(
                "Unable to find transaction or chunk with append index " + appendIndex
                        + " in any of available logical logs.",
                e.getMessage());
    }
}
