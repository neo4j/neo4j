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
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;

class LogPositionMarkerTest {
    @Test
    void shouldReturnUnspecifiedIfNothingHasBeenMarked() {
        // given
        final LogPositionMarker marker = new LogPositionMarker();

        // when
        final LogPosition logPosition = marker.newPosition();

        // given
        assertEquals(LogPosition.UNSPECIFIED, logPosition);
    }

    @Test
    void shouldReturnTheMarkedPosition() {
        // given
        final LogPositionMarker marker = new LogPositionMarker();

        // when
        marker.mark(1, 2);
        final LogPosition logPosition = marker.newPosition();

        // given
        assertEquals(new LogPosition(1, 2), logPosition);
    }

    @Test
    void shouldReturnUnspecifiedWhenAskedTo() {
        // given
        final LogPositionMarker marker = new LogPositionMarker();

        // when
        marker.mark(1, 2);
        marker.unspecified();
        final LogPosition logPosition = marker.newPosition();

        // given
        assertEquals(LogPosition.UNSPECIFIED, logPosition);
    }

    @Test
    void isMarkerInLog() {
        // given
        final LogPositionMarker marker = new LogPositionMarker();

        // when
        final var logVersion = 1;
        marker.mark(logVersion, 2);
        assertTrue(marker.isMarkerInLog(logVersion), "should match the log version");
        assertFalse(marker.isMarkerInLog(logVersion - 1), "should NOT match the log version");
        assertFalse(marker.isMarkerInLog(logVersion + 1), "should NOT match the log version");

        marker.unspecified();
        assertFalse(marker.isMarkerInLog(logVersion), "should NOT match for the unspecified marker");
    }
}
