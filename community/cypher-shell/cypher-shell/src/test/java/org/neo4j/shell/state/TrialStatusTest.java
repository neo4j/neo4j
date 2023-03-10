/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.shell.state;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import org.junit.jupiter.api.Test;

class TrialStatusTest {
    @Test
    void parseYes() {
        final var status = TrialStatus.parse("yes", 0, 0);
        assertFalse(status.expired());
        assertEquals(Optional.empty(), status.daysLeft());
        assertEquals(Optional.empty(), status.trialDays());
    }

    @Test
    void parseDays() {
        final var status = TrialStatus.parse("eval", 12, 30);
        assertFalse(status.expired());
        assertEquals(Optional.of(12L), status.daysLeft());
        assertEquals(Optional.of(30L), status.trialDays());
    }

    @Test
    void parseExpired() {
        final var status = TrialStatus.parse("expired", -1, 120);
        assertTrue(status.expired());
        assertEquals(Optional.of(-1L), status.daysLeft());
        assertEquals(Optional.of(120L), status.trialDays());
    }

    @Test
    void parseNo() {
        final var status = TrialStatus.parse("no", 0, 0);
        assertFalse(status.expired());
        assertEquals(Optional.empty(), status.daysLeft());
        assertEquals(Optional.empty(), status.trialDays());
    }

    @Test
    void parseEmptyString() {
        assertThrows(IllegalArgumentException.class, () -> TrialStatus.parse("", 0, 0));
    }

    @Test
    void parseOtherStatus() {
        assertThrows(IllegalArgumentException.class, () -> TrialStatus.parse("other", 0, 0));
    }
}
