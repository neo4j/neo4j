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
package org.neo4j.shell.state;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Optional;
import org.junit.jupiter.api.Test;

class LicenseDetailsTest {
    @Test
    void parseYes() {
        final var license = LicenseDetails.parse("yes", 0, 0);
        assertEquals(license.status(), LicenseDetails.Status.YES);
        assertEquals(Optional.empty(), license.daysLeft());
        assertEquals(Optional.empty(), license.trialDays());
    }

    @Test
    void parseDays() {
        final var license = LicenseDetails.parse("eval", 12, 30);
        assertEquals(license.status(), LicenseDetails.Status.EVAL);
        assertEquals(Optional.of(12L), license.daysLeft());
        assertEquals(Optional.of(30L), license.trialDays());
    }

    @Test
    void parseExpired() {
        final var license = LicenseDetails.parse("expired", -1, 120);
        assertEquals(license.status(), LicenseDetails.Status.EXPIRED);
        assertEquals(Optional.of(0L), license.daysLeft());
        assertEquals(Optional.of(120L), license.trialDays());
    }

    @Test
    void parseNo() {
        final var license = LicenseDetails.parse("no", 0, 0);
        assertEquals(license.status(), LicenseDetails.Status.NO);
        assertEquals(Optional.empty(), license.daysLeft());
        assertEquals(Optional.empty(), license.trialDays());
    }

    @Test
    void parseEmptyString() {
        assertThrows(IllegalArgumentException.class, () -> LicenseDetails.parse("", 0, 0));
    }

    @Test
    void parseOtherStatus() {
        assertThrows(IllegalArgumentException.class, () -> LicenseDetails.parse("other", 0, 0));
    }
}
