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
package org.neo4j.cypher.operations;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class UUIDGenerationTest {

    @Test
    void workingGenerationGivesVersion7() {
        // Should not throw for valid currentTimeMillis() timestamp
        long timestamp = System.currentTimeMillis();
        UUID u = CypherFunctions.ofEpochMillis(timestamp);
        assertThat(u.version()).isEqualTo(7);
    }

    @Test
    void throwsForInvalidTimestamp() {
        var value = 1L << 48;
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> CypherFunctions.ofEpochMillis(value));
    }

    @Test
    void throwsForNegativeTimestamp() {
        var value = -0xFEDCBA987654L;
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> CypherFunctions.ofEpochMillis(value));
    }

    @Test
    void variantIsIETF() {
        UUID u = CypherFunctions.ofEpochMillis(System.currentTimeMillis());
        assertThat(u.variant()).isEqualTo(2);
    }

    @Test
    void timestampRoundTrips() {
        long timestamp = System.currentTimeMillis();
        UUID u = CypherFunctions.ofEpochMillis(timestamp);
        long extracted = u.getMostSignificantBits() >>> 16;
        assertThat(extracted).isEqualTo(timestamp);
    }

    @Test
    void uniquenessWithSameTimestamp() {
        long timestamp = System.currentTimeMillis();
        Set<UUID> uuids = new HashSet<>();
        for (int i = 0; i < 1000; i++) {
            uuids.add(CypherFunctions.ofEpochMillis(timestamp));
        }
        assertThat(uuids).hasSize(1000);
    }

    @Test
    void temporalOrdering() {
        UUID earlier = CypherFunctions.ofEpochMillis(1000L);
        UUID later = CypherFunctions.ofEpochMillis(2000L);
        assertThat(earlier.compareTo(later)).isLessThan(0);
    }

    @Test
    void boundaryTimestamps() {
        UUID atEpoch = CypherFunctions.ofEpochMillis(0L);
        assertThat(atEpoch.version()).isEqualTo(7);
        assertThat(atEpoch.variant()).isEqualTo(2);

        UUID atMax = CypherFunctions.ofEpochMillis((1L << 48) - 1);
        assertThat(atMax.version()).isEqualTo(7);
        assertThat(atMax.variant()).isEqualTo(2);
    }
}
