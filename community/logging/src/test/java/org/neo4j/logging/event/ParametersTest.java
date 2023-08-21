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
package org.neo4j.logging.event;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.neo4j.logging.event.Parameters.of;

import org.junit.jupiter.api.Test;

class ParametersTest {

    private final String[] keys = {"k1", "k2", "k3", "k4", null, "k6"};
    private final Object[] vals = {new Object(), "a", 1, null, 1.2f, "five"};

    @Test
    void shouldCreateEmptyParameters() {
        assertThat(Parameters.EMPTY.toString()).isEqualTo("");
    }

    @Test
    void shouldCreateParameters() {
        assertThat(of(keys[0], vals[0]).toString()).isEqualTo("[" + keys[0] + "=" + vals[0] + "]");
        assertThat(of(keys[0], vals[0], keys[1], vals[1]).toString())
                .isEqualTo("[" + keys[0] + "=" + vals[0] + ", " + keys[1] + "=" + vals[1] + "]");
        assertThat(of(keys[0], vals[0], keys[1], vals[1], keys[2], vals[2]).toString())
                .isEqualTo("[" + keys[0] + "=" + vals[0] + ", " + keys[1] + "=" + vals[1] + ", " + keys[2]
                        + "=" + vals[2]
                        + "]");
        assertThat(of(keys[0], vals[0], keys[1], vals[1], keys[2], vals[2], keys[3], vals[3])
                        .toString())
                .isEqualTo("[" + keys[0] + "=" + vals[0] + ", " + keys[1] + "=" + vals[1] + ", " + keys[2]
                        + "=" + vals[2]
                        + ", " + keys[3]
                        + "=" + vals[3] + "]");
        assertThat(of(keys[0], vals[0], keys[1], vals[1], keys[2], vals[2], keys[3], vals[3], keys[4], vals[4])
                        .toString())
                .isEqualTo("[" + keys[0] + "=" + vals[0] + ", " + keys[1] + "=" + vals[1] + ", " + keys[2]
                        + "=" + vals[2]
                        + ", " + keys[3]
                        + "=" + vals[3] + ", " + keys[4] + "=" + vals[4] + "]");
        assertThat(of(
                                keys[0], vals[0], keys[1], vals[1], keys[2], vals[2], keys[3], vals[3], keys[4],
                                vals[4], keys[5], vals[5])
                        .toString())
                .isEqualTo("[" + keys[0] + "=" + vals[0] + ", " + keys[1] + "=" + vals[1] + ", " + keys[2]
                        + "=" + vals[2]
                        + ", " + keys[3]
                        + "=" + vals[3] + ", " + keys[4] + "=" + vals[4] + ", " + keys[5]
                        + "=" + vals[5] + "]");
    }
}
