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
package org.neo4j.bolt.negotiation;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.bolt.negotiation.version.ProtocolVersion;
import org.neo4j.bolt.negotiation.version.ProtocolVersionReducer;
import org.neo4j.bolt.testing.assertions.ProtocolVersionAssertions;

class ProtocolVersionReducerTest {

    private ProtocolVersionReducer reducer;

    @BeforeEach
    void prepare() {
        this.reducer = new ProtocolVersionReducer();
    }

    @Test
    void shouldHandleEmptySets() {
        var result = this.reducer.finalizeResult();

        Assertions.assertThat(result).isEmpty();
    }

    @Test
    void shouldHandleSingleVersions() {
        this.reducer.submit(new ProtocolVersion(5, 1));

        var result = this.reducer.finalizeResult();

        Assertions.assertThat(result)
                .hasSize(1)
                .element(0, ProtocolVersionAssertions.protocolVersion())
                .hasMajor(5)
                .hasMinor(1)
                .representsSingleVersion();
    }

    @Test
    void shouldGroupSequentialVersions() {
        this.reducer.submit(new ProtocolVersion(5, 3));
        this.reducer.submit(new ProtocolVersion(5, 4));
        this.reducer.submit(new ProtocolVersion(5, 5));

        var result = this.reducer.finalizeResult();

        Assertions.assertThat(result)
                .hasSize(1)
                .element(0, ProtocolVersionAssertions.protocolVersion())
                .hasMajor(5)
                .hasMinor(5)
                .hasRange(2);
    }

    @Test
    void shouldGroupVersionsByMinorGap() {
        this.reducer.submit(new ProtocolVersion(5, 3));
        this.reducer.submit(new ProtocolVersion(5, 5));
        this.reducer.submit(new ProtocolVersion(5, 6));
        this.reducer.submit(new ProtocolVersion(5, 8));

        var result = this.reducer.finalizeResult();

        Assertions.assertThat(result).hasSize(3);

        Assertions.assertThat(result)
                .element(0, ProtocolVersionAssertions.protocolVersion())
                .hasMajor(5)
                .hasMinor(3)
                .representsSingleVersion();

        Assertions.assertThat(result)
                .element(1, ProtocolVersionAssertions.protocolVersion())
                .hasMajor(5)
                .hasMinor(6)
                .hasRange(1);

        Assertions.assertThat(result)
                .element(2, ProtocolVersionAssertions.protocolVersion())
                .hasMajor(5)
                .hasMinor(8)
                .representsSingleVersion();
    }

    @Test
    void shouldGroupVersionsByMajorGap() {
        this.reducer.submit(new ProtocolVersion(5, 3));
        this.reducer.submit(new ProtocolVersion(6, 4));
        this.reducer.submit(new ProtocolVersion(6, 5));
        this.reducer.submit(new ProtocolVersion(7, 6));

        var result = this.reducer.finalizeResult();

        Assertions.assertThat(result).hasSize(3);

        Assertions.assertThat(result)
                .element(0, ProtocolVersionAssertions.protocolVersion())
                .hasMajor(5)
                .hasMinor(3)
                .representsSingleVersion();

        Assertions.assertThat(result)
                .element(1, ProtocolVersionAssertions.protocolVersion())
                .hasMajor(6)
                .hasMinor(5)
                .hasRange(1);

        Assertions.assertThat(result)
                .element(2, ProtocolVersionAssertions.protocolVersion())
                .hasMajor(7)
                .hasMinor(6)
                .representsSingleVersion();
    }
}
