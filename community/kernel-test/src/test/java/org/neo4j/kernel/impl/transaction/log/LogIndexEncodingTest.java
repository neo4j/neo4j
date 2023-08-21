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
package org.neo4j.kernel.impl.transaction.log;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.kernel.impl.transaction.log.LogIndexEncoding.decodeLogIndex;
import static org.neo4j.kernel.impl.transaction.log.LogIndexEncoding.encodeLogIndex;

import org.junit.jupiter.api.Test;

class LogIndexEncodingTest {
    @Test
    void shouldEncodeIndexAsBytes() {
        long index = 123_456_789_012_567L;
        byte[] bytes = encodeLogIndex(index);
        assertEquals(index, decodeLogIndex(bytes));
    }

    @Test
    void shouldThrowExceptionForAnEmptyByteArray() {
        assertThatThrownBy(() -> decodeLogIndex(new byte[1]))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unable to decode log index from the transaction header.");
    }
}
