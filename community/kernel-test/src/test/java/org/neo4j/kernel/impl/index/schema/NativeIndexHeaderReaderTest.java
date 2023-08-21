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
package org.neo4j.kernel.impl.index.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.neo4j.kernel.impl.index.schema.NativeIndexPopulator.BYTE_FAILED;

import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

class NativeIndexHeaderReaderTest {
    @Test
    void mustReportFailedIfNoHeader() {
        ByteBuffer emptyBuffer = ByteBuffer.wrap(new byte[0]);
        NativeIndexHeaderReader nativeIndexHeaderReader = new NativeIndexHeaderReader();
        nativeIndexHeaderReader.read(emptyBuffer);
        assertSame(BYTE_FAILED, nativeIndexHeaderReader.state);
        assertThat(nativeIndexHeaderReader.failureMessage)
                .contains(
                        "Could not read header, most likely caused by index not being fully constructed. Index needs to be recreated. Stacktrace:");
    }

    @Test
    void mustNotThrowIfHeaderLongEnough() {
        ByteBuffer emptyBuffer = ByteBuffer.wrap(new byte[1]);
        NativeIndexHeaderReader nativeIndexHeaderReader = new NativeIndexHeaderReader();
        nativeIndexHeaderReader.read(emptyBuffer);
    }
}
