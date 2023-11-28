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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.io.ByteUnit.kibiBytes;

import java.io.IOException;
import java.nio.ByteOrder;
import org.junit.jupiter.api.Test;
import org.neo4j.io.fs.ReadPastEndException;
import org.neo4j.io.memory.ByteBuffers;
import org.neo4j.memory.EmptyMemoryTracker;

class InMemoryClosableChannelTest {
    @Test
    void throwReadPastEndExceptionOnReadExhaust() throws IOException {
        try (var channel = new InMemoryClosableChannel()) {
            channel.put((byte) 0);
            channel.put((byte) 1);
            channel.put((byte) 2);
            channel.put((byte) 3);

            var buffer = ByteBuffers.allocate((int) kibiBytes(1), ByteOrder.LITTLE_ENDIAN, EmptyMemoryTracker.INSTANCE);

            channel.read(buffer);
            buffer.flip();

            assertEquals(0, buffer.get());
            assertEquals(1, buffer.get());
            assertEquals(2, buffer.get());
            assertEquals(3, buffer.get());

            assertThrows(ReadPastEndException.class, () -> channel.read(buffer));
        }
    }
}
