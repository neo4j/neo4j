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
package org.neo4j.server.queryapi;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.io.InputStream;
import org.junit.jupiter.api.Test;
import org.neo4j.server.queryapi.request.PeekedFirstByteInputStream;

public class PeekedFirstByteInputStreamTest {

    @Test
    void shouldPeekFirstByte() throws IOException {
        var testStream = new PeekedFirstByteInputStream(new TestStream());

        assertThat(testStream.peek()).isEqualTo(3);

        // peeking should always return the same unless consumed
        assertThat(testStream.peek()).isEqualTo(3);
    }

    @Test
    void readingShouldMovePeekAhead() throws IOException {
        var testStream = new PeekedFirstByteInputStream(new TestStream());

        assertThat(testStream.peek()).isEqualTo(3);

        var readValue = testStream.read();
        assertThat(readValue).isEqualTo(3);

        assertThat(testStream.peek()).isEqualTo(6);
    }

    @Test
    void shouldCloseUnderlyingStreamOnClosure() throws IOException {
        var inputStream = mock(InputStream.class);
        var testStream = new PeekedFirstByteInputStream(inputStream);

        testStream.close();

        verify(inputStream).close();
    }

    private static class TestStream extends InputStream {

        private final int[] bytes = {3, 6, 9};
        private int index = 0;

        @Override
        public int read() throws IOException {
            var next = bytes[index];
            index++;
            return next;
        }
    }
}
