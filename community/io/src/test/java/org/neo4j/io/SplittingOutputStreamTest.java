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
package org.neo4j.io;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.SequenceInputStream;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

class SplittingOutputStreamTest {

    private final OutputStream sink1 = Mockito.mock(OutputStream.class);
    private final OutputStream sink2 = Mockito.mock(OutputStream.class);
    private final InOrder calls = Mockito.inOrder(sink1, sink2);
    private final List<OutputStream> files = List.of(sink1, sink2);

    @Test
    void requirePositiveBlockSize() {
        assertThatThrownBy(() -> new SplittingOutputStream(Collections.emptyIterator(), 0, null))
                .hasMessage("blksize must be positive");
    }

    @Test
    void emptyWritePerformsNoInteractions() throws IOException {
        var os = new SplittingOutputStream(files, 1);
        os.write(new byte[0]);
        os.close();
        assertThatThrownBy(() -> os.write(new byte[0]))
                .isInstanceOf(IOException.class)
                .hasMessage("stream closed");

        verifyNoMoreInteractions();
    }

    @Test
    void emptyWriteShouldNotRunCloseAction() throws IOException {
        var onClose = Mockito.mock(Runnable.class);
        var os = new SplittingOutputStream(files.iterator(), 1, onClose);
        os.write(new byte[0]);
        os.close();

        verifyNoMoreInteractions();
        Mockito.verifyNoMoreInteractions(onClose);
    }

    @Test
    void emptyFlushPerformsNoInteractions() throws IOException {
        try (var os = new SplittingOutputStream(files, 1)) {
            os.flush();
        }
        verifyNoMoreInteractions();
    }

    @Test
    void splitsCorrectlyAtBlockSize1_singleByte() throws IOException {
        try (var os = new SplittingOutputStream(files, 1)) {
            os.write(1);
            os.write(2);
        }
        verify(sink1).write(1);
        verify(sink1).close();
        verify(sink2).write(2);
        verify(sink2).close();
        verifyNoMoreInteractions();
    }

    @Test
    void splitsCorrectlyAtBlockSize1_multiByte() throws IOException {
        byte[] b = {1, 2};
        try (var os = new SplittingOutputStream(files, 1)) {
            os.write(b);
        }

        verify(sink1).write(b, 0, 1);
        verify(sink1).close();
        verify(sink2).write(b, 1, 1);
        verify(sink2).close();
        verifyNoMoreInteractions();
    }

    @Test
    void splitsCorrectlyAtBlockSize1_multiByteWithOffset() throws IOException {
        byte[] b = {1, 2};
        try (var os = new SplittingOutputStream(files, 1)) {
            os.write(b, 1, 1);
            os.write(b, 0, 1);
        }
        verify(sink1).write(b, 1, 1);
        verify(sink1).close();
        verify(sink2).write(b, 0, 1);
        verify(sink2).close();
        verifyNoMoreInteractions();
    }

    @Test
    void splitsCorrectlyAtBlockSize10_singleByte() throws IOException {
        try (var os = new SplittingOutputStream(files, 10)) {
            os.write(0);
            os.write(1);
            os.write(2);
            os.write(3);
            os.write(4);
            os.write(5);
            os.write(6);
            os.write(7);
            os.write(8);
            os.write(9);
            os.write(10);
        }
        verify(sink1).write(0);
        verify(sink1).write(1);
        verify(sink1).write(2);
        verify(sink1).write(3);
        verify(sink1).write(4);
        verify(sink1).write(5);
        verify(sink1).write(6);
        verify(sink1).write(7);
        verify(sink1).write(8);
        verify(sink1).write(9);
        verify(sink1).close();

        verify(sink2).write(10);
        verify(sink2).close();
        verifyNoMoreInteractions();
    }

    @Test
    void splitsCorrectlyAtBlockSize10_multiByte() throws IOException {
        byte[] b = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        try (var os = new SplittingOutputStream(files, 10)) {
            os.write(b);
        }
        verify(sink1).write(b, 0, 10);
        verify(sink1).close();
        verify(sink2).write(b, 10, 1);
        verify(sink2).close();
        verifyNoMoreInteractions();
    }

    @Test
    void splitsCorrectlyAtBlockSize10_multiByteWithOffset() throws IOException {
        byte[] b = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        try (var os = new SplittingOutputStream(files, 10)) {
            os.write(b, 1, 10);
            os.write(b, 0, 1);
        }

        verify(sink1).write(b, 1, 10);
        verify(sink1).close();
        verify(sink2).write(b, 0, 1);
        verify(sink2).close();
        verifyNoMoreInteractions();
    }

    @Test
    void splitsCorrectlyAtBlockSize10WithSmallData() throws IOException {
        try (var os = new SplittingOutputStream(files, 10)) {
            os.write(0);
            os.write(1);
            os.write(2);
            os.write(3);
            os.write(4);
            os.write(5);
            os.write(6);
            os.write(7);
            os.write(8);
            os.write(9);
        }

        verify(sink1).write(0);
        verify(sink1).write(1);
        verify(sink1).write(2);
        verify(sink1).write(3);
        verify(sink1).write(4);
        verify(sink1).write(5);
        verify(sink1).write(6);
        verify(sink1).write(7);
        verify(sink1).write(8);
        verify(sink1).write(9);
        verify(sink1).close();
        verifyNoMoreInteractions();
    }

    @Test
    void splitsCorrectlyAtBlockSize10WithSmallData_multiByte() throws IOException {
        byte[] b = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        try (var os = new SplittingOutputStream(files, 10)) {
            os.write(b);
        }

        verify(sink1).write(b, 0, 10);
        verify(sink1).close();
        verifyNoMoreInteractions();
    }

    @Test
    void splitsCorrectlyAtBlockSize10WithSmallData_multiByteWithOffset() throws IOException {
        byte[] b = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        try (var os = new SplittingOutputStream(files, 10)) {
            os.write(b, 1, 9);
            os.write(b, 0, 1);
        }

        verify(sink1).write(b, 1, 9);
        verify(sink1).write(b, 0, 1);
        verify(sink1).close();
        verifyNoMoreInteractions();
    }

    @Test
    void canFlush() throws IOException {
        try (var os = new SplittingOutputStream(files, 10)) {
            os.write(0);
            os.flush();
            os.write(1);
        }
        verify(sink1).write(0);
        verify(sink1).flush();
        verify(sink1).write(1);
        verify(sink1).close();
        verifyNoMoreInteractions();
    }

    @Test
    void throwIfFlushAfterClose() throws IOException {
        var os = new SplittingOutputStream(files, 10);
        os.close();

        assertThatThrownBy(os::flush).isInstanceOf(IOException.class).hasMessage("stream closed");
        verifyNoMoreInteractions();
    }

    @Test
    void throwIfWriteAfterClose() throws IOException {
        var os = new SplittingOutputStream(files, 10);
        os.close();

        assertThatThrownBy(() -> os.write(0)).isInstanceOf(IOException.class).hasMessage("stream closed");
        verifyNoMoreInteractions();
    }

    @Test
    void throwIfNoMoreStreamsAvailable() throws IOException {
        try (var os = new SplittingOutputStream(files, 1)) {
            os.write(1);
            os.write(2);
            assertThatThrownBy(() -> os.write(3))
                    .isInstanceOf(IOException.class)
                    .hasMessage("no more output streams available");
        }
        verify(sink1).write(1);
        verify(sink1).close();
        verify(sink2).write(2);
        verify(sink2).close();
        verifyNoMoreInteractions();
    }

    @Test
    void canCloseAfterFailedWrite() throws IOException {
        Mockito.doThrow(new IOException("oops")).when(sink2).write(1);
        try (var os = new SplittingOutputStream(files, 1)) {
            os.write(0);
            assertThatThrownBy(() -> os.write(1)).hasMessage("oops");
        }
        verify(sink1).write(0);
        verify(sink1).close();
        verify(sink2).write(1);
        verify(sink2).close();
        verifyNoMoreInteractions();
    }

    @Test
    void canCloseMultipleTimes() throws IOException {
        var os = new SplittingOutputStream(files, 10);
        os.write(0);
        os.close();
        os.close();

        // Underlying stream gets closed once.
        verify(sink1).write(0);
        verify(sink1).close();
        verifyNoMoreInteractions();
    }

    @Test
    void callsCloseActionWhenClosing() throws IOException {
        var onClose = Mockito.mock(Runnable.class);
        try (var os = new SplittingOutputStream(files.iterator(), 10, onClose)) {
            os.write(0);
        }
        Mockito.verify(onClose).run();
    }

    @Test
    void callsCloseActionOnlyWhenClosingMultipleTimes() throws IOException {
        var onClose = Mockito.mock(Runnable.class);
        var os = new SplittingOutputStream(files.iterator(), 10, onClose);
        os.write(0);
        os.close();
        os.close();

        // Underlying stream gets closed once.
        verify(sink1).write(0);
        verify(sink1).close();
        verifyNoMoreInteractions();
        Mockito.verify(onClose).run();
        Mockito.verifyNoMoreInteractions(onClose);
    }

    @Test
    void propagateCloseFailure() throws IOException {
        Mockito.doThrow(new IOException("oops")).when(sink1).close();

        var os = new SplittingOutputStream(files, 1);
        os.write(0);
        assertThatThrownBy(os::close).isInstanceOf(IOException.class).hasMessage("oops");

        verify(sink1).write(0);
        verify(sink1).close();
        verifyNoMoreInteractions();
    }

    @Test
    void closeFailureWhenChangingStream() throws IOException {
        Mockito.doThrow(new IOException("oops")).when(sink1).close();
        try (var os = new SplittingOutputStream(files, 1)) {
            os.write(0);
            assertThatThrownBy(() -> os.write(1))
                    .isInstanceOf(IOException.class)
                    .hasMessage("oops"); // Swapping file
        }
        verify(sink1).write(0);
        verify(sink1).close();
        verifyNoMoreInteractions();
    }

    @Test
    void propagateFlushFailure() throws IOException {
        Mockito.doThrow(new IOException("oops")).when(sink1).flush();

        try (var os = new SplittingOutputStream(files, 1)) {
            os.write(0);
            assertThatThrownBy(os::flush).isInstanceOf(IOException.class).hasMessage("oops");
            verify(sink1).write(0);
            verify(sink1).flush();
            verifyNoMoreInteractions();
        }
    }

    @Test
    void canRestoreWritten() throws IOException {
        byte[] expected = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        var first = new ByteArrayOutputStream(10);
        var second = new ByteArrayOutputStream(10);

        var sinks = List.of(first, second).iterator();
        try (var os = new SplittingOutputStream(sinks, 10, null)) {
            os.write(expected);
        }

        byte[] actual = new byte[expected.length];
        try (var is = new SequenceInputStream(
                new ByteArrayInputStream(first.toByteArray()), new ByteArrayInputStream(second.toByteArray()))) {
            int read = is.read(actual);
            if (read < expected.length) {
                is.read(actual, read, expected.length - read);
            }
        }
        assertThat(actual).isEqualTo(expected);
    }

    private OutputStream verify(OutputStream sink) {
        // Perform ordered interaction checks.
        return calls.verify(sink);
    }

    private void verifyNoMoreInteractions() {
        Mockito.verifyNoMoreInteractions(sink1, sink2);
    }
}
