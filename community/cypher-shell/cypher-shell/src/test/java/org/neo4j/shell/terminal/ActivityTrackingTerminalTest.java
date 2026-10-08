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
package org.neo4j.shell.terminal;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import org.jline.terminal.Attributes;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.junit.jupiter.api.Test;

/**
 * Activity must be observable on every character a reader consumes, independent of whether a jline
 * {@code MaskingCallback} is in play, on both dumb and non-dumb terminals: see JlineTerminal, which relies on
 * this to keep idle-timeout bookkeeping accurate without (ab)using {@code MaskingCallback} for it.
 */
class ActivityTrackingTerminalTest {
    @Test
    void firesOnceForEachCharacterRead() throws Exception {
        var listener = new ActivityListener();
        var activityCount = new AtomicInteger();
        listener.set(activityCount::incrementAndGet);

        try (var terminal = new ActivityTrackingTerminal(dumbTerminal("hello"), listener)) {
            var reader = terminal.reader();
            for (int i = 0; i < "hello".length(); i++) {
                assertThat(reader.read()).isEqualTo("hello".charAt(i));
            }
            assertThat(reader.read()).isEqualTo(-1); // EOF

            assertThat(activityCount.get()).isEqualTo("hello".length());
        }
    }

    @Test
    void doesNotFireOnPeek() throws Exception {
        var listener = new ActivityListener();
        var activityCount = new AtomicInteger();
        listener.set(activityCount::incrementAndGet);

        try (var terminal = new ActivityTrackingTerminal(dumbTerminal("x"), listener)) {
            var reader = terminal.reader();
            reader.peek(0L);
            reader.peek(0L);

            assertThat(activityCount.get()).isEqualTo(0);
        }
    }

    @Test
    void returnsTheSameReaderInstanceOnRepeatedCalls() throws Exception {
        var listener = new ActivityListener();
        try (var terminal = new ActivityTrackingTerminal(dumbTerminal(""), listener)) {
            assertThat(terminal.reader()).isSameAs(terminal.reader());
        }
    }

    private static Terminal dumbTerminal(String input) throws Exception {
        var in = new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8));
        var attributes = new Attributes();
        attributes.setLocalFlag(Attributes.LocalFlag.ECHO, false);
        return TerminalBuilder.builder()
                .streams(in, new ByteArrayOutputStream())
                .jna(false)
                .jansi(false)
                .jni(false)
                .dumb(true)
                .type(Terminal.TYPE_DUMB)
                .attributes(attributes)
                .build();
    }
}
