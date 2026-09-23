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
package org.neo4j.kernel.impl.newapi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.Neo4jLogMessage;
import org.neo4j.logging.log4j.Neo4jLogMarkers;

public class DefaultPooledCursorsTest extends DefaultPooledCursorsTestBase<ReadTestSupport> {
    @Override
    public ReadTestSupport newTestSupport() {
        return new ReadTestSupport();
    }

    @Test
    void shouldLogKernelMarkedWarningWhenLeakLoggingEnabled() {
        InternalLog log = mock(InternalLog.class);

        DefaultPooledCursors.reportLeakedCursors(false, true, log, "node: 1");

        ArgumentCaptor<Neo4jLogMessage> message = ArgumentCaptor.forClass(Neo4jLogMessage.class);
        verify(log).warn(message.capture());
        assertThat(message.getValue().getMarker()).isEqualTo(Neo4jLogMarkers.KERNEL);
        assertThat(message.getValue().getFormattedMessage()).contains("node: 1").contains("track_cursor_close");
    }

    @Test
    void shouldStaySilentWhenLeakLoggingDisabled() {
        InternalLog log = mock(InternalLog.class);

        assertThatCode(() -> DefaultPooledCursors.reportLeakedCursors(false, false, log, "node: 1"))
                .doesNotThrowAnyException();
        verifyNoInteractions(log);
    }

    @Test
    void shouldThrowWhenLeakFailureEnabled() {
        InternalLog log = mock(InternalLog.class);

        assertThatThrownBy(() -> DefaultPooledCursors.reportLeakedCursors(true, false, log, "node: 1"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("node: 1")
                .hasMessageContaining("track_cursor_close");
        verifyNoInteractions(log);
    }
}
