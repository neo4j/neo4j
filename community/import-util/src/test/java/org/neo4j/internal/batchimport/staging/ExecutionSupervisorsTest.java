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
package org.neo4j.internal.batchimport.staging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ExecutionSupervisorsTest {
    @Test
    void shouldStartCheckAndEndMonitor() throws InterruptedException {
        // given
        var started = new AtomicBoolean();
        var ended = new AtomicBoolean();
        var checked = new AtomicInteger();
        var monitor = new ExecutionMonitor.Adapter(1, TimeUnit.MILLISECONDS) {
            @Override
            public void start(StageExecution execution) {
                started.set(true);
            }

            @Override
            public void end(StageExecution execution, long totalTimeMillis) {
                ended.set(true);
            }

            @Override
            public void check(StageExecution execution) {
                checked.incrementAndGet();
            }
        };
        var stage = mock(Stage.class);
        var execution = mock(StageExecution.class);
        when(stage.execute()).thenReturn(execution);
        var checks = 10;
        var completionCounter = new AtomicInteger(checks);
        when(execution.awaitCompletion(anyLong(), any()))
                .thenAnswer(invocation -> completionCounter.getAndDecrement() == 0);

        // when
        ExecutionSupervisors.superviseExecution(monitor, stage);

        // then
        assertThat(started).isTrue();
        assertThat(ended).isTrue();
        assertThat(checked.get()).isEqualTo(checks);
    }
}
