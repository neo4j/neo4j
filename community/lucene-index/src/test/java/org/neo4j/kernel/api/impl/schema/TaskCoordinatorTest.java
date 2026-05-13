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
package org.neo4j.kernel.api.impl.schema;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.kernel.api.impl.schema.TaskCoordinator.Task;
import org.neo4j.test.Barrier;

class TaskCoordinatorTest {
    private ByteArrayOutputStream out;
    private PrintStream err;

    @BeforeEach
    void setup() {
        out = new ByteArrayOutputStream();
        err = new PrintStream(out);
    }

    @AfterEach
    void checkStream() {
        err.flush();
        assertThat(out.toString()).isEmpty();
    }

    @Test
    void shouldCancelAllTasksWithOneCall() {
        // given
        TaskCoordinator coordinator = new TaskCoordinator();

        try (Task task1 = coordinator.newTask();
                Task task2 = coordinator.newTask();
                Task task3 = coordinator.newTask()) {
            assertThat(task1.cancellationRequested()).isFalse();
            assertThat(task2.cancellationRequested()).isFalse();
            assertThat(task3.cancellationRequested()).isFalse();

            // when
            coordinator.cancel();

            // then
            assertThat(task1.cancellationRequested()).isTrue();
            assertThat(task2.cancellationRequested()).isTrue();
            assertThat(task3.cancellationRequested()).isTrue();
        }
    }

    @Test
    void shouldAwaitCompletionOfAllTasks() throws Exception {
        // given
        TaskCoordinator coordinator = new TaskCoordinator();
        AtomicReference<String> state = new AtomicReference<>();
        List<String> states = new ArrayList<>();
        Barrier.Control phaseA = new Barrier.Control();
        Barrier.Control phaseB = new Barrier.Control();
        Barrier.Control phaseC = new Barrier.Control();

        state.set("A");
        new Thread("awaitCompletion") {
            @Override
            public void run() {
                try {
                    states.add(state.get()); // expects A
                    phaseA.reached();
                    states.add(state.get()); // expects B
                    phaseB.await();
                    phaseB.release();
                    coordinator.awaitCompletion();
                    states.add(state.get()); // expects C
                    phaseC.reached();
                } catch (InterruptedException e) {
                    e.printStackTrace(err);
                }
            }
        }.start();

        // when
        try (Task task1 = coordinator.newTask();
                Task task2 = coordinator.newTask()) {
            phaseA.await();
            state.set("B");
            phaseA.release();
            phaseC.release();
            phaseB.reached();
            state.set("C");
        }
        phaseC.await();

        // then
        assertThat(states).contains("A", "B", "C");
    }
}
