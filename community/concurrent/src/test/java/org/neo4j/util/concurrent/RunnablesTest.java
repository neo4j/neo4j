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
package org.neo4j.util.concurrent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.helpers.Exceptions;

class RunnablesTest {
    @Test
    void runAllMustRunAll() {
        // given
        Task task1 = new Task();
        Task task2 = new Task();
        Task task3 = new Task();

        // when
        Runnables.runAll("", task1, task2, task3);

        // then
        assertRun(task1, task2, task3);
    }

    @Test
    void runAllMustRunAllAndPropagateError() {
        // given
        Task task1 = new Task();
        Task task2 = new Task();
        Task task3 = new Task();
        Error expectedError = new Error("Killroy was here");
        Runnable throwingTask = error(expectedError);

        List<Runnable> runnables = Arrays.asList(task1, task2, task3, throwingTask);
        Collections.shuffle(runnables);

        String failureMessage = "Something wrong, Killroy must be here somewhere.";
        assertThatExceptionOfType(Error.class)
                .isThrownBy(() -> Runnables.runAll(failureMessage, runnables.toArray(new Runnable[0])))
                .isSameAs(expectedError)
                .withMessage(expectedError.getMessage())
                .satisfies(e -> assertThat(e.getSuppressed()).isEmpty());

        assertRun(task1, task2, task3);
    }

    @Test
    void runAllMustRunAllAndPropagateMultipleErrors() {
        // given
        Task task1 = new Task();
        Task task2 = new Task();
        Task task3 = new Task();
        Error expectedError = new Error("Killroy was here");
        Runnable throwingTask1 = error(expectedError);
        RuntimeException expectedException = new RuntimeException("and here");
        Runnable throwingTask2 = runtimeException(expectedException);

        List<Runnable> runnables = Arrays.asList(task1, task2, task3, throwingTask1, throwingTask2);
        Collections.shuffle(runnables);

        String failureMessage = "Something wrong, Killroy must be here somewhere.";
        assertThatExceptionOfType(RuntimeException.class)
                .isThrownBy(() -> Runnables.runAll(failureMessage, runnables.toArray(new Runnable[0])))
                .withMessage(failureMessage)
                .satisfies(e -> {
                    assertThat(Exceptions.findCauseOrSuppressed(e, t -> t == expectedError))
                            .isPresent();
                    assertThat(Exceptions.findCauseOrSuppressed(e, t -> t == expectedException))
                            .isPresent();
                });

        assertRun(task1, task2, task3);
    }

    @Test
    void runAllMustRunAllAndPropagateSingleErrorAsIs() {
        // given
        Task task1 = new Task();
        Task task2 = new Task();
        Task task3 = new Task();
        RuntimeException expectedError = new RuntimeException("Killroy was here");
        Runnable throwingTask1 = runtimeException(expectedError);

        List<Runnable> runnables = Arrays.asList(task1, throwingTask1, task2, task3);
        Collections.shuffle(runnables);

        assertThatExceptionOfType(RuntimeException.class)
                .isThrownBy(() -> Runnables.runAll("something", runnables.toArray(new Runnable[0])))
                .isSameAs(expectedError);

        assertRun(task1, task2, task3);
    }

    private static Runnable error(Error error) {
        return () -> {
            throw error;
        };
    }

    private static Runnable runtimeException(RuntimeException runtimeException) {
        return () -> {
            throw runtimeException;
        };
    }

    private static void assertRun(Task... tasks) {
        for (Task task : tasks) {
            assertThat(task.run).as("didn't run all expected tasks").isTrue();
        }
    }

    private static class Task implements Runnable {
        private boolean run;

        @Override
        public void run() {
            run = true;
        }
    }
}
