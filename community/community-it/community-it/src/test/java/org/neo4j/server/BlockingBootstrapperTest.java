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
package org.neo4j.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.conditions.Conditions.FALSE;
import static org.neo4j.test.conditions.Conditions.TRUE;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.utils.TestDirectory;

@Neo4jLayoutExtension
class BlockingBootstrapperTest {
    @Inject
    private TestDirectory homeDir;

    @Test
    void shouldBlockUntilStoppedIfTheWrappedStartIsSuccessful() {
        AtomicInteger status = new AtomicInteger();
        AtomicBoolean exited = new AtomicBoolean(false);
        AtomicBoolean running = new AtomicBoolean(false);

        BlockingBootstrapper bootstrapper = new BlockingBootstrapper(new Bootstrapper() {
            @Override
            public int start(
                    Path homeDir,
                    Path configFile,
                    Map<String, String> configOverrides,
                    boolean expandCommands,
                    boolean daemonMode) {
                running.set(true);
                return 0;
            }

            @Override
            public int stop() {
                running.set(false);
                return 0;
            }
        });

        new Thread(() -> {
                    status.set(bootstrapper.start(homeDir.directory("home-dir"), Collections.emptyMap()));
                    exited.set(true);
                })
                .start();

        assertEventually("Wrapped was not started", running::get, TRUE, 10, TimeUnit.SECONDS);
        assertThat(exited.get()).as("Bootstrapper exited early").isEqualTo(false);

        bootstrapper.stop();

        assertEventually("Wrapped was not stopped", running::get, FALSE, 10, TimeUnit.SECONDS);
        assertEventually("Bootstrapper did not exit", exited::get, TRUE, 10, TimeUnit.SECONDS);
        assertThat(status.get())
                .as("Bootstrapper did not propagate exit status")
                .isEqualTo(0);
    }

    @Test
    void shouldNotBlockIfTheWrappedStartIsUnsuccessful() {
        AtomicInteger status = new AtomicInteger();
        AtomicBoolean exited = new AtomicBoolean(false);

        BlockingBootstrapper bootstrapper = new BlockingBootstrapper(new Bootstrapper() {
            @Override
            public int start(
                    Path homeDir,
                    Path configFile,
                    Map<String, String> configOverrides,
                    boolean expandCommands,
                    boolean daemonMode) {
                return 1;
            }

            @Override
            public int stop() {
                return 0;
            }
        });

        new Thread(() -> {
                    status.set(bootstrapper.start(
                            homeDir.directory("home-dir"), null, Collections.emptyMap(), false, false));
                    exited.set(true);
                })
                .start();

        assertEventually("Blocked unexpectedly", exited::get, TRUE, 10, TimeUnit.SECONDS);
        assertThat(status.get())
                .as("Bootstrapper did not propagate exit status")
                .isEqualTo(1);
    }
}
