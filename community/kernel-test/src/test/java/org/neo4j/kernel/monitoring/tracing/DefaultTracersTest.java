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
package org.neo4j.kernel.monitoring.tracing;

import static org.mockito.Mockito.mock;
import static org.neo4j.logging.AssertableLogProvider.Level.WARN;
import static org.neo4j.logging.LogAssertions.assertThat;

import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.impl.api.tracer.DefaultTracer;
import org.neo4j.kernel.impl.transaction.tracing.DatabaseTracer;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.InternalLog;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.time.Clocks;
import org.neo4j.time.SystemNanoClock;

class DefaultTracersTest {
    private final AssertableLogProvider logProvider = new AssertableLogProvider();
    private final JobScheduler jobScheduler = mock(JobScheduler.class);
    private final SystemNanoClock clock = Clocks.nanoClock();
    private final Monitors monitors = new Monitors();

    private InternalLog log;

    @BeforeEach
    void setUp() {
        log = logProvider.getLog(getClass());
    }

    @Test
    void mustProduceNullImplementationsWhenRequested() {
        DefaultTracers tracers = createTracers("null");
        var namedDatabaseId = DatabaseIdFactory.from("foo", UUID.randomUUID());
        assertThat(tracers.getPageCacheTracer()).isEqualTo(PageCacheTracer.NULL);
        assertThat(tracers.getDatabaseTracer(namedDatabaseId)).isEqualTo(DatabaseTracer.NULL.NULL);
        assertNoWarning();
    }

    @Test
    void mustProduceDefaultImplementationForNullConfiguration() {
        DefaultTracers tracers = createTracers(null);
        assertDefaultImplementation(tracers);
        assertNoWarning();
    }

    @Test
    void mustProduceDefaultImplementationWhenRequested() {
        DefaultTracers tracers = createTracers("default");
        assertDefaultImplementation(tracers);
        assertNoWarning();
    }

    @Test
    void mustProduceDefaultImplementationWhenRequestingUnknownImplementation() {
        DefaultTracers tracers = createTracers("there's nothing like this");
        assertDefaultImplementation(tracers);
        assertWarning("there's nothing like this");
    }

    private DefaultTracers createTracers(String s) {
        return new DefaultTracers(s, log, monitors, jobScheduler, clock, Config.defaults());
    }

    private static void assertDefaultImplementation(DefaultTracers tracers) {
        var namedDatabaseId = DatabaseIdFactory.from("bar", UUID.randomUUID());
        assertThat(tracers.getPageCacheTracer()).isInstanceOf(DefaultPageCacheTracer.class);
        assertThat(tracers.getDatabaseTracer(namedDatabaseId)).isInstanceOf(DefaultTracer.class);
    }

    private void assertNoWarning() {
        assertThat(logProvider).doesNotHaveAnyLogs();
    }

    private void assertWarning(String tracerName) {
        assertThat(logProvider)
                .forClass(getClass())
                .forLevel(WARN)
                .containsMessageWithArguments("Using default tracer implementations instead of '%s'", tracerName);
    }
}
