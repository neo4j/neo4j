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
package org.neo4j.kernel.api.index;

import static org.neo4j.internal.schema.IndexPrototype.forSchema;
import static org.neo4j.internal.schema.SchemaDescriptors.forLabel;
import static org.neo4j.logging.LogAssertions.assertThat;

import org.junit.jupiter.api.Test;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.logging.AssertableLogProvider;

class LoggingMonitorTest {
    @Test
    void shouldNotIncludeStackTraceWhenNotDebugLevel() {
        AssertableLogProvider logProvider = new AssertableLogProvider(false);
        LoggingMonitor monitor = new LoggingMonitor(logProvider.getLog(LoggingMonitorTest.class));
        IndexDescriptor index = forSchema(forLabel(1, 1)).withName("index").materialise(1);
        monitor.failedToOpenIndex(index, "I'll do something about this.", new Exception("Dammit Leroy!"));
        assertThat(logProvider).doesNotContainMessage("java.lang.Exception: Dammit Leroy!");
    }

    @Test
    void shouldIncludeStackTraceWhenDebugLevel() {
        AssertableLogProvider logProvider = new AssertableLogProvider(true);
        LoggingMonitor monitor = new LoggingMonitor(logProvider.getLog(LoggingMonitorTest.class));
        IndexDescriptor index = forSchema(forLabel(1, 1)).withName("index").materialise(1);
        monitor.failedToOpenIndex(index, "I'll do something about this.", new Exception("Dammit Leroy!"));
        assertThat(logProvider).containsMessages("java.lang.Exception: Dammit Leroy!");
    }
}
