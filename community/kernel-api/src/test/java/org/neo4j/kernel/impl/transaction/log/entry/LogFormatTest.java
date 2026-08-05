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
package org.neo4j.kernel.impl.transaction.log.entry;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.kernel.KernelVersion;

class LogFormatTest {
    private static final KernelVersion LATEST = KernelVersion.getLatestVersion(Config.defaults());

    @Test
    void latestKernelVersionUsesV9WhenNoMergedLogAndNewLogFormatNotAllowed() {
        assertThat(LogFormat.fromConfigAndKernelVersion(Config.defaults(), LATEST))
                .isEqualTo(LogFormat.V9);
    }

    @Test
    void latestKernelVersionUsesV10WhenNoMergedLogAndNewLogFormatAllowed() {
        Config config = Config.newBuilder()
                .set(GraphDatabaseInternalSettings.allow_new_log_format_on_upgrade_or_create, true)
                .build();

        assertThat(LogFormat.fromConfigAndKernelVersion(config, LATEST)).isEqualTo(LogFormat.V10);
    }

    @Test
    void gloriousFutureUsesV11RegardlessOfMergedLogWhenNewLogFormatAllowed() {
        Config config = Config.newBuilder()
                .set(GraphDatabaseInternalSettings.allow_new_log_format_on_upgrade_or_create, true)
                .build();

        assertThat(LogFormat.fromConfigAndKernelVersion(config, KernelVersion.GLORIOUS_FUTURE))
                .isEqualTo(LogFormat.V11);
    }

    @Test
    void mergedLogForcesV11() {
        // Enabling merged_log without merge_log_on_latest requires GLORIOUS_FUTURE Kernel and Runtime versions
        Config config = Config.newBuilder()
                .set(GraphDatabaseInternalSettings.latest_kernel_version, KernelVersion.GLORIOUS_FUTURE.version())
                .set(GraphDatabaseInternalSettings.latest_runtime_version, Integer.MAX_VALUE)
                .set(GraphDatabaseInternalSettings.merged_log, true)
                .build();

        assertThat(LogFormat.fromConfigAndKernelVersion(config, KernelVersion.GLORIOUS_FUTURE))
                .isEqualTo(LogFormat.V11);
    }

    @Test
    void mergeLogOnLatestForcesV11WhenLatestKernelVersion() {
        Config config = Config.newBuilder()
                .set(GraphDatabaseInternalSettings.merge_log_on_latest, true)
                .build();

        assertThat(LogFormat.fromConfigAndKernelVersion(config, LATEST)).isEqualTo(LogFormat.V11);
    }
}
