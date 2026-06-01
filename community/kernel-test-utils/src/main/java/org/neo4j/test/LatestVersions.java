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
package org.neo4j.test;

import static org.neo4j.kernel.KernelVersion.GLORIOUS_FUTURE;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.dbms.database.DbmsRuntimeVersion;
import org.neo4j.kernel.BinarySupportedKernelVersions;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.KernelVersionProviders;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexVersion;
import org.neo4j.kernel.impl.transaction.log.LogFormatVersionProvider;
import org.neo4j.kernel.impl.transaction.log.entry.LogFormat;

public final class LatestVersions {
    private LatestVersions() {}

    public static final KernelVersion LATEST_KERNEL_VERSION = KernelVersion.getLatestVersion(Config.defaults());
    public static final KernelVersionProvider LATEST_KERNEL_VERSION_PROVIDER =
            KernelVersionProviders.fixed(LATEST_KERNEL_VERSION);
    public static final DbmsRuntimeVersion LATEST_RUNTIME_VERSION =
            DbmsRuntimeVersion.getLatestVersion(Config.defaults());
    public static final BinarySupportedKernelVersions BINARY_VERSIONS =
            new BinarySupportedKernelVersions(Config.defaults());
    public static final LogFormat LATEST_LOG_FORMAT =
            LogFormat.fromConfigAndKernelVersion(Config.defaults(), LATEST_KERNEL_VERSION);
    public static final LogFormatVersionProvider LATEST_LOG_FORMAT_PROVIDER = () -> LATEST_LOG_FORMAT;

    // TODO MERGELOG: remember to update version
    // NOTE this is only correct if allow_new_log_format_on_upgrade_or_create is false
    public static final KernelVersion LATEST_KERNEL_VERSION_WITHOUT_ENVELOPES =
            LogFormat.getLastVersionPreEnvelopeFormat();
    public static final DbmsRuntimeVersion LATEST_RUNTIME_VERSION_WITHOUT_ENVELOPES =
            findDbmsVersionMatchingKernelVersion(LATEST_KERNEL_VERSION_WITHOUT_ENVELOPES);

    public static final BinarySupportedKernelVersions FUTURE_BINARY_VERSIONS = new BinarySupportedKernelVersions(
            Config.defaults(GraphDatabaseInternalSettings.latest_kernel_version, GLORIOUS_FUTURE.version()));

    private static DbmsRuntimeVersion findDbmsVersionMatchingKernelVersion(KernelVersion version) {
        for (DbmsRuntimeVersion dbmsRuntimeVersion : DbmsRuntimeVersion.VERSIONS) {
            if (dbmsRuntimeVersion.kernelVersion() == version) {
                return dbmsRuntimeVersion;
            }
        }
        throw new IllegalArgumentException("No matching Dbms version found for " + version.toString());
    }

    public static final VectorIndexVersion LATEST_VECTOR_INDEX_VERSION =
            VectorIndexVersion.latestSupportedVersion(LATEST_KERNEL_VERSION);
}
