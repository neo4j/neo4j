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

import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.DbmsRuntimeVersion;
import org.neo4j.kernel.BinarySupportedKernelVersions;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.impl.transaction.log.entry.LogFormat;

public final class LatestVersions {
    public static final KernelVersion LATEST_KERNEL_VERSION = KernelVersion.getLatestVersion(Config.defaults());
    public static final KernelVersionProvider LATEST_KERNEL_VERSION_PROVIDER = () -> LATEST_KERNEL_VERSION;
    public static final DbmsRuntimeVersion LATEST_RUNTIME_VERSION =
            DbmsRuntimeVersion.getLatestVersion(Config.defaults());
    public static final BinarySupportedKernelVersions BINARY_VERSIONS =
            new BinarySupportedKernelVersions(Config.defaults());
    public static final LogFormat LATEST_LOG_FORMAT = LogFormat.fromKernelVersion(LATEST_KERNEL_VERSION);

    private LatestVersions() {}
}
