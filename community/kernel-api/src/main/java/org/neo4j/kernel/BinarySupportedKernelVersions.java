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
package org.neo4j.kernel;

import org.neo4j.configuration.Config;

/**
 * Class that keeps track of the latest kernel version the binaries support.
 * Kept in config to be able to override in tests.
 */
public final class BinarySupportedKernelVersions {

    private final KernelVersion latestKernelVersionSupportedByBinaries;

    public BinarySupportedKernelVersions(Config config) {
        this.latestKernelVersionSupportedByBinaries = KernelVersion.getLatestVersion(config);
    }

    public boolean latestSupportedIsAtLeast(KernelVersion version) {
        return latestKernelVersionSupportedByBinaries.isAtLeast(version);
    }

    public boolean latestSupportedIsLessThan(byte version) {
        return latestKernelVersionSupportedByBinaries.isLessThan(version);
    }

    @Override
    public String toString() {
        return latestKernelVersionSupportedByBinaries.toString();
    }
}
