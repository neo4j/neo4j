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
package org.neo4j.kernel.database;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.DbmsRuntimeVersionProvider;
import org.neo4j.dbms.database.DbmsRuntimeVersion;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.KernelVersionProvider;

public class DbmsRuntimeFallbackKernelVersionProvider implements KernelVersionProvider {
    private final KernelVersionProvider kernelVersionProvider;

    public DbmsRuntimeFallbackKernelVersionProvider(Dependencies dependencies, String databaseName, Config config) {
        // Use the kernel version provider if we have one.
        // If for some reason we don't have access to a kernel version provider, we have to
        // fall back to something else. This provider tries to use the version from the system database.
        // Note that this should only be done if we don't have any transaction logs!
        // If logs are present a MetadataCache based on the transaction logs should be used.
        //
        // For system database we can't use ourselves because we might not be completely operational yet,
        // so default to the latest version.
        if (dependencies.containsDependency(KernelVersionProvider.class)) {
            this.kernelVersionProvider = dependencies.resolveDependency(KernelVersionProvider.class);
        } else if (SYSTEM_DATABASE_NAME.equals(databaseName)
                || !dependencies.containsDependency(DbmsRuntimeVersionProvider.class)) {
            this.kernelVersionProvider = DbmsRuntimeVersion.getLatestVersion(config);
        } else {
            DbmsRuntimeVersionProvider dbmsRuntimeVersionProvider =
                    dependencies.resolveDependency(DbmsRuntimeVersionProvider.class);
            this.kernelVersionProvider =
                    () -> dbmsRuntimeVersionProvider.getVersion().kernelVersion();
        }
    }

    @Override
    public KernelVersion kernelVersion() {
        return kernelVersionProvider.kernelVersion();
    }
}
