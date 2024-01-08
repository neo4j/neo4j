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
package org.neo4j.dbms.database;

import java.util.List;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.KernelVersionProvider;

public enum DbmsRuntimeVersion implements ComponentVersion, KernelVersionProvider {
    /**
     * Introduced new transaction log version
     */
    V4_2(2, DBMS_RUNTIME_COMPONENT, Neo4jVersions.VERSION_42, KernelVersion.V4_2),

    /**
     * Switch to use the Version node
     */
    V4_3(3, DBMS_RUNTIME_COMPONENT, Neo4jVersions.VERSION_43D2, KernelVersion.V4_2),

    /**
     * Dense node locking changes, token indexes and relationship property indexes.
     */
    V4_3_D4(4, DBMS_RUNTIME_COMPONENT, Neo4jVersions.VERSION_43D4, KernelVersion.V4_3_D4),

    /**
     * Range, Point and Text index types.
     */
    V4_4(5, DBMS_RUNTIME_COMPONENT, Neo4jVersions.VERSION_44, KernelVersion.V4_4),

    /**
     * Introduced new transaction log version
     */
    V5_0(6, DBMS_RUNTIME_COMPONENT, Neo4jVersions.VERSION_50, KernelVersion.V5_0),

    /**
     * Relationship uniqueness constraints
     */
    V5_7(7, DBMS_RUNTIME_COMPONENT, Neo4jVersions.VERSION_57, KernelVersion.V5_7),

    /**
     * Index usage statistics and enrichment command
     */
    V5_8(8, DBMS_RUNTIME_COMPONENT, Neo4jVersions.VERSION_58, KernelVersion.V5_8),

    /**
     * Property type constraints for single scalar types.
     */
    V5_9(9, DBMS_RUNTIME_COMPONENT, Neo4jVersions.VERSION_59, KernelVersion.V5_9),
    V5_10(10, DBMS_RUNTIME_COMPONENT, Neo4jVersions.VERSION_510, KernelVersion.V5_10),

    /**
     * Specific add/delete commands for entities and properties
     */
    V5_11(11, DBMS_RUNTIME_COMPONENT, Neo4jVersions.VERSION_511, KernelVersion.V5_11),

    /**
     * User metadata being tracked for CDC
     */
    V5_12(12, DBMS_RUNTIME_COMPONENT, Neo4jVersions.VERSION_512, KernelVersion.V5_12),

    V5_13(13, DBMS_RUNTIME_COMPONENT, Neo4jVersions.VERSION_513, KernelVersion.V5_13),

    V5_14(14, DBMS_RUNTIME_COMPONENT, Neo4jVersions.VERSION_514, KernelVersion.V5_14),
    V5_15(15, DBMS_RUNTIME_COMPONENT, Neo4jVersions.VERSION_515, KernelVersion.V5_15),
    V5_18(16, DBMS_RUNTIME_COMPONENT, Neo4jVersions.VERSION_518, KernelVersion.V5_18),

    /**
     * Glorious future version to be used for testing coming versions.
     */
    GLORIOUS_FUTURE(Integer.MAX_VALUE, DBMS_RUNTIME_COMPONENT, "Future version", KernelVersion.GLORIOUS_FUTURE);

    public static final List<DbmsRuntimeVersion> VERSIONS = List.of(values());

    // The latest version should be kept private to be able to override it from tests.
    // getLatestVersion should be used when the latest version is required.
    private static final DbmsRuntimeVersion LATEST_DBMS_RUNTIME_COMPONENT_VERSION = V5_18;

    public static DbmsRuntimeVersion getLatestVersion(Config config) {
        Integer version = config.get(GraphDatabaseInternalSettings.latest_runtime_version);
        return version != null ? DbmsRuntimeVersion.fromVersionNumber(version) : LATEST_DBMS_RUNTIME_COMPONENT_VERSION;
    }

    DbmsRuntimeVersion(
            int version, SystemGraphComponent.Name componentName, String description, KernelVersion kernelVersion) {
        this.version = version;
        this.componentName = componentName;
        this.description = description;
        this.kernelVersion = kernelVersion;
    }

    private final SystemGraphComponent.Name componentName;
    private final String description;
    private final KernelVersion kernelVersion;
    private final int version;

    @Override
    public int getVersion() {
        return version;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public SystemGraphComponent.Name getComponentName() {
        return componentName;
    }

    @Override
    public boolean isCurrent(Config config) {
        return version == getLatestVersion(config).version;
    }

    @Override
    public boolean migrationSupported() {
        return true;
    }

    @Override
    public boolean runtimeSupported() {
        return true;
    }

    public static DbmsRuntimeVersion fromVersionNumber(int versionNumber) {
        for (DbmsRuntimeVersion componentVersion : VERSIONS) {
            if (componentVersion.version == versionNumber) {
                return componentVersion;
            }
        }
        throw new IllegalArgumentException("Unrecognised DBMS runtime version number: " + versionNumber);
    }

    @Override
    public boolean isGreaterThan(ComponentVersion other) {
        if (!(other instanceof DbmsRuntimeVersion)) {
            throw new IllegalArgumentException("Comparison to different Version type");
        }
        return this.getVersion() > other.getVersion();
    }

    @Override
    public KernelVersion kernelVersion() {
        return kernelVersion;
    }
}
