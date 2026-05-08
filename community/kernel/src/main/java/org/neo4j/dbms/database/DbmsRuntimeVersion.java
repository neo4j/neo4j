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
import org.neo4j.dbms.database.SystemGraphComponent.Name;
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
    V5_19(17, DBMS_RUNTIME_COMPONENT, Neo4jVersions.VERSION_519, KernelVersion.V5_19),
    V5_20(18, DBMS_RUNTIME_COMPONENT, Neo4jVersions.VERSION_520, KernelVersion.V5_20),
    V5_22(19, DBMS_RUNTIME_COMPONENT, Neo4jVersions.VERSION_522, KernelVersion.V5_22),
    V5_23(20, DBMS_RUNTIME_COMPONENT, Neo4jVersions.VERSION_523, KernelVersion.V5_23),
    V5_25(21, DBMS_RUNTIME_COMPONENT, Neo4jVersions.VERSION_525, KernelVersion.V5_25),
    V2025_04(22, DBMS_RUNTIME_COMPONENT, Neo4jVersions.VERSION_202504, KernelVersion.V2025_04),
    V2025_05(23, DBMS_RUNTIME_COMPONENT, Neo4jVersions.VERSION_202505, KernelVersion.V2025_05),
    V2025_07(24, DBMS_RUNTIME_COMPONENT, Neo4jVersions.VERSION_202507, KernelVersion.V2025_07),
    V2025_08(25, DBMS_RUNTIME_COMPONENT, Neo4jVersions.VERSION_202508, KernelVersion.V2025_08),
    V2025_09(26, DBMS_RUNTIME_COMPONENT, Neo4jVersions.VERSION_202509, KernelVersion.V2025_09),
    V2025_10(27, DBMS_RUNTIME_COMPONENT, Neo4jVersions.VERSION_202510, KernelVersion.V2025_10),
    V2025_11(28, DBMS_RUNTIME_COMPONENT, Neo4jVersions.VERSION_202511, KernelVersion.V2025_11),
    V2026_01(29, DBMS_RUNTIME_COMPONENT, Neo4jVersions.VERSION_202601, KernelVersion.V2026_01),
    V2026_02(30, DBMS_RUNTIME_COMPONENT, Neo4jVersions.VERSION_202602, KernelVersion.V2026_02),

    /**
     * Glorious future version to be used for testing coming versions.
     */
    GLORIOUS_FUTURE(Integer.MAX_VALUE, DBMS_RUNTIME_COMPONENT, "Future version", KernelVersion.GLORIOUS_FUTURE);

    public static final List<DbmsRuntimeVersion> VERSIONS = List.of(values());

    // The latest version should be kept private to be able to override it from tests.
    // getLatestVersion should be used when the latest version is required.
    // Select the second last element (the last being GLORIOUS_FUTURE).
    private static final DbmsRuntimeVersion LATEST_DBMS_RUNTIME_COMPONENT_VERSION = VERSIONS.get(VERSIONS.size() - 2);

    public static DbmsRuntimeVersion getLatestVersion(Config config) {
        Integer version = config.get(GraphDatabaseInternalSettings.latest_runtime_version);
        return version != null ? DbmsRuntimeVersion.fromVersionNumber(version) : LATEST_DBMS_RUNTIME_COMPONENT_VERSION;
    }

    DbmsRuntimeVersion(int version, Name componentName, String description, KernelVersion kernelVersion) {
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

    public static DbmsRuntimeVersion fromKernelVersion(KernelVersion kernelVersion) {
        for (DbmsRuntimeVersion componentVersion : VERSIONS) {
            if (componentVersion.kernelVersion == kernelVersion) {
                return componentVersion;
            }
        }
        throw new IllegalArgumentException("Unrecognised DBMS runtime version for: " + kernelVersion);
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
