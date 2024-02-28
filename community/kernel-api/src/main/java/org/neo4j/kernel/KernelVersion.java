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

import java.util.List;
import org.eclipse.collections.api.map.primitive.ImmutableByteObjectMap;
import org.eclipse.collections.impl.factory.primitive.ByteObjectMaps;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;

/**
 * One version scheme to unify various internal versions into one with the intent of conceptual simplification and simplification of version bumping.
 * The existing version byte codes originally comes from legacy versioning of the log entry versions. This kernel version now controls that said version
 * as well as the explicitly set version which a database is set to run with.
 * <br>
 * On a high level there's a DBMS runtime version which granularity is finer and is therefore a super set of the version set in here, which only
 * contains versions that has some sort of format change. This kernel version codes doesn't follow the same codes as the DBMS runtime version codes
 * and kernel will have a translation between the two.
 */
public enum KernelVersion {
    // Version V2_3 and V4_0 only here to keep support for old checkpoint records parsing.
    // We do not support reading or writing anything else in those legacy formats.
    V2_3((byte) -10), // 2.3 to 3.5.
    V4_0((byte) 1), // 4.0 to 4.1. Added checksums to the log files.

    V4_2((byte) 2), // 4.2+. Removed checkpoint entries.
    // 4.3(some drop)+. Not a change to log entry format, but record storage engine log format change. Since record
    // storage commands
    // has no command version of their own it relies on a bump of the parser set version to distinguish between versions
    // unfortunately.
    // Also introduces token index and relationship property index features.
    V4_3_D4((byte) 3),
    V4_4((byte) 4), // 4.4. Introduces RANGE, POINT and TEXT index types.
    V5_0((byte) 5), // 5.0.
    V5_7((byte) 6), // 5.7. Introduces chunked transactions and relationship uniqueness/key constraints.
    V5_8((byte) 7), // 5.8. Introduces index usage statistics and enrichment command.
    V5_9((byte) 8), // 5.9. Introduces type constraints for single scalar types.
    V5_10((byte) 9), // 5.10. Introduces block format and type constraints for unions and lists.
    V5_11((byte) 10), // 5.11. Introduces specific add/remove commands, and the VECTOR index type.
    V5_12((byte) 11), // 5.12. Introduces user metadata for CDC
    V5_13((byte) 12), // 5.13.
    V5_14((byte) 13), // 5.14.
    V5_15((byte) 14), // 5.15. Changes around CDC logical keys
    V5_18((byte) 15), // 5.18. Introduce vector-2.0 index provider

    // An unreleased future version.
    // This version is meant to be used when developing a new feature
    // and it is not sure which version the feature will land in.
    GLORIOUS_FUTURE(Byte.MAX_VALUE);

    public static final KernelVersion EARLIEST = V4_2;
    // The latest version should be kept private to be able to override it from tests.
    // getLatestVersion should be used when the latest version is required.
    private static final KernelVersion LATEST = V5_18;
    public static final KernelVersion VERSION_IN_WHICH_TOKEN_INDEXES_ARE_INTRODUCED = V4_3_D4;
    public static final KernelVersion VERSION_RANGE_POINT_TEXT_INDEXES_ARE_INTRODUCED = V4_4;
    public static final KernelVersion VERSION_LITTLE_ENDIAN_TX_LOG_INTRODUCED = V5_0;
    public static final KernelVersion VERSION_TRIGRAM_INDEX_INTRODUCED = V5_0;
    public static final KernelVersion VERSION_REL_UNIQUE_CONSTRAINTS_INTRODUCED = V5_7;
    public static final KernelVersion VERSION_INDEX_USAGE_STATISTICS_INTRODUCED = V5_8;
    public static final KernelVersion VERSION_CDC_INTRODUCED = V5_8;
    public static final KernelVersion VERSION_TYPE_CONSTRAINTS_INTRODUCED = V5_9;
    public static final KernelVersion VERSION_BLOCKFORMAT_INTRODUCED = V5_10;
    public static final KernelVersion VERSION_UNIONS_AND_LIST_TYPE_CONSTRAINTS_INTRODUCED = V5_10;
    public static final KernelVersion VERSION_NODE_VECTOR_INDEX_INTRODUCED = V5_11;
    public static final KernelVersion VERSION_CDC_USER_METADATA_INTRODUCED = V5_12;
    public static final KernelVersion VERSION_CDC_LOGICAL_KEY_CHANGES = V5_15;
    public static final KernelVersion VERSION_ENVELOPED_TRANSACTION_LOGS_INTRODUCED = GLORIOUS_FUTURE;
    public static final KernelVersion VERSION_VECTOR_2_INTRODUCED = V5_18;

    // Keep updated each time there is an new schema rule added
    // related to IntegrityValidator
    public static final KernelVersion LATEST_SCHEMA_CHANGE = VERSION_VECTOR_2_INTRODUCED;

    // All neo4j 5.0-5.6 members defaulted to this version when bootstrapping a rafted database
    public static final KernelVersion DEFAULT_BOOTSTRAP_VERSION = V5_0;

    public static final List<KernelVersion> VERSIONS = List.of(values());
    private static final ImmutableByteObjectMap<KernelVersion> versionMap =
            ByteObjectMaps.immutable.from(VERSIONS, KernelVersion::version, v -> v);

    private final byte version;

    public static KernelVersion getLatestVersion(Config config) {
        Byte version = config.get(GraphDatabaseInternalSettings.latest_kernel_version);
        return version == null ? LATEST : KernelVersion.getForVersion(version);
    }

    KernelVersion(byte version) {
        this.version = version;
    }

    public byte version() {
        return this.version;
    }

    public boolean isLatest(Config config) {
        return this == getLatestVersion(config);
    }

    public boolean isGreaterThan(KernelVersion other) {
        return version > other.version;
    }

    public boolean isGreaterThan(byte other) {
        return version > other;
    }

    public boolean isLessThan(KernelVersion other) {
        return version < other.version;
    }

    public boolean isLessThan(byte other) {
        return version < other;
    }

    public boolean isAtLeast(KernelVersion other) {
        return version >= other.version;
    }

    @Override
    public String toString() {
        return "KernelVersion{" + name() + ",version=" + version + '}';
    }

    public static KernelVersion getForVersion(byte version) {
        KernelVersion kernelVersion = versionMap.get(version);
        if (kernelVersion == null) {
            throw new IllegalArgumentException(
                    "No matching " + KernelVersion.class.getSimpleName() + " for version " + version);
        }
        return kernelVersion;
    }
}
