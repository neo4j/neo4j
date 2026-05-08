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

import static java.lang.Byte.compareUnsigned;
import static java.lang.Byte.toUnsignedInt;
import static org.neo4j.util.Preconditions.checkArgument;

import java.util.List;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.internal.helpers.collection.ByteToEnum;

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
    // V4_0 only here to keep support for old checkpoint records parsing.
    V4_0(1), // 4.0 to 4.1. Added checksums to the log files.
    V4_2(2), // 4.2+. Removed checkpoint entries.
    // 4.3(some drop)+. Not a change to log entry format, but record storage engine log format change. Since record
    // storage commands has no command version of their own it relies on a bump of the parser set version to
    // distinguish between versions unfortunately. Also introduces token index and relationship property index features.
    V4_3_D4(3),
    V4_4(4), // 4.4. Introduces RANGE, POINT and TEXT index types.
    V5_0(5), // 5.0.
    V5_7(6), // 5.7. Introduces chunked transactions and relationship uniqueness/key constraints.
    V5_8(7), // 5.8. Introduces index usage statistics and enrichment command.
    V5_9(8), // 5.9. Introduces type constraints for single scalar types.
    V5_10(9), // 5.10. Introduces block format and type constraints for unions and lists.
    V5_11(10), // 5.11. Introduces specific add/remove commands, and the VECTOR index type.
    V5_12(11), // 5.12. Introduces user metadata for CDC
    V5_13(12), // 5.13.
    V5_14(13), // 5.14.
    V5_15(14), // 5.15. Changes around CDC logical keys
    V5_18(15), // 5.18. Introduce vector-2.0 index provider
    V5_19(16), // 5.19. Introduce commit timestamps to change identifiers
    V5_20(17), // 5.20. Append index for commands, logs, checkpoint
    V5_22(18), // 5.22. Checkpoint entry with the earliest not completed position
    V5_23(19), // 5.23. Introduce quantization for vector index. Also partitions large int arrays in block format
    V5_25(20), // 5.25. MVCC index commands schema rules. Also introduce token length limit.
    V2025_04(21), // 2025_04. CDC ID switch to DB name instead of UUID
    V2025_05(22), // 2025_05. New start entry serialization (without previous checksum)
    V2025_07(23), // 2025_07. Block MVCC index commands
    V2025_08(24), // 2025_08. No actual change; however, a new version enables rollout of envelopes for more DBs
    V2025_09(25), // 2025_09. Introduced Lucene 10 and bumped the lucene index providers
    V2025_10(26), // 2025_10. Introduce vector types, distributed database creation
    V2025_11(27), // 2025_11. Before state serialization for block commands
    V2026_01(28), // 2026_01. Introduce label existence and endpoint constraints
    V2026_02(29), // 2026_02. SPD property shards keeps only relevant commands in tx log

    // An unreleased future version.
    // This version is meant to be used when developing a new feature
    // and it is not sure which version the feature will land in.
    GLORIOUS_FUTURE(254, 7);
    // 255(or -1) is typically used as a non-existing value, so we don't use that here

    public static final KernelVersion EARLIEST = V4_2;
    // The latest version should be kept private to be able to override it from tests.
    // getLatestVersion should be used when the latest version is required.
    private static final KernelVersion LATEST = V2026_02;
    public static final KernelVersion VERSION_IN_WHICH_TOKEN_INDEXES_ARE_INTRODUCED = V4_3_D4;
    public static final KernelVersion VERSION_RANGE_POINT_TEXT_INDEXES_ARE_INTRODUCED = V4_4;
    public static final KernelVersion VERSION_LITTLE_ENDIAN_TX_LOG_INTRODUCED = V5_0;
    public static final KernelVersion VERSION_TRIGRAM_INDEX_INTRODUCED = V5_0; // technically 5.1, but no kernel version
    public static final KernelVersion VERSION_REL_UNIQUE_CONSTRAINTS_INTRODUCED = V5_7;
    public static final KernelVersion VERSION_INDEX_USAGE_STATISTICS_INTRODUCED = V5_8;
    public static final KernelVersion VERSION_CDC_INTRODUCED = V5_8;
    public static final KernelVersion VERSION_TYPE_CONSTRAINTS_INTRODUCED = V5_9;
    public static final KernelVersion VERSION_AUTOMATIC_SYSTEM_DB_UPGRADE_INTRODUCED = V5_9;
    public static final KernelVersion VERSION_BLOCKFORMAT_INTRODUCED = V5_14;
    public static final KernelVersion VERSION_UNIONS_AND_LIST_TYPE_CONSTRAINTS_INTRODUCED = V5_10;
    public static final KernelVersion VERSION_NODE_VECTOR_INDEX_INTRODUCED = V5_11;
    public static final KernelVersion VERSION_CDC_USER_METADATA_INTRODUCED = V5_12;
    public static final KernelVersion CLUSTER_FALLBACK_IN_RAW = V5_12;
    public static final KernelVersion VERSION_CDC_LOGICAL_KEY_CHANGES = V5_15;
    // Despite the name not actually guaranteed from this version yet. Depends on if settings turn it on.
    public static final KernelVersion VERSION_ENVELOPED_TRANSACTION_LOGS_GUARANTEED = GLORIOUS_FUTURE;
    // Enveloped format will be introduced disconnected from a specific kernel version. It still relies on
    // upgrading kernel version but it is not guaranteed on which one. Any version from this one can have the format
    public static final KernelVersion VERSION_ENVELOPED_TRANSACTION_CAN_EXIST_FROM = V2025_07;
    public static final KernelVersion VERSION_VECTOR_2_INTRODUCED = V5_18;
    public static final KernelVersion VERSION_CDC_CHECKSUMS_INTRODUCED = V5_19;
    public static final KernelVersion VERSION_APPEND_INDEX_INTRODUCED = V5_20;
    public static final KernelVersion VERSION_CHECKPOINT_NOT_COMPLETED_POSITION_INTRODUCED = V5_22;
    public static final KernelVersion VERSION_VECTOR_QUANTIZATION_AND_HYPER_PARAMS = V5_23;
    public static final KernelVersion VERSION_PARTITIONED_BLOCK_SCHEMA_RULE_VALUES = V5_23;
    public static final KernelVersion VERSION_PARTITIONED_BLOCK_SCHEMA_RULE_INT_ARRAY =
            VERSION_PARTITIONED_BLOCK_SCHEMA_RULE_VALUES;
    public static final KernelVersion VERSION_PARTITIONED_BLOCK_TOKENS = V5_25;
    public static final KernelVersion VERSION_SPDFORMAT_INTRODUCED = V5_25;
    public static final KernelVersion VERSION_CDC_USE_NAME_INTRODUCED = V2025_04;
    public static final KernelVersion VERSION_UPGRADE_CONTAINS_LOG_FORMAT = V2025_05;
    public static final KernelVersion VERSION_LUCENE_10_INTRODUCED = V2025_09;
    public static final KernelVersion VERSION_VECTOR_TYPE_INTRODUCED = V2025_10;
    public static final KernelVersion VERSION_DISTRIBUTED_CREATE_DATABASE_INTRODUCED = V2025_10;
    public static final KernelVersion VERSION_RELATIONSHIP_ENDPOINT_LABEL_AND_LABEL_EXISTENCE_CONSTRAINTS_INTRODUCED =
            V2026_01;
    public static final KernelVersion VERSION_VECTOR_INDEX_SINGLE_STAGE_FILTERING = V2026_01;
    public static final KernelVersion VERSION_PARTITIONED_BLOCK_SCHEMA_RULE_TEXT = V2026_01;
    public static final KernelVersion VERSION_SPD_FILTERED_PROPERTY_SHARD_TX_LOG = V2026_02;
    public static final KernelVersion VERSION_VECTOR_BINARY_QUANTIZATION = GLORIOUS_FUTURE;

    // Keep updated each time there is an new schema rule added
    // related to IntegrityValidator
    public static final KernelVersion LATEST_SCHEMA_CHANGE = VERSION_VECTOR_INDEX_SINGLE_STAGE_FILTERING;
    public static final KernelVersion VERSION_UUID_VALUE_INTRODUCED = GLORIOUS_FUTURE;

    // All neo4j 5.0-5.6 members defaulted to this version when bootstrapping a rafted database
    public static final KernelVersion DEFAULT_BOOTSTRAP_VERSION = V5_0;

    public static final KernelVersion VERSION_LEASES_IN_START_ENTRIES = GLORIOUS_FUTURE;

    public static final List<KernelVersion> VERSIONS = List.of(values());
    private static final ByteToEnum<KernelVersion> VERSION_MAP =
            new ByteToEnum<>(KernelVersion.class, KernelVersion::version);

    private final byte version;
    private final int contentMarshallerVersion;

    public static KernelVersion getLatestVersion(Configuration config) {
        Byte version = config.get(GraphDatabaseInternalSettings.latest_kernel_version);
        return version == null ? LATEST : KernelVersion.getForVersion(version);
    }

    KernelVersion(int version) {
        checkArgument((version & ~0xFF) == 0, "Byte overflow");
        this.version = (byte) version;
        this.contentMarshallerVersion = 0;
    }

    KernelVersion(int version, int contentMarshallerVersion) {
        checkArgument((version & ~0xFF) == 0, "Byte overflow");
        this.version = (byte) version;
        this.contentMarshallerVersion = contentMarshallerVersion;
    }

    /**
     * Get the byte representation of the kernel version.
     * <p>
     * <strong>
     *   NOTE! Since this is signed it's not possible to compare based on it.
     *   Use {@link #versionAsInt} if comparing is required.
     * </strong>
     * @return the byte representation of the kernel version.
     */
    public byte version() {
        return this.version;
    }

    /**
     * Get the integer representation of the kernel version.
     * This can be used to compare different kernel versions lexicographically.
     * @return the integer representation of the kernel version.
     */
    public int versionAsInt() {
        return Byte.toUnsignedInt(this.version);
    }

    public boolean isLatest(Config config) {
        return this == getLatestVersion(config);
    }

    public boolean isGreaterThan(KernelVersion other) {
        return isGreaterThan(other.version);
    }

    public boolean isGreaterThan(byte other) {
        return compareUnsigned(version, other) > 0;
    }

    public boolean isLessThan(KernelVersion other) {
        return isLessThan(other.version);
    }

    public boolean isLessThan(byte other) {
        return compareUnsigned(version, other) < 0;
    }

    public boolean isAtLeast(KernelVersion other) {
        return compareUnsigned(version, other.version) >= 0;
    }

    @Override
    public String toString() {
        return "KernelVersion{" + name() + ",version=" + toUnsignedInt(version) + '}';
    }

    public static KernelVersion getForVersion(byte version) {
        KernelVersion kernelVersion = VERSION_MAP.get(version);
        if (kernelVersion == null) {
            throw new IllegalArgumentException(
                    "No matching " + KernelVersion.class.getSimpleName() + " for version " + toUnsignedInt(version));
        }
        return kernelVersion;
    }

    public static KernelVersion precedingVersion(KernelVersion kernelVersion) {
        int index = VERSIONS.indexOf(kernelVersion);
        checkArgument(index != -1, "Unknown kernel version " + kernelVersion);
        checkArgument(index > 0, "There's no kernel version preceding " + kernelVersion);
        return VERSIONS.get(index - 1);
    }

    public int contentMarshallerVersion() {
        return contentMarshallerVersion;
    }
}
