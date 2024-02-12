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

import static org.neo4j.common.Edition.COMMUNITY;

public enum ZippedStoreCommunity implements ZippedStore {
    // Stores with special node label index
    AF430_V42_INJECTED_NLI(
            "AF4.3.0_V4.2_empty_community_injected_nli.zip",
            new DbStatistics("AF4.3.0", KernelVersion.V4_2, 1, COMMUNITY, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)),
    AF430_V43D4_PERSISTED_NLI(
            "AF4.3.0_V4.3.D4_empty_community_persisted_nli.zip",
            new DbStatistics("AF4.3.0", KernelVersion.V4_3_D4, 2, COMMUNITY, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 1)),
    // 4.3 stores
    SF430_V43D4_ALL_NO_BTREE(
            "SF4.3.0_V4.3.D4_all-no-btree_community.zip",
            new DbStatistics(
                    "SF4.3.0",
                    KernelVersion.V4_3_D4,
                    11,
                    COMMUNITY,
                    2587,
                    2576,
                    11,
                    2586,
                    351,
                    3726,
                    3725,
                    11946,
                    10,
                    2,
                    0,
                    0,
                    0,
                    10)),
    AF430_V43D4_ALL_NO_BTREE(
            "AF4.3.0_V4.3.D4_all-no-btree_community.zip",
            new DbStatistics(
                    "AF4.3.0",
                    KernelVersion.V4_3_D4,
                    11,
                    COMMUNITY,
                    2586,
                    2574,
                    12,
                    2585,
                    349,
                    3740,
                    3739,
                    11941,
                    10,
                    2,
                    0,
                    0,
                    0,
                    10)),
    // 4.4 stores
    SF430_V44_EMPTY(
            "SF4.3.0_V4.4_empty_community.zip",
            new DbStatistics("SF4.3.0", KernelVersion.V4_4, 4, COMMUNITY, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)),
    AF430_V44_EMPTY(
            "AF4.3.0_V4.4_empty_community.zip",
            new DbStatistics("AF4.3.0", KernelVersion.V4_4, 4, COMMUNITY, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)),
    SF430_V44_ALL(
            "SF4.3.0_V4.4_all_community.zip",
            new DbStatistics(
                    "SF4.3.0",
                    KernelVersion.V4_4,
                    23,
                    COMMUNITY,
                    2585,
                    2573,
                    12,
                    2584,
                    366,
                    3738,
                    3737,
                    11872,
                    36,
                    2,
                    12,
                    8,
                    4,
                    36)),
    AF430_V44_ALL(
            "AF4.3.0_V4.4_all_community.zip",
            new DbStatistics(
                    "AF4.3.0",
                    KernelVersion.V4_4,
                    23,
                    COMMUNITY,
                    2549,
                    2537,
                    12,
                    2548,
                    337,
                    3636,
                    3635,
                    11676,
                    36,
                    2,
                    12,
                    8,
                    4,
                    36)),
    // 5.0 stores
    REC_AF11_V50_ALL(
            "record-aligned-1.1_V5.0_all_community.zip",
            new DbStatistics(
                    "record-aligned-1.1",
                    KernelVersion.V5_0,
                    19,
                    COMMUNITY,
                    2521,
                    2509,
                    12,
                    2520,
                    371,
                    3573,
                    3572,
                    11250,
                    22,
                    2,
                    0,
                    4,
                    0,
                    22)),
    REC_AF11_V50_EMPTY(
            "record-aligned-1.1_V5.0_empty_community.zip",
            new DbStatistics(
                    "record-aligned-1.1", KernelVersion.V5_0, 4, COMMUNITY, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)),
    REC_SF11_V50_ALL(
            "record-standard-1.1_V5.0_all_community.zip",
            new DbStatistics(
                    "record-standard-1.1",
                    KernelVersion.V5_0,
                    19,
                    COMMUNITY,
                    2417,
                    2404,
                    13,
                    2416,
                    348,
                    3432,
                    3431,
                    11019,
                    22,
                    2,
                    0,
                    4,
                    0,
                    22)),
    REC_SF11_V50_EMPTY(
            "record-standard-1.1_V5.0_empty_community.zip",
            new DbStatistics(
                    "record-standard-1.1", KernelVersion.V5_0, 4, COMMUNITY, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)),
    // 5.8 stores
    REC_AF11_V58_ALL(
            "record-aligned-1.1_V5.8_all_community.zip",
            new DbStatistics(
                    "record-aligned-1.1",
                    KernelVersion.V5_8,
                    19,
                    COMMUNITY,
                    2527,
                    2513,
                    14,
                    2526,
                    369,
                    3616,
                    3615,
                    11648,
                    22,
                    2,
                    0,
                    4,
                    0,
                    22)),
    REC_AF11_V58_EMPTY(
            "record-aligned-1.1_V5.8_empty_community.zip",
            new DbStatistics(
                    "record-aligned-1.1", KernelVersion.V5_8, 4, COMMUNITY, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)),
    REC_SF11_V58_ALL(
            "record-standard-1.1_V5.8_all_community.zip",
            new DbStatistics(
                    "record-standard-1.1",
                    KernelVersion.V5_8,
                    19,
                    COMMUNITY,
                    2437,
                    2426,
                    11,
                    2436,
                    358,
                    3504,
                    3503,
                    11218,
                    22,
                    2,
                    0,
                    4,
                    0,
                    22)),
    REC_SF11_V58_EMPTY(
            "record-standard-1.1_V5.8_empty_community.zip",
            new DbStatistics(
                    "record-standard-1.1", KernelVersion.V5_8, 4, COMMUNITY, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)),
    REC_AF11_V510_EMPTY(
            "record-aligned-1.1_V5.10_empty_community.zip",
            new DbStatistics(
                    "record-aligned-1.1", KernelVersion.V5_10, 4, COMMUNITY, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)),

    REC_AF11_V515_EMPTY(
            "record-aligned-1.1_V5.15_empty_community.zip",
            new DbStatistics(
                    "record-aligned-1.1", KernelVersion.V5_15, 4, COMMUNITY, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0));

    private final String zipFileName;
    private final DbStatistics statistics;

    ZippedStoreCommunity(String zipFileName, DbStatistics statistics) {
        this.zipFileName = zipFileName;
        this.statistics = statistics;
    }

    @Override
    public DbStatistics statistics() {
        return statistics;
    }

    @Override
    public String zipFileName() {
        return zipFileName;
    }
}
