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
package org.neo4j.kernel.impl.store.format;

/**
 * All known store formats are collected here.
 */
public enum StoreVersion {
    STANDARD_V4_3(FormatFamily.STANDARD, 0, 1, "4.3.0", true),
    STANDARD_V5_0(FormatFamily.STANDARD, 1, 1, "5.0.0", false),

    ALIGNED_V4_3(FormatFamily.ALIGNED, 0, 1, "4.3.0", true),
    ALIGNED_V5_0(FormatFamily.ALIGNED, 1, 1, "5.0.0", false),

    MULTIVERSION(FormatFamily.MULTIVERSION, 1, 1, "5.0.0", false),

    HIGH_LIMIT_V4_3(FormatFamily.HIGH_LIMIT, 0, 1, "4.3.0", true),
    HIGH_LIMIT_V5_0(FormatFamily.HIGH_LIMIT, 1, 1, "5.0.0", false);

    private final FormatFamily formatFamily;
    private final int majorVersion;
    private final int minorVersion;
    private final String introductionVersion;
    private final boolean onlyForMigration;

    StoreVersion(
            FormatFamily formatFamily,
            int majorVersion,
            int minorVersion,
            String introductionVersion,
            boolean onlyForMigration) {
        this.formatFamily = formatFamily;
        this.majorVersion = majorVersion;
        this.minorVersion = minorVersion;
        this.introductionVersion = introductionVersion;
        this.onlyForMigration = onlyForMigration;
    }

    public FormatFamily formatFamily() {
        return formatFamily;
    }

    public int majorVersion() {
        return majorVersion;
    }

    public int minorVersion() {
        return minorVersion;
    }

    public String introductionVersion() {
        return introductionVersion;
    }

    public boolean onlyForMigration() {
        return onlyForMigration;
    }
}
