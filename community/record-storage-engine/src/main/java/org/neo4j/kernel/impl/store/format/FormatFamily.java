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
 * Family of the format. Family of format is specific to a format across all version that format support.
 * Two formats in different versions should have same format family.
 * Family is one of the criteria that will determine if migration between formats is possible.
 */
public record FormatFamily(String name, int rank) {
    public static final FormatFamily STANDARD = new FormatFamily("standard", 0);
    public static final FormatFamily ALIGNED = new FormatFamily("aligned", 1);
    public static final FormatFamily HIGH_LIMIT = new FormatFamily("high_limit", 2);
    public static final FormatFamily MULTIVERSION = new FormatFamily("multiversion", 3);

    /**
     * Check if this format family is higher ranked than another format family.
     * It is generally possible to migrate from a lower ranked family to a higher ranked family.
     * @param other family to compare with.
     * @return {@code true} if this family is higher ranked than {@code other}.
     */
    public boolean isHigherThan(FormatFamily other) {
        return rank > other.rank;
    }

    @Override
    public String toString() {
        return name;
    }
}
