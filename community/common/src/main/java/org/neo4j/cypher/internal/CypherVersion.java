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
package org.neo4j.cypher.internal;

import java.util.Optional;

public enum CypherVersion {
    Cypher5("5", "CYPHER 5", false, "cypher-5", 5),
    Cypher25("25", "CYPHER 25", false, "cypher-25", 25);

    public final String versionName;
    public final String description;
    public final boolean experimental;
    public final String persistedValue; // stored on the :Database node in the system graph
    private final int order;

    CypherVersion(String versionName, String description, boolean experimental, String persistedValue, int order) {
        this.versionName = versionName;
        this.description = description;
        this.experimental = experimental;
        this.persistedValue = persistedValue;
        this.order = order;
    }

    @Override
    public String toString() {
        return versionName;
    }

    public boolean isAfter(CypherVersion other) {
        return order > other.order;
    }

    public CypherVersion otherVersion() {
        if (this == CypherVersion.Cypher25) {
            return CypherVersion.Cypher5;
        } else {
            return CypherVersion.Cypher25;
        }
    }

    public boolean isEqualOrAfter(CypherVersion other) {
        return order >= other.order;
    }

    public static CypherVersion fromStoredValue(String storedValue) {
        return fromStoredValueOptional(storedValue)
                .orElseThrow(() -> new IllegalArgumentException(storedValue + " is not a valid CypherVersion"));
    }

    public static Optional<CypherVersion> fromStoredValueOptional(Object storedValue) {
        for (CypherVersion version : CypherVersion.values()) {
            if (version.persistedValue.equals(storedValue)) {
                return Optional.of(version);
            }
        }
        return Optional.empty();
    }

    public static class Legacy {
        /**
         * Never use this, it has no meaning!
         * For testing, have assertions for all languages.
         * For production, query language for a specific query is resolved by Cypher and accessible in InputQuery.
         */
        @Deprecated(since = "2025.06", forRemoval = true)
        public static CypherVersion legacyVersion() {
            return CypherVersion.Cypher5;
        }
    }
}
