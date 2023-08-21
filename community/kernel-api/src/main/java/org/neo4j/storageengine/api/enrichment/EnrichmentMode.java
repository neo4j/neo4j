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
package org.neo4j.storageengine.api.enrichment;

import java.util.Locale;
import java.util.Objects;

public enum EnrichmentMode {
    /**
     * Indicates that no enrichment of transactions should occur
     */
    OFF,
    /**
     * Indicates that enrichment should occur but only the changes (rather than the entire state) should be recorded
     */
    DIFF,
    /**
     * Indicates that enrichment should occur and the entire entity state should be recorded
     */
    FULL;

    public static EnrichmentMode create(Object value) {
        Objects.requireNonNull(value);

        final var mode = value.toString().toUpperCase(Locale.ROOT);
        try {
            return EnrichmentMode.valueOf(mode);
        } catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException("Unsupported enrichment mode: " + mode, ex);
        }
    }
}
