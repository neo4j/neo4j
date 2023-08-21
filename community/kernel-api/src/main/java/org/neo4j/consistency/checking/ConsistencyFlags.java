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
package org.neo4j.consistency.checking;

import java.util.Objects;

public record ConsistencyFlags(
        boolean checkStructure,
        boolean checkIndexes,
        boolean checkGraph,
        boolean checkCounts,
        boolean checkPropertyOwners,
        boolean checkPropertyValues) {
    public static final ConsistencyFlags NONE = new ConsistencyFlags(false, false, false, false, false, false);
    public static final ConsistencyFlags ALL = new ConsistencyFlags(true, true, true, true, true, true);
    public static final ConsistencyFlags DEFAULT =
            ALL.withoutCheckPropertyOwners().withoutCheckPropertyValues();

    public ConsistencyFlags {
        IllegalArgumentException argumentException = null;
        if (!checkGraph) {
            if (checkCounts) {
                argumentException =
                        requireNonNullAndAppendException(argumentException, checkGraphInvariant("checkCounts"));
            }
            if (checkPropertyOwners) {
                argumentException =
                        requireNonNullAndAppendException(argumentException, checkGraphInvariant("checkPropertyOwners"));
            }
            if (checkPropertyValues) {
                argumentException =
                        requireNonNullAndAppendException(argumentException, checkGraphInvariant("checkPropertyValues"));
            }
        }

        if (argumentException != null) {
            throw argumentException;
        }
    }

    public ConsistencyFlags(
            boolean checkIndexes, boolean checkGraph, boolean checkCounts, boolean checkPropertyOwners) {
        this(
                DEFAULT.checkStructure(),
                checkIndexes,
                checkGraph,
                checkCounts,
                checkPropertyOwners,
                DEFAULT.checkPropertyValues());
    }

    public ConsistencyFlags withCheckStructure() {
        return new ConsistencyFlags(
                true, checkIndexes, checkGraph, checkCounts, checkPropertyOwners, checkPropertyValues);
    }

    public ConsistencyFlags withoutCheckStructure() {
        return new ConsistencyFlags(
                false, checkIndexes, checkGraph, checkCounts, checkPropertyOwners, checkPropertyValues);
    }

    public ConsistencyFlags withCheckIndexes() {
        return new ConsistencyFlags(
                checkStructure, true, checkGraph, checkCounts, checkPropertyOwners, checkPropertyValues);
    }

    public ConsistencyFlags withoutCheckIndexes() {
        return new ConsistencyFlags(
                checkStructure, false, checkGraph, checkCounts, checkPropertyOwners, checkPropertyValues);
    }

    public ConsistencyFlags withCheckGraph() {
        return new ConsistencyFlags(
                checkStructure, checkIndexes, true, checkCounts, checkPropertyOwners, checkPropertyValues);
    }

    public ConsistencyFlags withoutCheckGraph() {
        return new ConsistencyFlags(checkStructure, checkIndexes, false, false, false, false);
    }

    public ConsistencyFlags withCheckCounts() {
        return new ConsistencyFlags(checkStructure, checkIndexes, true, true, checkPropertyOwners, checkPropertyValues);
    }

    public ConsistencyFlags withoutCheckCounts() {
        return new ConsistencyFlags(
                checkStructure, checkIndexes, checkGraph, false, checkPropertyOwners, checkPropertyValues);
    }

    public ConsistencyFlags withCheckPropertyOwners() {
        return new ConsistencyFlags(checkStructure, checkIndexes, true, checkCounts, true, checkPropertyValues);
    }

    public ConsistencyFlags withoutCheckPropertyOwners() {
        return new ConsistencyFlags(checkStructure, checkIndexes, checkGraph, checkCounts, false, checkPropertyValues);
    }

    public ConsistencyFlags withCheckPropertyValues() {
        return new ConsistencyFlags(checkStructure, checkIndexes, true, checkCounts, checkPropertyOwners, true);
    }

    public ConsistencyFlags withoutCheckPropertyValues() {
        return new ConsistencyFlags(checkStructure, checkIndexes, checkGraph, checkCounts, checkPropertyOwners, false);
    }

    private static IllegalArgumentException requireNonNullAndAppendException(
            IllegalArgumentException argumentException, Exception exception) {
        final var ex = Objects.requireNonNullElseGet(argumentException, IllegalArgumentException::new);
        ex.addSuppressed(exception);
        return ex;
    }

    private static IllegalArgumentException checkGraphInvariant(String check) {
        return new IllegalArgumentException(
                "'%s' cannot be set to '%s' with 'checkGraph' set to '%s'.".formatted(check, true, false));
    }
}
