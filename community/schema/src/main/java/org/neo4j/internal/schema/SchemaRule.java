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
package org.neo4j.internal.schema;

import org.neo4j.string.Mask;

/**
 * Represents a stored schema rule.
 */
public interface SchemaRule extends SchemaDescriptorSupplier, Mask.Maskable {
    /**
     * The persistence id for this rule.
     */
    long getId();

    /**
     * @return The (possibly user supplied) name of this schema rule.
     */
    String getName();

    /**
     * Produce a copy of this schema rule, that has the given name.
     * If the given name is {@code null}, then this schema rule is returned unchanged.
     * @param name The name of the new schema rule.
     * @return a modified copy of this schema rule.
     * @throws IllegalArgumentException if the given name is not {@code null}, and it fails the sanitise check.
     */
    SchemaRule withName(String name);
}
