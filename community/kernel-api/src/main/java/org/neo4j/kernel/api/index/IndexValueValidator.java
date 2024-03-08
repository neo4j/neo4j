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
package org.neo4j.kernel.api.index;

import static java.lang.String.format;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.values.storable.Value;

public interface IndexValueValidator {
    void validate(long entityId, Value... values);

    IndexValueValidator NO_VALIDATION = (entityId, values) -> {};

    static void throwSizeViolationException(
            IndexDescriptor descriptor, TokenNameLookup tokenNameLookup, long entityId, int size) {
        throw new IllegalArgumentException(format(
                "Property value is too large to index, please see index documentation for limitations. Index: %s, entity id: %d, property size: %d.",
                descriptor.userDescription(tokenNameLookup), entityId, size));
    }
}
