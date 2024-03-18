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
package org.neo4j.internal.batchimport.input.csv;

import java.util.ArrayList;
import java.util.List;
import org.neo4j.internal.batchimport.input.Group;
import org.neo4j.internal.helpers.collection.Iterables;

/**
 * Contains logic around a single or multiple :ID columns, the combined value and also which parts are stored
 * as properties on the node.
 */
class IdValueBuilder {
    private final List<Part> parts = new ArrayList<>();
    private Group group;

    void clear() {
        parts.clear();
        group = null;
    }

    void part(Object value, Header.Entry entry) {
        if (group != null && !entry.group().equals(group)) {
            throw new IllegalStateException(
                    "Multiple ID columns for different groups:" + group + " and " + entry.group());
        }
        parts.add(new Part(entry.name(), value));
        this.group = entry.group();
    }

    Object value() {
        return switch (parts.size()) {
            case 0 -> null;
            case 1 -> parts.get(0).value;
            default -> {
                var result = new StringBuilder();
                for (var part : parts) {
                    result.append(part.value);
                }
                yield result.toString();
            }
        };
    }

    Group group() {
        return group;
    }

    Iterable<Part> idPropertyValues() {
        return Iterables.filter(p -> p.name != null, parts);
    }

    boolean isEmpty() {
        return parts.isEmpty();
    }

    record Part(String name, Object value) {}
}
