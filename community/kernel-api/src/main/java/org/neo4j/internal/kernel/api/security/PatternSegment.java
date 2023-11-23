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
package org.neo4j.internal.kernel.api.security;

import java.util.Set;
import java.util.stream.Collectors;
import org.neo4j.util.Preconditions;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public record PatternSegment(Set<String> labels, String property, Value value, boolean eq) implements Segment {

    private static final Set<String> EMPTY = Set.of();

    public PatternSegment(String property, Value value, boolean equals) {
        this(EMPTY, property, value, equals);
    }

    public PatternSegment {
        Preconditions.requireNonNull(labels, "labels must not be null");
        Preconditions.requireNonNull(property, "property must not be null");
        Preconditions.requireNonNull(value, "value must not be null");
    }

    @Override
    public String toString() {
        String labelsString =
                labels.isEmpty() ? "" : labels.stream().sorted().collect(Collectors.joining("|", ":", ""));
        String nodeString = String.format("(n%s)", labelsString);
        String propertyString = String.format("n.%s", property);
        String predicateString = (this.value == Values.NO_VALUE
                ? (this.eq ? "IS NULL" : "IS NOT NULL")
                : (this.eq ? "= " : "<> ") + this.value.prettyPrint());

        return String.format("FOR %s WHERE %s %s", nodeString, propertyString, predicateString);
    }

    @Override
    public boolean satisfies(Segment segment) {
        throw new UnsupportedOperationException();
    }
}
