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

import java.util.Objects;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public record LabelPropertySegment(String label, String property, Value value, boolean equals) implements Segment {

    public LabelPropertySegment(String property, Value value, boolean equals) {
        this(null, property, value, equals);
    }

    public LabelPropertySegment(String label, String property, Value value, boolean equals) {
        Objects.requireNonNull(property, "property must not be null");
        Objects.requireNonNull(value, "value must not be null");
        this.property = property;
        this.value = value;
        this.equals = equals;
        this.label = (Objects.equals(label, "*") ? null : label);
    }

    @Override
    public String toString() {
        String nodeString = String.format("(n%s)", label == null ? "" : ":" + label);
        String propertyString = String.format("n.%s", property);
        String predicateString = (this.value == Values.NO_VALUE
                ? (this.equals ? "IS NULL" : "IS NOT NULL")
                : (this.equals ? "= " : "<> ") + this.value.prettyPrint());

        return String.format("FOR %s WHERE %s %s", nodeString, propertyString, predicateString);
    }

    @Override
    public boolean satisfies(Segment segment) {
        throw new UnsupportedOperationException();
    }
}
