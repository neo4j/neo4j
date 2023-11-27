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

public interface PatternSegment extends Segment {

    default String labelsString() {
        return labels().isEmpty() ? "" : labels().stream().sorted().collect(Collectors.joining("|", ":", ""));
    }

    default String nodeString() {
        return String.format("(n%s)", labelsString());
    }

    default String propertyString() {
        return String.format("n.%s", property());
    }

    Set<String> ALL_LABELS = Set.of();

    Set<String> labels();

    String property();

    @Override
    default boolean satisfies(Segment segment) {
        throw new UnsupportedOperationException();
    }

    record ValuePatternSegment(
            Set<String> labels, String property, Value value, PropertyRule.ComparisonOperator operator)
            implements PatternSegment {

        public ValuePatternSegment(String property, Value value, PropertyRule.ComparisonOperator operator) {
            this(ALL_LABELS, property, value, operator);
        }

        public ValuePatternSegment {
            Preconditions.requireNonNull(labels, "labels must not be null");
            Preconditions.requireNonNull(property, "property must not be null");
            Preconditions.requireNonNull(value, "value must not be null");
            Preconditions.checkArgument(
                    value != Values.NO_VALUE, "value must not be NO_VALUE. Use NullPatternSegment for this purpose.");
        }

        @Override
        public String toString() {
            return String.format(
                    "FOR %s WHERE %s %s %s",
                    nodeString(), propertyString(), this.operator.getSymbol(), this.value.prettyPrint());
        }
    }

    record NullPatternSegment(Set<String> labels, String property, PropertyRule.NullOperator operator)
            implements PatternSegment {

        public NullPatternSegment(String property, PropertyRule.NullOperator operator) {
            this(ALL_LABELS, property, operator);
        }

        public NullPatternSegment {
            Preconditions.requireNonNull(labels, "labels must not be null");
            Preconditions.requireNonNull(property, "property must not be null");
        }

        @Override
        public String toString() {
            return String.format("FOR %s WHERE %s %s", nodeString(), propertyString(), this.operator.getSymbol());
        }
    }
}
