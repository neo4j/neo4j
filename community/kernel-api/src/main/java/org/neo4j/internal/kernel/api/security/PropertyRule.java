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
import java.util.function.Predicate;
import org.neo4j.values.storable.Value;

public interface PropertyRule extends Predicate<Value> {

    static PropertyRule newRule(int propertyKey, Value value, boolean equals) {
        return new PropertyRule.PropertyValueRule(propertyKey, value, equals);
    }

    int property();

    record PropertyValueRule(int property, Value value, boolean equals) implements PropertyRule {

        public PropertyValueRule {
            Objects.requireNonNull(value, "value must not be null");
        }

        @Override
        public boolean test(Value value) {
            return equals == this.value.equals(value);
        }
    }
}
