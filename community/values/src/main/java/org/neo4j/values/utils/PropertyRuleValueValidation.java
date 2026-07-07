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
package org.neo4j.values.utils;

import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.NoValue;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.virtual.ListValue;

/**
 * Shared validation of values used in property-based access control rules. The same checks must be applied at
 * semantic-analysis time (on statically known literals) and at runtime (once parameters and functions are resolved),
 * so the detection logic lives here and each caller maps the outcome to its own error type.
 */
public final class PropertyRuleValueValidation {

    private PropertyRuleValueValidation() {}

    public enum ListValidity {
        VALID,
        CONTAINS_NULL,
        CONTAINS_NAN,
        MIXED_TYPES
    }

    /**
     * Validates the elements of a list intended for use in a property-based access control rule. A valid list contains
     * no nulls, no NaN values, and is homogeneous in its value group.
     */
    public static ListValidity validateListElements(ListValue list) {
        // NULL takes precedence over NaN, which takes precedence over mixed types, regardless of element order
        boolean containsNaN = false;
        boolean mixedTypes = false;
        ValueGroup group = null;
        for (AnyValue value : list.asArray()) {
            if (value instanceof NoValue) {
                return ListValidity.CONTAINS_NULL;
            }
            containsNaN |= AnyValue.isNaN(value);
            ValueGroup valueGroup = value.valueRepresentation().valueGroup();
            if (group == null) {
                group = valueGroup;
            } else if (valueGroup != group) {
                mixedTypes = true;
            }
        }

        if (containsNaN) {
            return ListValidity.CONTAINS_NAN;
        }
        if (mixedTypes) {
            return ListValidity.MIXED_TYPES;
        }
        return ListValidity.VALID;
    }
}
