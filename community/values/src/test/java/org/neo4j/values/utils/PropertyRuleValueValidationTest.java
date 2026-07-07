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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.storable.Values.doubleValue;
import static org.neo4j.values.storable.Values.floatValue;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.stringValue;

import org.junit.jupiter.api.Test;
import org.neo4j.values.utils.PropertyRuleValueValidation.ListValidity;
import org.neo4j.values.virtual.VirtualValues;

class PropertyRuleValueValidationTest {

    @Test
    void homogeneousListIsValid() {
        assertEquals(
                ListValidity.VALID,
                PropertyRuleValueValidation.validateListElements(VirtualValues.list(intValue(1), intValue(2))));
    }

    @Test
    void emptyListIsValid() {
        assertEquals(ListValidity.VALID, PropertyRuleValueValidation.validateListElements(VirtualValues.list()));
    }

    @Test
    void singleElementListIsValid() {
        assertEquals(
                ListValidity.VALID,
                PropertyRuleValueValidation.validateListElements(VirtualValues.list(stringValue("a"))));
    }

    @Test
    void listContainingNullIsRejected() {
        assertEquals(
                ListValidity.CONTAINS_NULL,
                PropertyRuleValueValidation.validateListElements(VirtualValues.list(intValue(1), NO_VALUE)));
    }

    @Test
    void listContainingNaNIsRejected() {
        assertEquals(
                ListValidity.CONTAINS_NAN,
                PropertyRuleValueValidation.validateListElements(
                        VirtualValues.list(doubleValue(1.0), doubleValue(Double.NaN))));
    }

    @Test
    void listContainingFloatNaNIsRejected() {
        assertEquals(
                ListValidity.CONTAINS_NAN,
                PropertyRuleValueValidation.validateListElements(
                        VirtualValues.list(floatValue(1.0f), floatValue(Float.NaN))));
    }

    @Test
    void mixedTypeListIsRejected() {
        assertEquals(
                ListValidity.MIXED_TYPES,
                PropertyRuleValueValidation.validateListElements(VirtualValues.list(intValue(1), stringValue("a"))));
    }

    @Test
    void nullTakesPrecedenceOverNaN() {
        assertEquals(
                ListValidity.CONTAINS_NULL,
                PropertyRuleValueValidation.validateListElements(
                        VirtualValues.list(doubleValue(Double.NaN), NO_VALUE)));
    }

    @Test
    void nanTakesPrecedenceOverMixedTypes() {
        assertEquals(
                ListValidity.CONTAINS_NAN,
                PropertyRuleValueValidation.validateListElements(
                        VirtualValues.list(stringValue("a"), doubleValue(Double.NaN))));
    }
}
