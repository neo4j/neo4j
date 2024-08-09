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
package org.neo4j.kernel.impl.index.schema;

import static org.neo4j.kernel.impl.index.schema.GenericKey.BIGGEST_STATIC_SIZE;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexValueValidator;
import org.neo4j.values.AnyValue;
import org.neo4j.values.SequenceValue;
import org.neo4j.values.storable.TextArray;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueCategory;

/**
 * Validates Value[] tuples, whether or not they fit inside a {@link GBPTree} with a layout using {@link GenericKey}.
 * Most values won't even be serialized to {@link GenericKey}, values that fit well within the margin.
 */
class GenericIndexKeyValidator implements IndexValueValidator {
    private final IndexDescriptor descriptor;
    private final int maxLength;
    private final Layout<? extends GenericKey<?>, NullValue> layout;
    private final TokenNameLookup tokenNameLookup;

    GenericIndexKeyValidator(
            int maxLength,
            IndexDescriptor descriptor,
            Layout<? extends GenericKey<?>, NullValue> layout,
            TokenNameLookup tokenNameLookup) {
        this.maxLength = maxLength;
        this.descriptor = descriptor;
        this.layout = layout;
        this.tokenNameLookup = tokenNameLookup;
    }

    @Override
    public void validate(long entityId, Value... values) {
        int worstCaseSize = worstCaseLength(values);
        if (worstCaseSize > maxLength) {
            int size = actualLength(values);
            if (size > maxLength) {
                IndexValueValidator.throwSizeViolationException(descriptor, tokenNameLookup, entityId, size);
            }
        }
    }

    /**
     * A method for calculating some sort of worst-case length of a value tuple. This have to be a cheap call and can return false positives.
     * It exists to avoid serializing all value tuples into native keys, which can be expensive.
     *
     * @param values the value tuple to calculate some exaggerated worst-case size of.
     * @return the calculated worst-case size of the value tuple.
     */
    private static int worstCaseLength(Value[] values) {
        int length = Long.BYTES;
        for (Value value : values) {
            // Add some generic overhead, slightly exaggerated
            length += Long.BYTES;
            // Add worst-case length of this value
            length += worstCaseLength(value);
        }
        return length;
    }

    private static int worstCaseLength(AnyValue value) {
        if (value.isSequenceValue()) {
            SequenceValue sequenceValue = (SequenceValue) value;
            if (sequenceValue instanceof TextArray textArray) {
                int length = 0;
                for (int i = 0; i < textArray.intSize(); i++) {
                    length += stringWorstCaseLength(textArray.stringValue(i).length());
                }
                return length;
            }
            return sequenceValue.intSize() * BIGGEST_STATIC_SIZE;
        } else {
            if (((Value) value).valueGroup().category() == ValueCategory.TEXT) {
                // For text, which is very dynamic in its nature do a worst-case off of number of characters in it
                return stringWorstCaseLength(((TextValue) value).length());
            }
            // For all else then use the biggest possible value for a non-dynamic, non-array value a state can occupy
            return BIGGEST_STATIC_SIZE;
        }
    }

    private static int stringWorstCaseLength(int stringLength) {
        return Types.SIZE_STRING_LENGTH + stringLength * 4;
    }

    private int actualLength(Value[] values) {
        GenericKey<?> key = layout.newKey();
        key.initialize(0 /*doesn't quite matter for size calculations, but an important method to call*/);
        for (int i = 0; i < values.length; i++) {
            key.initFromValue(i, values[i], NativeIndexKey.Inclusion.NEUTRAL);
        }
        return key.size();
    }
}
