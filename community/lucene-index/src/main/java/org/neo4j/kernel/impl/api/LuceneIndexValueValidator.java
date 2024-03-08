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
package org.neo4j.kernel.impl.api;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexValueValidator;
import org.neo4j.util.Preconditions;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

/**
 * Validates {@link Value values} that are about to get indexed into a Lucene index.
 * Values passing this validation are OK to commit and apply to a Lucene index.
 */
public class LuceneIndexValueValidator implements IndexValueValidator {
    // Maximum bytes value length that supported by indexes.
    // Absolute hard maximum length for a term, in bytes once
    // encoded as UTF8.  If a term arrives from the analyzer
    // longer than this length, an IllegalArgumentException
    // when lucene writer trying to add or update document
    public static final int MAX_TERM_LENGTH = (1 << 15) - 2;

    private final IndexDescriptor descriptor;
    private final TokenNameLookup tokenNameLookup;
    private final int checkThreshold;

    public LuceneIndexValueValidator(IndexDescriptor descriptor, TokenNameLookup tokenNameLookup) {
        this.descriptor = descriptor;
        this.tokenNameLookup = tokenNameLookup;
        // This check threshold is for not having to check every value that comes in, only those that may have a chance
        // to exceed the max length.
        // The value 5 comes from a safer 4, which is the number of bytes that a max size UTF-8 code point needs.
        this.checkThreshold = MAX_TERM_LENGTH / 5;
    }

    @Override
    public void validate(long entityId, Value... values) {
        // In Lucene all values in a tuple (composite index) will be placed in a separate field, so validate their
        // fields individually.
        for (Value value : values) {
            validate(entityId, value);
        }
    }

    private void validate(long entityId, Value value) {
        Preconditions.checkArgument(value != null && value != Values.NO_VALUE, "Null value");
        if (Values.isTextValue(value) && ((TextValue) value).length() >= checkThreshold) {
            int length = ((TextValue) value).stringValue().getBytes().length;
            validateActualLength(entityId, length);
        }
        if (Values.isArrayValue(value)) {
            validateActualLength(entityId, ArrayEncoder.encode(value).getBytes().length);
        }
    }

    private void validateActualLength(long entityId, int length) {
        if (length > MAX_TERM_LENGTH) {
            IndexValueValidator.throwSizeViolationException(descriptor, tokenNameLookup, entityId, length);
        }
    }
}
