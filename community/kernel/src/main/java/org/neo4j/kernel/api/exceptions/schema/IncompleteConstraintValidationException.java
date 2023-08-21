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
package org.neo4j.kernel.api.exceptions.schema;

import java.util.Set;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.schema.constraints.IndexBackedConstraintDescriptor;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;

/**
 * Exception type to throw on a level where we don't know what constraint
 * is connected to the index where we see violation of uniqueness.
 * To be caught and turned into a {@link UniquePropertyValueValidationException} at a level where
 * that information is available.
 */
public class IncompleteConstraintValidationException extends Exception {
    private final Set<IndexEntryConflictException> conflicts;
    private final ConstraintValidationException.Phase phase;

    public IncompleteConstraintValidationException(
            ConstraintValidationException.Phase phase, Set<IndexEntryConflictException> conflicts) {
        this.phase = phase;
        this.conflicts = conflicts;
    }

    public UniquePropertyValueValidationException turnIntoRealException(
            IndexBackedConstraintDescriptor constraint, TokenNameLookup tokenNameLookup) {
        return new UniquePropertyValueValidationException(constraint, phase, conflicts, tokenNameLookup);
    }
}
