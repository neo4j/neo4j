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

import java.util.Collections;
import java.util.Set;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.schema.constraints.IndexBackedConstraintDescriptor;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;

public class UniquePropertyValueValidationException extends ConstraintValidationException {
    private final Set<IndexEntryConflictException> conflicts;

    public UniquePropertyValueValidationException(
            IndexBackedConstraintDescriptor constraint,
            ConstraintValidationException.Phase phase,
            IndexEntryConflictException conflict,
            TokenNameLookup tokenNameLookup) {
        this(constraint, phase, Collections.singleton(conflict), tokenNameLookup);
    }

    public UniquePropertyValueValidationException(
            IndexBackedConstraintDescriptor constraint,
            ConstraintValidationException.Phase phase,
            Set<IndexEntryConflictException> conflicts,
            TokenNameLookup tokenNameLookup) {
        super(
                constraint,
                phase,
                phase == Phase.VERIFICATION ? "Existing data" : "New data",
                buildCauseChain(conflicts),
                tokenNameLookup);
        this.conflicts = conflicts;
    }

    private static IndexEntryConflictException buildCauseChain(Set<IndexEntryConflictException> conflicts) {
        IndexEntryConflictException chainedConflicts = null;
        for (IndexEntryConflictException conflict : conflicts) {
            chainedConflicts = Exceptions.chain(chainedConflicts, conflict);
        }
        return chainedConflicts;
    }

    public UniquePropertyValueValidationException(
            IndexBackedConstraintDescriptor constraint,
            ConstraintValidationException.Phase phase,
            Throwable cause,
            TokenNameLookup tokenNameLookup) {
        super(constraint, phase, phase == Phase.VERIFICATION ? "Existing data" : "New data", cause, tokenNameLookup);
        this.conflicts = Collections.emptySet();
    }

    @Override
    public String getUserMessage(TokenNameLookup tokenNameLookup) {
        var cause = getCause();
        //noinspection StatementWithEmptyBody
        if (cause instanceof IndexEntryConflictException ignored) {
            // Drop through to the final branch
        } else if (cause instanceof KernelException exc) {
            return exc.getUserMessage(tokenNameLookup);
        } else if (cause instanceof Exception exc) {
            return exc.getMessage();
        }

        final StringBuilder message = new StringBuilder();
        boolean appendLine = false;
        for (IndexEntryConflictException conflict : conflicts) {
            if (appendLine) {
                message.append(System.lineSeparator());
            }
            message.append(conflict.getUserMessage(tokenNameLookup));
            appendLine = true;
        }
        return message.toString();
    }

    public Set<IndexEntryConflictException> conflicts() {
        return conflicts;
    }
}
