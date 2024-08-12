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

import static java.lang.String.format;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.schema.ConstraintDescriptor;

/**
 * Attempting to validate constraints, but the apparatus for validation was not available. For example,
 * this exception is thrown when an index required to implement a uniqueness constraint is not available.
 */
public class UnableToValidateConstraintException extends ConstraintValidationException {
    public UnableToValidateConstraintException(
            ConstraintDescriptor constraint, Throwable cause, TokenNameLookup tokenNameLookup) {
        super(
                constraint,
                Phase.VERIFICATION,
                format("Unable to validate constraint %s", constraint.userDescription(tokenNameLookup)),
                cause,
                tokenNameLookup);
    }

    public UnableToValidateConstraintException(
            ErrorGqlStatusObject gqlStatusObject,
            ConstraintDescriptor constraint,
            Throwable cause,
            TokenNameLookup tokenNameLookup) {
        super(
                gqlStatusObject,
                constraint,
                Phase.VERIFICATION,
                format("Unable to validate constraint %s", constraint.userDescription(tokenNameLookup)),
                cause,
                tokenNameLookup);
    }

    @Override
    public String getUserMessage(TokenNameLookup tokenNameLookup) {
        return format("Unable to validate constraint %s", constraint.userDescription(tokenNameLookup));
    }
}
