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
package org.neo4j.internal.kernel.api.exceptions.schema;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.kernel.api.exceptions.Status;

public class CreateConstraintFailureException extends SchemaKernelException {
    private final ConstraintDescriptor constraint;

    private final String cause;

    public CreateConstraintFailureException(ConstraintDescriptor constraint, Throwable cause) {
        super(
                Status.Schema.ConstraintCreationFailed,
                cause,
                "Unable to create constraint %s: %s",
                constraint,
                cause.getMessage());
        this.constraint = constraint;
        this.cause = null;
    }

    public CreateConstraintFailureException(ConstraintDescriptor constraint, String cause) {
        super(Status.Schema.ConstraintCreationFailed, null, "Unable to create constraint %s: %s", constraint, cause);
        this.constraint = constraint;
        this.cause = cause;
    }

    public ConstraintDescriptor constraint() {
        return constraint;
    }

    @Override
    public String getUserMessage(TokenNameLookup tokenNameLookup) {
        final var sb = new StringBuilder("Unable to create ").append(constraint.userDescription(tokenNameLookup));
        if (getCause() instanceof KernelException kernelCause) {
            sb.append(':').append(System.lineSeparator()).append(kernelCause.getUserMessage(tokenNameLookup));
        } else if (cause != null) {
            sb.append(':').append(System.lineSeparator()).append(cause);
        }
        return sb.append(". Note that only the first found violation is shown.").toString();
    }
}
