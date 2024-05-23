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

import org.neo4j.common.TokenNameLookup;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.internal.schema.SchemaDescriptorSupplier;
import org.neo4j.kernel.api.exceptions.Status;

public class DropConstraintFailureException extends SchemaKernelException {
    private final SchemaDescriptorSupplier constraint;
    private final String nameOrSchema;

    public DropConstraintFailureException(SchemaDescriptorSupplier constraint, Throwable cause) {
        super(Status.Schema.ConstraintDropFailed, cause, "Unable to drop constraint: " + cause.getMessage());
        this.constraint = constraint;
        this.nameOrSchema = null;
    }

    public DropConstraintFailureException(String nameOrSchema, Throwable cause) {
        // nameOrSchema is just 'name' or 'on schema'
        super(
                Status.Schema.ConstraintDropFailed,
                cause,
                "Unable to drop constraint `" + nameOrSchema + "`: " + cause.getMessage());
        this.nameOrSchema = nameOrSchema;
        this.constraint = null;
    }

    public DropConstraintFailureException(SchemaDescriptorSupplier constraint, String message) {
        super(Status.Schema.ConstraintDropFailed, "Unable to drop constraint: " + message);
        this.constraint = constraint;
        this.nameOrSchema = null;
    }

    @Override
    public String getUserMessage(TokenNameLookup tokenNameLookup) {
        String message;
        if (constraint != null) {
            message = "Unable to drop constraint on " + constraint.userDescription(tokenNameLookup) + ": ";

        } else if (nameOrSchema != null) {
            message = "Unable to drop constraint `" + nameOrSchema + "`: ";
        } else {
            return getMessage();
        }

        Throwable cause = getCause();
        if (cause instanceof KernelException exception) {
            message += exception.getUserMessage(tokenNameLookup);
        } else {
            message += cause.getMessage();
        }

        return message;
    }
}
