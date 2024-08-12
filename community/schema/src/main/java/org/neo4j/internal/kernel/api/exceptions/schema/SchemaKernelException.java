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
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.kernel.api.exceptions.Status;

/**
 * Signals that some constraint has been violated, for example a name containing invalid characters or length.
 */
public abstract class SchemaKernelException extends KernelException {
    public enum OperationContext {
        INDEX_CREATION,
        CONSTRAINT_CREATION
    }

    protected SchemaKernelException(Status statusCode, Throwable cause, String message, Object... parameters) {
        super(statusCode, cause, message, parameters);
    }

    protected SchemaKernelException(
            ErrorGqlStatusObject gqlStatusObject,
            Status statusCode,
            Throwable cause,
            String message,
            Object... parameters) {
        super(gqlStatusObject, statusCode, cause, message, parameters);
    }

    public SchemaKernelException(Status statusCode, String message, Throwable cause) {
        super(statusCode, cause, message);
    }

    public SchemaKernelException(
            ErrorGqlStatusObject gqlStatusObject, Status statusCode, String message, Throwable cause) {
        super(gqlStatusObject, statusCode, cause, message);
    }

    public SchemaKernelException(Status statusCode, String message) {
        super(statusCode, message);
    }

    public SchemaKernelException(ErrorGqlStatusObject gqlStatusObject, Status statusCode, String message) {
        super(gqlStatusObject, statusCode, message);
    }

    protected static String messageWithLabelAndPropertyName(
            TokenNameLookup tokenNameLookup, String formatString, SchemaDescriptor descriptor) {
        return String.format(formatString, descriptor.userDescription(tokenNameLookup));
    }
}
