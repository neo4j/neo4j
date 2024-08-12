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
package org.neo4j.internal.kernel.api.exceptions;

import org.neo4j.exceptions.KernelException;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.kernel.api.exceptions.Status;

/**
 * A {@link KernelException} thrown by a {@link org.neo4j.storageengine.api.StorageEngine} if it failed to apply a transaction.
 */
public class TransactionApplyKernelException extends KernelException {
    public TransactionApplyKernelException(Throwable cause, String message, Object... parameters) {
        super(Status.General.UnknownError, cause, message, parameters);
    }

    public TransactionApplyKernelException(
            ErrorGqlStatusObject gqlStatusObject, Throwable cause, String message, Object... parameters) {
        super(gqlStatusObject, Status.General.UnknownError, cause, message, parameters);
    }
}
