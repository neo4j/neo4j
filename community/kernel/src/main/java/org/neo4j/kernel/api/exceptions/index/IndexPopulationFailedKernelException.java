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
package org.neo4j.kernel.api.exceptions.index;

import org.neo4j.exceptions.KernelException;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.kernel.api.exceptions.Status;

public class IndexPopulationFailedKernelException extends KernelException {
    private static final String FORMAT_MESSAGE = "Failed to populate index %s";

    public IndexPopulationFailedKernelException(String indexUserDescription, Throwable cause) {
        super(Status.Schema.IndexCreationFailed, cause, FORMAT_MESSAGE, indexUserDescription);
    }

    public IndexPopulationFailedKernelException(
            ErrorGqlStatusObject gqlStatusObject, String indexUserDescription, Throwable cause) {
        super(gqlStatusObject, Status.Schema.IndexCreationFailed, cause, FORMAT_MESSAGE, indexUserDescription);
    }

    public IndexPopulationFailedKernelException(String indexUserDescription, String message) {
        super(Status.Schema.IndexCreationFailed, FORMAT_MESSAGE + ", due to " + message, indexUserDescription);
    }

    public IndexPopulationFailedKernelException(
            ErrorGqlStatusObject gqlStatusObject, String indexUserDescription, String message) {
        super(
                gqlStatusObject,
                Status.Schema.IndexCreationFailed,
                FORMAT_MESSAGE + ", due to " + message,
                indexUserDescription);
    }
}
