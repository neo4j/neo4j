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
package org.neo4j.fabric.executor;

import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.kernel.api.exceptions.Status;

/**
 * A Fabric exception that is not the primary cause of a failure.
 * <p>
 * Fabric concurrent parts are not independent. A failure in one record stream can cause secondary failures in other streams for instance
 * if the streams are using the same transaction. Due to the concurrent nature, there might be a race between the primary exception and secondary exceptions.
 * The user should be always presented with the primary exception.
 */
public class FabricSecondaryException extends FabricException {
    private final FabricException primaryException;

    public FabricSecondaryException(
            Status statusCode, String message, Throwable cause, FabricException primaryException) {
        super(statusCode, message, cause);
        this.primaryException = primaryException;
    }

    public FabricSecondaryException(
            ErrorGqlStatusObject gqlStatusObject,
            Status statusCode,
            String message,
            Throwable cause,
            FabricException primaryException) {
        super(gqlStatusObject, statusCode, message, cause);

        this.primaryException = primaryException;
    }

    public FabricException getPrimaryException() {
        return primaryException;
    }
}
