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

import static java.lang.String.format;

import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.kernel.api.exceptions.Status;

public class TokenCapacityExceededKernelException extends SchemaKernelException {
    public TokenCapacityExceededKernelException(Throwable cause, String tokenType) {
        super(
                Status.Schema.TokenLimitReached,
                format("The maximum number of %ss available has been reached, no more can be created.", tokenType),
                cause);
    }

    public TokenCapacityExceededKernelException(
            ErrorGqlStatusObject gqlStatusObject, Throwable cause, String tokenType) {
        super(
                gqlStatusObject,
                Status.Schema.TokenLimitReached,
                format("The maximum number of %ss available has been reached, no more can be created.", tokenType),
                cause);
    }
}
