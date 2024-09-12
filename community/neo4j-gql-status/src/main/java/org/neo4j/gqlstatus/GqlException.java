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
package org.neo4j.gqlstatus;

import org.neo4j.annotations.api.PublicApi;

@PublicApi
public abstract class GqlException extends Exception implements ErrorGqlStatusObject {
    private final ErrorGqlStatusObject innerGqlStatusObject;
    private final String oldMessage;

    protected GqlException(String message, Throwable cause) {
        super(message, cause);
        this.innerGqlStatusObject = null;
        this.oldMessage = message;
    }

    protected GqlException(ErrorGqlStatusObject gqlStatusObject, String message, Throwable cause) {
        super(ErrorMessageHolder.getMessage(gqlStatusObject, message), cause);
        this.innerGqlStatusObject = gqlStatusObject;
        this.oldMessage = message;
    }

    protected GqlException(String message) {
        this(message, null);
    }

    protected GqlException(ErrorGqlStatusObject gqlStatusObject, String message) {
        this(gqlStatusObject, message, null);
    }

    @Override
    public String legacyMessage() {
        return oldMessage;
    }

    @Override
    public ErrorGqlStatusObject gqlStatusObject() {
        return innerGqlStatusObject;
    }
}
