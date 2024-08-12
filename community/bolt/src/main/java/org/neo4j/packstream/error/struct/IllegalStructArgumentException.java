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
package org.neo4j.packstream.error.struct;

import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.packstream.error.reader.PackstreamReaderException;

public class IllegalStructArgumentException extends PackstreamStructException {
    private final String fieldName;

    public IllegalStructArgumentException(String fieldName, PackstreamReaderException cause) {
        // In case of Packstream exceptions, we'll copy the cause message in order to make it available to the client
        // as well - in all other cases this information will be suppressed as we do not wish to accidentally leak any
        // information that could provide information about internal processes
        super(String.format("Illegal value for field \"%s\": %s", fieldName, cause.getMessage()), cause);
        this.fieldName = fieldName;
    }

    public IllegalStructArgumentException(
            ErrorGqlStatusObject gqlStatusObject, String fieldName, PackstreamReaderException cause) {
        // In case of Packstream exceptions, we'll copy the cause message in order to make it available to the client
        // as well - in all other cases this information will be suppressed as we do not wish to accidentally leak any
        // information that could provide information about internal processes
        super(String.format("Illegal value for field \"%s\": %s", fieldName, cause.getMessage()), cause);
        this.fieldName = fieldName;
    }

    public IllegalStructArgumentException(String fieldName, Throwable cause) {
        super(String.format("Illegal value for field \"%s\"", fieldName), cause);
        this.fieldName = fieldName;
    }

    public IllegalStructArgumentException(ErrorGqlStatusObject gqlStatusObject, String fieldName, Throwable cause) {
        super(gqlStatusObject, String.format("Illegal value for field \"%s\"", fieldName), cause);

        this.fieldName = fieldName;
    }

    public IllegalStructArgumentException(String fieldName, String message, Throwable cause) {
        super(String.format("Illegal value for field \"%s\": %s", fieldName, message), cause);
        this.fieldName = fieldName;
    }

    public IllegalStructArgumentException(
            ErrorGqlStatusObject gqlStatusObject, String fieldName, String message, Throwable cause) {
        super(gqlStatusObject, String.format("Illegal value for field \"%s\": %s", fieldName, message), cause);

        this.fieldName = fieldName;
    }

    public IllegalStructArgumentException(String fieldName, String message) {
        this(fieldName, message, null);
    }

    public IllegalStructArgumentException(ErrorGqlStatusObject gqlStatusObject, String fieldName, String message) {
        this(gqlStatusObject, fieldName, message, null);
    }

    public String getFieldName() {
        return this.fieldName;
    }

    @Override
    public Status status() {
        // When we're wrapping another Packstream related exception which bears its own status, we'll take over the
        // original status code instead.
        var cause = this.getCause();
        if (cause instanceof PackstreamReaderException && cause instanceof Status.HasStatus) {
            return ((Status.HasStatus) cause).status();
        }

        return Status.Request.Invalid;
    }
}
