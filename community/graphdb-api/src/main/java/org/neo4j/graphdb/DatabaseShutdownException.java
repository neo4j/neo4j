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
package org.neo4j.graphdb;

import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorMessageHolder;
import org.neo4j.gqlstatus.HasGqlStatusInfo;
import org.neo4j.kernel.api.exceptions.Status;

public class DatabaseShutdownException extends RuntimeException implements Status.HasStatus, HasGqlStatusInfo {
    private static final String MESSAGE = "This database is shutdown.";
    private final ErrorGqlStatusObject gqlStatusObject;

    public DatabaseShutdownException() {
        super(MESSAGE);
        this.gqlStatusObject = null;
    }

    public DatabaseShutdownException(ErrorGqlStatusObject gqlStatusObject) {
        super(ErrorMessageHolder.getMessage(gqlStatusObject, MESSAGE));
        this.gqlStatusObject = gqlStatusObject;
    }

    public DatabaseShutdownException(Throwable cause) {
        super(MESSAGE, cause);
        this.gqlStatusObject = null;
    }

    public DatabaseShutdownException(ErrorGqlStatusObject gqlStatusObject, Throwable cause) {
        super(ErrorMessageHolder.getMessage(gqlStatusObject, MESSAGE), cause);
        this.gqlStatusObject = gqlStatusObject;
    }

    @Override
    public Status status() {
        return Status.General.DatabaseUnavailable;
    }

    @Override
    public String getOldMessage() {
        return MESSAGE;
    }

    @Override
    public ErrorGqlStatusObject gqlStatusObject() {
        return gqlStatusObject;
    }
}
