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
package org.neo4j.dbms.api;

import org.neo4j.annotations.api.PublicApi;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.kernel.api.exceptions.Status;

/**
 * A {@link DatabaseManagementService} tried to create a new database, but a database with that name already exists.
 */
@PublicApi
public class DatabaseExistsException extends DatabaseManagementException {
    public DatabaseExistsException() {
        super();
    }

    public DatabaseExistsException(ErrorGqlStatusObject gqlStatusObject) {
        super(gqlStatusObject);
    }

    public DatabaseExistsException(String message) {
        super(message);
    }

    public DatabaseExistsException(ErrorGqlStatusObject gqlStatusObject, String message) {
        super(gqlStatusObject, message);
    }

    public DatabaseExistsException(String message, Throwable cause) {
        super(message, cause);
    }

    public DatabaseExistsException(ErrorGqlStatusObject gqlStatusObject, String message, Throwable cause) {
        super(gqlStatusObject, message, cause);
    }

    public DatabaseExistsException(Throwable cause) {
        super(cause);
    }

    public DatabaseExistsException(ErrorGqlStatusObject gqlStatusObject, Throwable cause) {
        super(gqlStatusObject, cause);
    }

    @Override
    public Status status() {
        return Status.Database.ExistingDatabaseFound;
    }
}
