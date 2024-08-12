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

import static java.util.Objects.requireNonNull;

import org.neo4j.common.EntityType;
import org.neo4j.exceptions.KernelException;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.kernel.api.exceptions.Status;

public class EntityAlreadyExistsException extends KernelException {
    private final EntityType entityType;
    private final String entityId;

    public EntityAlreadyExistsException(EntityType entityType, String entityId) {
        super(Status.Statement.EntityNotFound, "%s %s already exists.", entityType.name(), entityId);
        this.entityType = requireNonNull(entityType);
        this.entityId = requireNonNull(entityId);
    }

    public EntityAlreadyExistsException(ErrorGqlStatusObject gqlStatusObject, EntityType entityType, String entityId) {
        super(gqlStatusObject, Status.Statement.EntityNotFound, "%s %s already exists.", entityType.name(), entityId);

        this.entityType = requireNonNull(entityType);
        this.entityId = requireNonNull(entityId);
    }

    public EntityType entityType() {
        return entityType;
    }

    public String entityId() {
        return entityId;
    }
}
