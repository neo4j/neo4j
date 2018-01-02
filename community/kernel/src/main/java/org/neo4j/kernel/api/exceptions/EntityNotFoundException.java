/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.api.exceptions;

import org.neo4j.kernel.api.EntityType;

public class EntityNotFoundException extends KernelException
{
    private final EntityType entityType;
    private final long entityId;

    public EntityNotFoundException( EntityType entityType, long entityId )
    {
        super( Status.Statement.EntityNotFound, "Unable to load %s with id %s.", entityType.name(), entityId );
        this.entityType = entityType;
        this.entityId = entityId;
    }

    public EntityType entityType()
    {
        if ( entityType == null )
        {
            throw new IllegalStateException( "No entity type specified for this exception", this );
        }
        return entityType;
    }

    public long entityId()
    {
        if ( entityId == -1 )
        {
            throw new IllegalStateException( "No entity id specified for this exception", this );
        }
        return entityId;
    }
}
