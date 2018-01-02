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
import org.neo4j.kernel.api.TokenNameLookup;

import static java.lang.String.format;

public class PropertyNotFoundException extends KernelException
{
    private final String entity;
    private final int propertyKeyId;

    public PropertyNotFoundException( int propertyKeyId, EntityType entityType, long entityId )
    {
        this( entityType == EntityType.GRAPH ? "GraphProperties" : entityType.name() + "[" + entityId + "]",
              propertyKeyId );
    }

    private PropertyNotFoundException( String entity, int propertyKeyId )
    {
        super( Status.Statement.NoSuchProperty, "%s has no property with propertyKeyId=%s.", entity, propertyKeyId );
        this.entity = entity;
        this.propertyKeyId = propertyKeyId;
    }

    @Override
    public String getUserMessage( TokenNameLookup tokenNameLookup )
    {
        return format( "%s has no property with propertyKey=\"%s\".", entity,
                       tokenNameLookup.propertyKeyGetName( propertyKeyId ) );
    }
}
