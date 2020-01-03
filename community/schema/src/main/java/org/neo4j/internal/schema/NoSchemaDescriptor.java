/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.internal.schema;

import org.neo4j.common.EntityType;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.lock.ResourceType;

/**
 * A schema descriptor sentinel used for signalling the absence of a real schema descriptor.
 * <p>
 * The instance is acquired via the {@link SchemaDescriptor#noSchema()} method.
 */
class NoSchemaDescriptor implements SchemaDescriptor
{
    static final SchemaDescriptor NO_SCHEMA = new NoSchemaDescriptor();

    private NoSchemaDescriptor()
    {
    }

    @Override
    public boolean isLabelSchemaDescriptor()
    {
        return false;
    }

    @Override
    public LabelSchemaDescriptor asLabelSchemaDescriptor()
    {
        throw new IllegalStateException( "NO_SCHEMA cannot be cast to a LabelSchemaDescriptor." );
    }

    @Override
    public boolean isRelationshipTypeSchemaDescriptor()
    {
        return false;
    }

    @Override
    public RelationTypeSchemaDescriptor asRelationshipTypeSchemaDescriptor()
    {
        throw new IllegalStateException( "NO_SCHEMA cannot be cast to a RelationTypeSchemaDescriptor." );
    }

    @Override
    public boolean isFulltextSchemaDescriptor()
    {
        return false;
    }

    @Override
    public FulltextSchemaDescriptor asFulltextSchemaDescriptor()
    {
        throw new IllegalStateException( "NO_SCHEMA cannot be cast to a FulltextSchemaDescriptor." );
    }

    @Override
    public boolean isAffected( long[] entityIds )
    {
        return false;
    }

    @Override
    public void processWith( SchemaProcessor processor )
    {
    }

    @Override
    public String userDescription( TokenNameLookup tokenNameLookup )
    {
        return "NO_SCHEMA";
    }

    @Override
    public int[] getPropertyIds()
    {
        return new int[0];
    }

    @Override
    public int[] getEntityTokenIds()
    {
        return new int[0];
    }

    @Override
    public ResourceType keyType()
    {
        return null;
    }

    @Override
    public EntityType entityType()
    {
        return null;
    }

    @Override
    public PropertySchemaType propertySchemaType()
    {
        return null;
    }

    @Override
    public SchemaDescriptor schema()
    {
        return null;
    }
}
