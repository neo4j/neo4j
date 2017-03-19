/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.api.constraints;

import org.neo4j.kernel.api.schema.RelationshipPropertyDescriptor;
import org.neo4j.kernel.api.schema_new.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema_new.RelationTypeSchemaDescriptor;
import org.neo4j.kernel.api.schema_new.SchemaBoundary;
import org.neo4j.kernel.api.schema_new.SchemaDescriptor;

/**
 * Base class describing property constraint on relationships.
 */
public abstract class RelationshipPropertyConstraint implements PropertyConstraint
{
    protected final RelationshipPropertyDescriptor descriptor;

    public RelationshipPropertyConstraint( RelationshipPropertyDescriptor descriptor )
    {
        this.descriptor = descriptor;
    }

    @Override
    public final SchemaDescriptor descriptor()
    {
        return SchemaBoundary.map( descriptor );
    }

    public boolean matches( RelationshipPropertyDescriptor descriptor )
    {
        return this.descriptor.equals( descriptor );
    }

    public boolean matches( RelationTypeSchemaDescriptor other )
    {
        return other != null &&
                descriptor.getRelationshipTypeId() == other.getRelTypeId() &&
                descriptor.getPropertyId() == other.getPropertyIds()[0];
        // this is safe because we are replacing this class before introducing composite constraints
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        RelationshipPropertyConstraint that = (RelationshipPropertyConstraint) o;
        return this.descriptor.equals( that.descriptor );

    }

    @Override
    public int hashCode()
    {
        return descriptor.hashCode();
    }
}
