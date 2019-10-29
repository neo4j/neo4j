/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.internal.schema.constraints;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.RelationTypeSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;

import static org.neo4j.internal.schema.ConstraintType.EXISTS;
import static org.neo4j.internal.schema.ConstraintType.UNIQUE;
import static org.neo4j.internal.schema.ConstraintType.UNIQUE_EXISTS;

public class ConstraintDescriptorFactory
{
    private ConstraintDescriptorFactory()
    {
    }

    public static NodeExistenceConstraintDescriptor existsForLabel( int labelId, int... propertyIds )
    {
        return new ConstraintDescriptorImplementation( EXISTS, SchemaDescriptor.forLabel( labelId, propertyIds ) );
    }

    public static RelExistenceConstraintDescriptor existsForRelType( int relTypeId, int... propertyIds )
    {
        return new ConstraintDescriptorImplementation( EXISTS, SchemaDescriptor.forRelType( relTypeId, propertyIds ) );
    }

    public static UniquenessConstraintDescriptor uniqueForLabel( int labelId, int... propertyIds )
    {
        return uniqueForSchema( SchemaDescriptor.forLabel( labelId, propertyIds ) );
    }

    public static NodeKeyConstraintDescriptor nodeKeyForLabel( int labelId, int... propertyIds )
    {
        return nodeKeyForSchema( SchemaDescriptor.forLabel( labelId, propertyIds ) );
    }

    public static ConstraintDescriptor existsForSchema( SchemaDescriptor schema )
    {
        ConstraintDescriptorImplementation constraint = new ConstraintDescriptorImplementation( EXISTS, schema );
        if ( schema.isLabelSchemaDescriptor() )
        {
            return constraint.asNodePropertyExistenceConstraint();
        }
        if ( schema.isRelationshipTypeSchemaDescriptor() )
        {
            return constraint.asRelationshipPropertyExistenceConstraint();
        }
        throw new UnsupportedOperationException(
                "Cannot create existence constraint for schema '" + schema.userDescription( TokenNameLookup.idTokenNameLookup ) + "'." );
    }

    public static NodeExistenceConstraintDescriptor existsForSchema( LabelSchemaDescriptor schema )
    {
        return new ConstraintDescriptorImplementation( EXISTS, schema );
    }

    public static RelExistenceConstraintDescriptor existsForSchema( RelationTypeSchemaDescriptor schema )
    {
        return new ConstraintDescriptorImplementation( EXISTS, schema );
    }

    public static UniquenessConstraintDescriptor uniqueForSchema( SchemaDescriptor schema )
    {
        return new ConstraintDescriptorImplementation( UNIQUE, schema );
    }

    public static NodeKeyConstraintDescriptor nodeKeyForSchema( SchemaDescriptor schema )
    {
        return new ConstraintDescriptorImplementation( UNIQUE_EXISTS, schema ).asNodeKeyConstraint();
    }
}
