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
package org.neo4j.kernel.api.schema_new.constaints;

import org.neo4j.kernel.api.constraints.NodePropertyConstraint;
import org.neo4j.kernel.api.constraints.NodePropertyExistenceConstraint;
import org.neo4j.kernel.api.constraints.PropertyConstraint;
import org.neo4j.kernel.api.constraints.RelationshipPropertyConstraint;
import org.neo4j.kernel.api.constraints.RelationshipPropertyExistenceConstraint;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.schema.IndexDescriptorFactory;
import org.neo4j.kernel.api.schema.RelationshipPropertyDescriptor;
import org.neo4j.kernel.api.schema_new.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema_new.RelationTypeSchemaDescriptor;
import org.neo4j.kernel.api.schema_new.SchemaComputer;

/**
 * This class represents the boundary of where new constraint descriptors are converted to old constraints. This class
 * should disappear once the old constraints are no longer used.
 */
public class ConstraintBoundary
{
    public static PropertyConstraint map( ConstraintDescriptor descriptor )
    {
        return descriptor.schema().computeWith( new BoundaryTransformer( descriptor ) );
    }

    public static NodePropertyConstraint mapNode( ConstraintDescriptor descriptor )
    {
        return (NodePropertyConstraint) descriptor.schema().computeWith( new BoundaryTransformer( descriptor ) );
    }

    public static RelationshipPropertyConstraint mapRelationship( ConstraintDescriptor descriptor )
    {
        return (RelationshipPropertyConstraint) descriptor.schema()
                                                          .computeWith( new BoundaryTransformer( descriptor ) );
    }

    static class BoundaryTransformer implements SchemaComputer<PropertyConstraint>
    {
        private final ConstraintDescriptor descriptor;

        BoundaryTransformer( ConstraintDescriptor descriptor )
        {
            this.descriptor = descriptor;
        }

        @Override
        public PropertyConstraint computeSpecific( LabelSchemaDescriptor schema )
        {
            switch ( descriptor.type() ) {
            case UNIQUE:
                return new UniquenessConstraint( IndexDescriptorFactory.getNodePropertyDescriptor(
                        schema.getLabelId(), schema.getPropertyId() ) );

            case EXISTS:
                return new NodePropertyExistenceConstraint( IndexDescriptorFactory.getNodePropertyDescriptor(
                        schema.getLabelId(), schema.getPropertyId() ) );

            default:
                throw new UnsupportedOperationException( "Although we cannot get here, this has not been implemented." );
            }
        }

        @Override
        public PropertyConstraint computeSpecific( RelationTypeSchemaDescriptor schema )
        {
            return new RelationshipPropertyExistenceConstraint( new RelationshipPropertyDescriptor(
                    schema.getRelTypeId(), schema.getPropertyId() ) );
        }
    }
}
