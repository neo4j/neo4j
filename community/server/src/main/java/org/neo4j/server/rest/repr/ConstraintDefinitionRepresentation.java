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
package org.neo4j.server.rest.repr;

import org.neo4j.function.Function;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.ConstraintType;

import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.server.rest.repr.RepresentationType.CONSTRAINT_DEFINITION;

public class ConstraintDefinitionRepresentation extends MappingRepresentation
{
    protected final ConstraintDefinition constraintDefinition;

    public ConstraintDefinitionRepresentation( ConstraintDefinition constraintDefinition )
    {
        super( CONSTRAINT_DEFINITION );
        this.constraintDefinition = constraintDefinition;
    }

    @Override
    protected void serialize( MappingSerializer serializer )
    {
        switch ( constraintDefinition.getConstraintType() )
        {
        case UNIQUENESS:
        case NODE_PROPERTY_EXISTENCE:
            serializer.putString( "label", constraintDefinition.getLabel().name() );
            break;
        case RELATIONSHIP_PROPERTY_EXISTENCE:
            serializer.putString( "relationshipType", constraintDefinition.getRelationshipType().name() );
            break;
        default:
            throw new IllegalStateException( "Unknown constraint type:" + constraintDefinition.getConstraintType() );
        }

        ConstraintType type = constraintDefinition.getConstraintType();
        serializer.putString( "type", type.name() );
        serialize( constraintDefinition, serializer );
    }

    protected void serialize( ConstraintDefinition constraintDefinition, MappingSerializer serializer )
    {
        Function<String, Representation> converter = new Function<String, Representation>()
        {
            @Override
            public Representation apply( String propertyKey )
            {
                return ValueRepresentation.string( propertyKey );
            }
        };
        Iterable<Representation> propertyKeyRepresentations = map( converter, constraintDefinition.getPropertyKeys() );
        serializer.putList( "property_keys",
            new ListRepresentation( RepresentationType.STRING, propertyKeyRepresentations ) );
    }
}
