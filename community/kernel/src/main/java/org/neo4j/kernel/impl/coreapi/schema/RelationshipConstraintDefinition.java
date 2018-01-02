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
package org.neo4j.kernel.impl.coreapi.schema;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;

import static java.util.Objects.requireNonNull;

abstract class RelationshipConstraintDefinition extends PropertyConstraintDefinition
{
    protected final RelationshipType relationshipType;

    protected RelationshipConstraintDefinition( InternalSchemaActions actions, RelationshipType relationshipType,
            String propertyKey )
    {
        super( actions, propertyKey );
        this.relationshipType = requireNonNull( relationshipType );
    }

    @Override
    public Label getLabel()
    {
        assertInUnterminatedTransaction();
        throw new IllegalStateException( "Constraint is associated with relationships" );
    }

    @Override
    public RelationshipType getRelationshipType()
    {
        assertInUnterminatedTransaction();
        return relationshipType;
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
        RelationshipConstraintDefinition that = (RelationshipConstraintDefinition) o;
        return relationshipType.name().equals( that.relationshipType.name() ) && propertyKey.equals( that.propertyKey );

    }

    @Override
    public int hashCode()
    {
        return 31 * relationshipType.name().hashCode() + propertyKey.hashCode();
    }
}
