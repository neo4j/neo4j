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

import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.schema.ConstraintType;

import static java.lang.String.format;

public class RelationshipPropertyExistenceConstraintDefinition extends RelationshipConstraintDefinition
{
    public RelationshipPropertyExistenceConstraintDefinition( InternalSchemaActions actions,
            RelationshipType relationshipType, String propertyKey )
    {
        super( actions, relationshipType, propertyKey );
    }

    @Override
    public void drop()
    {
        assertInUnterminatedTransaction();
        actions.dropRelationshipPropertyExistenceConstraint( relationshipType, propertyKey );
    }

    @Override
    public ConstraintType getConstraintType()
    {
        return ConstraintType.RELATIONSHIP_PROPERTY_EXISTENCE;
    }

    @Override
    public String toString()
    {
        return format( "ON ()-[%1$s:%2$s]-() ASSERT exists(%1$s.%3$s)",
                relationshipType.name().toLowerCase(), relationshipType.name(), propertyKey );
    }
}
