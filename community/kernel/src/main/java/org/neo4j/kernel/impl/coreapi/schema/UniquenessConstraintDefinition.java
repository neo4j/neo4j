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
import org.neo4j.graphdb.schema.ConstraintType;

import static java.lang.String.format;

public class UniquenessConstraintDefinition extends NodeConstraintDefinition
{
    public UniquenessConstraintDefinition( InternalSchemaActions actions, Label label, String propertyKey )
    {
        super( actions, label, propertyKey );
    }

    @Override
    public void drop()
    {
        assertInUnterminatedTransaction();
        actions.dropPropertyUniquenessConstraint( label, propertyKey );
    }

    @Override
    public ConstraintType getConstraintType()
    {
        assertInUnterminatedTransaction();
        return ConstraintType.UNIQUENESS;
    }

    @Override
    public String toString()
    {
        return format( "ON (%1$s:%2$s) ASSERT %1$s.%3$s IS UNIQUE",
                label.name().toLowerCase(), label.name(), propertyKey );
    }
}
