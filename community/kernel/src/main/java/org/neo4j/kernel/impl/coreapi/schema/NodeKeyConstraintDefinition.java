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

import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.graphdb.schema.IndexDefinition;

import static java.lang.String.format;

public class NodeKeyConstraintDefinition extends NodeConstraintDefinition
{
    public NodeKeyConstraintDefinition( InternalSchemaActions actions, IndexDefinition indexDefinition )
    {
        super( actions, indexDefinition );
    }

    @Override
    public void drop()
    {
        assertInUnterminatedTransaction();
        actions.dropNodeKeyConstraint( label, propertyKeys );
    }

    @Override
    public ConstraintType getConstraintType()
    {
        assertInUnterminatedTransaction();
        return ConstraintType.NODE_KEY;
    }

    @Override
    public String toString()
    {
        return format( "ON (%1$s:%2$s) ASSERT %3$s IS NODE KEY",
                label.name().toLowerCase(), label.name(), propertyText() );
    }
}
