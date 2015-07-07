/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.store;

import org.neo4j.kernel.api.constraints.RelationshipPropertyConstraint;

public abstract class RelationshipPropertyConstraintRule extends PropertyConstraintRule
{
    public RelationshipPropertyConstraintRule( long id, int relationshipType, Kind kind )
    {
        super( id, NOT_INITIALIZED, relationshipType, kind );
    }

    @Override
    public final int getLabel()
    {
        throw new IllegalStateException( "Constraint rule is associated with relationships" );
    }

    @Override
    public final int getRelationshipType()
    {
        return getLabelOrRelationshipType();
    }

    @Override
    public abstract RelationshipPropertyConstraint toConstraint();
}
