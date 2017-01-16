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
package org.neo4j.kernel.impl.store.record;

import org.neo4j.kernel.api.constraints.RelationshipPropertyConstraint;
import org.neo4j.kernel.api.schema.RelationshipPropertyDescriptor;

public abstract class RelationshipPropertyConstraintRule extends PropertyConstraintRule
{
    public RelationshipPropertyConstraintRule( long id, RelationshipPropertyDescriptor descriptor, Kind kind )
    {
        super( id, kind, descriptor );
    }

    @Override
    public final int getLabel()
    {
        throw new IllegalStateException( "Constraint rule is associated with relationships" );
    }

    public final RelationshipPropertyDescriptor descriptor()
    {
        return (RelationshipPropertyDescriptor) descriptor;
    }

    @Override
    public final int getRelationshipType()
    {
        return descriptor().getRelationshipTypeId();
    }

    @Override
    public abstract RelationshipPropertyConstraint toConstraint();
}
