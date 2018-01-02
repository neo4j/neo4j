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
package org.neo4j.kernel.api.exceptions.schema;

import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.constraints.RelationshipPropertyExistenceConstraint;

public class RelationshipPropertyExistenceConstraintVerificationFailedKernelException
        extends ConstraintVerificationFailedKernelException
{
    private final RelationshipPropertyExistenceConstraint constraint;
    private final long relationshipId;

    public RelationshipPropertyExistenceConstraintVerificationFailedKernelException(
            RelationshipPropertyExistenceConstraint constraint, long relationshipId )
    {
        super( constraint );
        this.constraint = constraint;
        this.relationshipId = relationshipId;
    }

    @Override
    public String getUserMessage( TokenNameLookup tokenNameLookup )
    {
        return String.format( "Relationship(%s) with type `%s` has no value for property `%s`",
                relationshipId,
                tokenNameLookup.relationshipTypeGetName( constraint.relationshipType() ),
                tokenNameLookup.propertyKeyGetName( constraint.propertyKey() ) );
    }

    @Override
    public RelationshipPropertyExistenceConstraint constraint()
    {
        return constraint;
    }
}
