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

import static java.lang.String.format;

public class RelationshipPropertyExistenceConstraintViolationKernelException extends ConstraintViolationKernelException
{
    private final int relationshipTypeId;
    private final int propertyKeyId;
    private final long relationshipId;

    public RelationshipPropertyExistenceConstraintViolationKernelException( int relationshipTypeId, int propertyKeyId,
            long relationshipId )
    {
        super( "Relationship %d with type %d must have the property %d", relationshipId, relationshipTypeId,
                propertyKeyId );
        this.relationshipTypeId = relationshipTypeId;
        this.propertyKeyId = propertyKeyId;
        this.relationshipId = relationshipId;
    }

    @Override
    public String getUserMessage( TokenNameLookup tokenNameLookup )
    {
        return format( "Relationship %d with type \"%s\" must have the property \"%s\" due to a constraint", relationshipId,
                tokenNameLookup.relationshipTypeGetName( relationshipTypeId ),
                tokenNameLookup.propertyKeyGetName( propertyKeyId ) );
    }

    public int relationshipTypeId()
    {
        return relationshipTypeId;
    }

    public int propertyKeyId()
    {
        return propertyKeyId;
    }
}
