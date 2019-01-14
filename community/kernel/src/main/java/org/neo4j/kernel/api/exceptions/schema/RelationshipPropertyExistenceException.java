/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.neo4j.internal.kernel.api.TokenNameLookup;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.kernel.api.schema.RelationTypeSchemaDescriptor;
import org.neo4j.kernel.api.schema.constaints.ConstraintDescriptorFactory;

import static java.lang.String.format;

public class RelationshipPropertyExistenceException extends ConstraintValidationException
{
    private final RelationTypeSchemaDescriptor schema;
    private final long relationshipId;

    public RelationshipPropertyExistenceException(
            RelationTypeSchemaDescriptor schema, ConstraintValidationException.Phase phase, long relationshipId )
    {
        super( ConstraintDescriptorFactory.existsForSchema( schema ),
                phase, format( "Relationship(%s)", relationshipId ) );
        this.schema = schema;
        this.relationshipId = relationshipId;
    }

    @Override
    public String getUserMessage( TokenNameLookup tokenNameLookup )
    {
        return format( "Relationship(%s) with type `%s` must have the property `%s`",
                relationshipId,
                tokenNameLookup.relationshipTypeGetName( schema.getRelTypeId() ),
                tokenNameLookup.propertyKeyGetName( schema.getPropertyId() ) );
    }
}
