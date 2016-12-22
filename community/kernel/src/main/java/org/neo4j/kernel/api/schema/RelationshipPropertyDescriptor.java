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
package org.neo4j.kernel.api.schema;

import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.storageengine.api.EntityType;

/**
 * Description of a combination of a relationship type and one property.
 */
public class RelationshipPropertyDescriptor extends EntityPropertyDescriptor
{
    public RelationshipPropertyDescriptor( int relationshipTypeId, int propertyKeyId )
    {
        super(relationshipTypeId, propertyKeyId);
    }

    public int getRelationshipTypeId()
    {
        return getEntityId();
    }

    @Override
    public String entityNameText( TokenNameLookup tokenNameLookup )
    {
        return tokenNameLookup.relationshipTypeGetName( getEntityId() );
    }

    public EntityType entityType()
    {
        return EntityType.RELATIONSHIP;
    }
}
