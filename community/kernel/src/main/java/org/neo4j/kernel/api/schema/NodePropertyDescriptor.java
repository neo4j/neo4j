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
public class NodePropertyDescriptor extends EntityPropertyDescriptor
{
    public NodePropertyDescriptor( int labelId, int propertyKeyId )
    {
        super(labelId, propertyKeyId);
    }

    public int getLabelId()
    {
        return getEntityId();
    }

    public boolean isComposite()
    {
        return false;
    }

    @Override
    public String entityNameText( TokenNameLookup tokenNameLookup )
    {
        return tokenNameLookup.labelGetName( getEntityId() );
    }

    public EntityType entityType()
    {
        return EntityType.NODE;
    }

    public NodePropertyDescriptor descriptor()
    {
        return this;
    }
}
