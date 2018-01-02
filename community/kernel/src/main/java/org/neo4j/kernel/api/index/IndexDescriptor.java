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
package org.neo4j.kernel.api.index;

import org.neo4j.kernel.api.TokenNameLookup;

import static java.lang.String.format;

/**
 * Description of a single index as needed by the {@link org.neo4j.kernel.impl.api.index.IndexProxy} cake
 * <p>
 * This is a IndexContext cake level representation of {@link org.neo4j.kernel.impl.store.record.IndexRule}
 */
public class IndexDescriptor
{
    private final int labelId;
    private final int propertyKeyId;

    public IndexDescriptor( int labelId, int propertyKeyId )
    {
        this.labelId = labelId;
        this.propertyKeyId = propertyKeyId;
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj )
        {
            return true;
        }
        if ( obj != null && getClass() == obj.getClass() )
        {
            IndexDescriptor that = (IndexDescriptor) obj;
            return this.labelId == that.labelId &&
                    this.propertyKeyId == that.propertyKeyId;
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        int result = labelId;
        result = 31 * result + propertyKeyId;
        return result;
    }

    public int getLabelId()
    {
        return labelId;
    }

    public int getPropertyKeyId()
    {
        return propertyKeyId;
    }

    @Override
    public String toString()
    {
        return format( ":label[%d](property[%d])", labelId, propertyKeyId );
    }

    public String userDescription( TokenNameLookup tokenNameLookup )
    {
        return format( ":%s(%s)",
                tokenNameLookup.labelGetName( labelId ), tokenNameLookup.propertyKeyGetName( propertyKeyId ) );
    }
}
