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
package org.neo4j.kernel.impl.store.counts.keys;

import static org.neo4j.kernel.impl.util.IdPrettyPrinter.label;
import static org.neo4j.kernel.impl.util.IdPrettyPrinter.propertyKey;

abstract class IndexKey implements CountsKey
{
    private final int labelId;
    private final int propertyKeyId;
    private final CountsKeyType type;

    IndexKey( int labelId, int propertyKeyId, CountsKeyType type )
    {
        this.labelId = labelId;
        this.propertyKeyId = propertyKeyId;
        this.type = type;
    }

    public int labelId()
    {
        return labelId;
    }

    public int propertyKeyId()
    {
        return propertyKeyId;
    }

    @Override
    public String toString()
    {
        return String.format( "IndexKey[%s (%s {%s})]", type.name(), label( labelId ), propertyKey( propertyKeyId ) );
    }

    @Override
    public CountsKeyType recordType()
    {
        return type;
    }


    @Override
    public int hashCode()
    {
        int result = labelId;
        result = 31 * result + propertyKeyId;
        result = 31 * result + type.hashCode();
        return result;
    }

    @Override
    public boolean equals( Object other )
    {
        if ( this == other )
        {
            return true;
        }
        if ( other == null || getClass() != other.getClass() )
        {
            return false;
        }

        IndexKey indexKey = (IndexKey) other;
        return labelId == indexKey.labelId && propertyKeyId == indexKey.propertyKeyId && type == indexKey.type;
    }
}
