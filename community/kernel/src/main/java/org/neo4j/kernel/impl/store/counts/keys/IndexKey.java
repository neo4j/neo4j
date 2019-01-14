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
package org.neo4j.kernel.impl.store.counts.keys;

abstract class IndexKey implements CountsKey
{
    private final long indexId;
    private final CountsKeyType type;

    IndexKey( long indexId, CountsKeyType type )
    {
        this.indexId = indexId;
        this.type = type;
    }

    public long indexId()
    {
        return indexId;
    }

    @Override
    public String toString()
    {
        return String.format( "IndexKey[%s:%d]", type.name(), indexId );
    }

    @Override
    public CountsKeyType recordType()
    {
        return type;
    }

    @Override
    public int hashCode()
    {
        return 31 * (int) indexId + type.hashCode();
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
        return ((IndexKey) other).indexId() == indexId;
    }

    @Override
    public int compareTo( CountsKey other )
    {
        if ( other instanceof IndexKey )
        {
            return Long.compare( indexId, ((IndexKey) other).indexId() );
        }
        return recordType().ordinal() - other.recordType().ordinal();
    }
}
