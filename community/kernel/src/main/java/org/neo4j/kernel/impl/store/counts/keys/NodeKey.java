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

import org.neo4j.kernel.impl.api.CountsVisitor;

import static org.neo4j.kernel.impl.util.IdPrettyPrinter.label;

public final class NodeKey implements CountsKey
{
    private final int labelId;

    NodeKey( int labelId )
    {
        this.labelId = labelId;
    }

    @Override
    public String toString()
    {
        return String.format( "NodeKey[(%s)]", label( labelId ) );
    }

    @Override
    public void accept( CountsVisitor visitor, long ignored, long count )
    {
        visitor.visitNodeCount( labelId, count );
    }

    @Override
    public CountsKeyType recordType()
    {
        return CountsKeyType.ENTITY_NODE;
    }

    @Override
    public int hashCode()
    {
        int result = labelId;
        result = 31 * result + recordType().hashCode();
        return result;
    }

    @Override
    public boolean equals( Object o )
    {
        return this == o || (o instanceof org.neo4j.kernel.impl.store.counts.keys.NodeKey) &&
                            labelId == ((org.neo4j.kernel.impl.store.counts.keys.NodeKey) o).labelId;
    }

    @Override
    public int compareTo( CountsKey other )
    {
        if ( other instanceof org.neo4j.kernel.impl.store.counts.keys.NodeKey )
        {
            org.neo4j.kernel.impl.store.counts.keys.NodeKey that =
                    (org.neo4j.kernel.impl.store.counts.keys.NodeKey) other;
            return this.labelId - that.labelId;
        }
        return recordType().ordinal() - other.recordType().ordinal();
    }
}
