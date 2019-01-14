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
package org.neo4j.kernel.impl.util;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.PathValue;
import org.neo4j.values.virtual.RelationshipValue;

public class PathWrappingPathValue extends PathValue
{
    private final Path path;

    PathWrappingPathValue( Path path )
    {
        this.path = path;
    }

    @Override
    public NodeValue startNode()
    {
        return ValueUtils.fromNodeProxy( path.startNode() );
    }

    @Override
    public NodeValue endNode()
    {
        return ValueUtils.fromNodeProxy( path.endNode() );
    }

    @Override
    public RelationshipValue lastRelationship()
    {
        return ValueUtils.fromRelationshipProxy( path.lastRelationship() );
    }

    @Override
    public NodeValue[] nodes()
    {
        int length = path.length() + 1;
        NodeValue[] values = new NodeValue[length];
        int i = 0;
        for ( Node node : path.nodes() )
        {
            values[i++] = ValueUtils.fromNodeProxy( node );
        }
        return values;
    }

    @Override
    public RelationshipValue[] relationships()
    {
        int length = path.length();
        RelationshipValue[] values = new RelationshipValue[length];
        int i = 0;
        for ( Relationship relationship : path.relationships() )
        {
            values[i++] = ValueUtils.fromRelationshipProxy( relationship );
        }
        return values;
    }

    public Path path()
    {
        return path;
    }
}
