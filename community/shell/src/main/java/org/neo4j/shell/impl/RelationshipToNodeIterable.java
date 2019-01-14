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
package org.neo4j.shell.impl;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.helpers.collection.MappingResourceIterator;

public class RelationshipToNodeIterable extends MappingResourceIterator<Node, Relationship>
{
    private final Node fromNode;

    private RelationshipToNodeIterable( ResourceIterable<Relationship> iterableToWrap, Node fromNode )
    {
        super( iterableToWrap.iterator() );
        this.fromNode = fromNode;
    }

    @Override
    protected Node map( Relationship rel )
    {
        return rel.getOtherNode( fromNode );
    }

    public static MappingResourceIterator<Node,Relationship> wrap( ResourceIterable<Relationship> relationships, Node fromNode )
    {
        return new RelationshipToNodeIterable( relationships, fromNode );
    }
}
