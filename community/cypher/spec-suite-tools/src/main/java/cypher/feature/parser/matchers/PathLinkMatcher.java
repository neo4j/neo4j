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
package cypher.feature.parser.matchers;

import org.neo4j.graphdb.Relationship;

public class PathLinkMatcher implements ValueMatcher
{
    private final RelationshipMatcher relMatcher;
    private final NodeMatcher leftNode;
    private NodeMatcher rightNode;
    private final boolean outgoing;

    public PathLinkMatcher( RelationshipMatcher relMatcher, NodeMatcher leftNode, boolean outgoing )
    {
        this.relMatcher = relMatcher;
        this.leftNode = leftNode;
        this.outgoing = outgoing;
    }

    @Override
    public boolean matches( Object value )
    {
        if ( value instanceof Relationship )
        {
            Relationship real = (Relationship) value;
            if ( !relMatcher.matches( real ) )
            {
                return false;
            }
            if ( outgoing )
            {
                if ( !leftNode.matches( real.getStartNode() ) || !rightNode.matches( real.getEndNode() ) )
                {
                    return false;
                }
            }
            // incoming
            else if ( !leftNode.matches( real.getEndNode() ) || !rightNode.matches( real.getStartNode() ) )
            {
                return false;
            }
            return true;
        }
        return false;
    }

    public void setRightNode( NodeMatcher rightNode )
    {
        this.rightNode = rightNode;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append( leftNode ).append( (outgoing ? "-" : "<-") );
        sb.append( relMatcher ).append( (outgoing ? "->" : "-") );
        sb.append( rightNode );
        return sb.toString();
    }
}
