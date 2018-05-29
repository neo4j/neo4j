/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
        sb.append( leftNode ).append( outgoing ? "-" : "<-" );
        sb.append( relMatcher ).append( outgoing ? "->" : "-" );
        sb.append( rightNode );
        return sb.toString();
    }
}
