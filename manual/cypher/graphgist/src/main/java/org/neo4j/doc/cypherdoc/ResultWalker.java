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
package org.neo4j.doc.cypherdoc;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.walk.Visitor;
import org.neo4j.walk.Walker;

public class ResultWalker
{
    public static Walker result( State state )
    {
        final Set<Node> nodes = new HashSet<>();

        for ( long nodeId : state.latestResult.nodeIds )
        {
            nodes.add( state.database.getNodeById( nodeId ) );
        }

        for ( long relationshipId : state.latestResult.relationshipIds )
        {
            Relationship rel = state.database.getRelationshipById( relationshipId );
            nodes.add( rel.getStartNode() );
            nodes.add( rel.getEndNode() );
        }

        return new Walker()
        {
            @Override
            public <R, E extends Throwable> R accept( Visitor<R, E> visitor ) throws E
            {
                for ( Node node : nodes )
                {
                    visitor.visitNode( node );
                    for ( Relationship relationship : node.getRelationships() )
                    {
                        if ( nodes.contains( relationship.getOtherNode( node ) ) )
                        {
                            visitor.visitRelationship( relationship );
                        }
                    }
                }
                return visitor.done();
            }
        };
    }
}
