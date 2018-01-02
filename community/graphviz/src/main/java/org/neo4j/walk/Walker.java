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
package org.neo4j.walk;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.tooling.GlobalGraphOperations;

public abstract class Walker
{
    public abstract <R, E extends Throwable> R accept( Visitor<R, E> visitor ) throws E;

    public static Walker fullGraph( final GraphDatabaseService graphDb )
    {
        return new Walker()
        {
            @Override
            public <R, E extends Throwable> R accept( Visitor<R, E> visitor ) throws E
            {
                for ( Node node : GlobalGraphOperations.at( graphDb ).getAllNodes() )
                {
                    visitor.visitNode( node );
                    for ( Relationship edge : node.getRelationships( Direction.OUTGOING ) )
                    {
                        visitor.visitRelationship( edge );
                    }
                }
                return visitor.done();
            }
        };
    }

    public static Walker crosscut( Iterable<Node> traverser, final RelationshipType... types )
    {
        final Set<Node> nodes = new HashSet<Node>();
        for ( Node node : traverser )
        {
            nodes.add( node );
        }
        return new Walker()
        {
            @Override
            public <R, E extends Throwable> R accept( Visitor<R, E> visitor ) throws E
            {
                for ( Node node : nodes )
                {
                    visitor.visitNode( node );
                    for ( Relationship relationship : node.getRelationships( types ) )
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
