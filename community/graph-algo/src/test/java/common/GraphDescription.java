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
package common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;

public class GraphDescription implements GraphDefinition
{

    private static class RelationshipDescription
    {
        private final String end;
        private final String start;
        private final RelationshipType type;

        public RelationshipDescription( String rel )
        {
            String[] parts = rel.split( " " );
            if ( parts.length != 3 )
            {
                throw new IllegalArgumentException( "syntax error: \"" + rel
                                                    + "\"" );
            }
            start = parts[0];
            type = DynamicRelationshipType.withName( parts[1] );
            end = parts[2];
        }

        public Relationship create( GraphDatabaseService graphdb,
                Map<String, Node> nodes )
        {
            Node startNode = getNode( graphdb, nodes, start );
            Node endNode = getNode( graphdb, nodes, end );
            return startNode.createRelationshipTo( endNode, type );
        }

        private Node getNode( GraphDatabaseService graphdb,
                Map<String, Node> nodes, String name )
        {
            Node node = nodes.get( name );
            if ( node == null )
            {
                if ( nodes.size() == 0 )
                {
                    node = graphdb.createNode();
                }
                else
                {
                    node = graphdb.createNode();
                }
                node.setProperty( "name", name );
                nodes.put( name, node );
            }
            return node;
        }
    }

    private final RelationshipDescription[] description;

    public GraphDescription( String... description )
    {
        List<RelationshipDescription> lines = new ArrayList<RelationshipDescription>();
        for ( String part : description )
        {
            for ( String line : part.split( "\n" ) )
            {
                lines.add( new RelationshipDescription( line ) );
            }
        }
        this.description = lines.toArray( new RelationshipDescription[lines.size()] );
    }

    public Node create( GraphDatabaseService graphdb )
    {
        Map<String, Node> nodes = new HashMap<String, Node>();
        Node node = null;
        try ( Transaction tx = graphdb.beginTx() )
        {
            for ( RelationshipDescription rel : description )
            {
                node = rel.create( graphdb, nodes ).getEndNode();
            }
            tx.success();
        }
        return node;
    }
}
