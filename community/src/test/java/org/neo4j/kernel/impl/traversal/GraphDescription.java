package org.neo4j.kernel.impl.traversal;

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
                    node = graphdb.getReferenceNode();
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
        Transaction tx = graphdb.beginTx();
        try
        {
            for ( RelationshipDescription rel : description )
            {
                node = rel.create( graphdb, nodes ).getEndNode();
            }
            tx.success();
        }
        finally
        {
            tx.finish();
        }
        return node;
    }
}
