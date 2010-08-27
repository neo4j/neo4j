package org.neo4j.kernel;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.kernel.apps.GraphDatabaseApp;

public class Noobinit extends GraphDatabaseApp
{
    @Override
    protected String exec( AppCommandParser parser, Session session, Output out ) throws Exception
    {
        RelationshipType type = DynamicRelationshipType.withName( "REL_TYPE" );
        out.println( "getOrCreateSingleOtherNode" );
        Node fromNode = getServer().getDb().getReferenceNode();
        fromNode.removeProperty( "___locking_property___" );
        out.println( "Grabbed lock" );
        Relationship singleRelationship =
                fromNode.getSingleRelationship( type, Direction.OUTGOING );
        out.println( "Single relationship is " + singleRelationship );
        Node otherNode = null;
        if ( singleRelationship != null )
        {
            otherNode = singleRelationship.getOtherNode( fromNode );
        }
        else
        {
            otherNode = fromNode.getGraphDatabase().createNode();
            fromNode.createRelationshipTo( otherNode, type );
            out.println( "Created new relationship" );
        }
        return null;
    }
}
