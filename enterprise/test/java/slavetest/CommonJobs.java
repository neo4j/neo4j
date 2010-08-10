package slavetest;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;

public class CommonJobs
{
    public static final RelationshipType REL_TYPE = DynamicRelationshipType.withName( "HA_TEST" );
    
    public static class CreateSubRefNodeJob implements Job<Void>
    {
        private final String name;

        public CreateSubRefNodeJob( String name )
        {
            this.name = name;
        }
        
        public Void execute( GraphDatabaseService db )
        {
            Transaction tx = db.beginTx();
            try
            {
                Node node = db.createNode();
                db.getReferenceNode().createRelationshipTo( node, REL_TYPE );
                node.setProperty( "name", name );
                tx.success();
            }
            finally
            {
                tx.finish();
            }
            return null;
        }
    }
    
    public static class SetSubRefNameJob implements Job<String>
    {
        public String execute( GraphDatabaseService db )
        {
            Transaction tx = db.beginTx();
            try
            {
                Node refNode = db.getReferenceNode();
                // To force it to pull updates
                refNode.removeProperty( "yoyoyoyo" );
                Node node = refNode.getSingleRelationship( REL_TYPE,
                        Direction.OUTGOING ).getEndNode();
                String name = (String) node.getProperty( "name" );
                node.setProperty( "title", "Whatever" );
                tx.success();
                return name;
            }
            finally
            {
                tx.finish();
            }
        }
    }
}
