package examples;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.index.Neo4jTestCase;
import org.neo4j.kernel.EmbeddedGraphDatabase;

public class SiteExamples
{
    private static GraphDatabaseService graphDb;
    private Transaction tx;
    
    @BeforeClass
    public static void setUpDb()
    {
        String path = "target/var/examples";
        Neo4jTestCase.deleteFileOrDirectory( new File( path ) );
        graphDb = new EmbeddedGraphDatabase( path );
    }
    
    @Before
    public void beginTx()
    {
        tx = graphDb.beginTx();
    }
    
    @After
    public void finishTx()
    {
        tx.success();
        tx.finish();
    }
    
    @Test
    public void addSomeThings()
    {
        // START SNIPPET: add
        Index<Node> persons = graphDb.index().forNodes( "persons" );
        Node morpheus = graphDb.createNode();
        Node trinity = graphDb.createNode();
        Node neo = graphDb.createNode();
        persons.add( morpheus, "name", "Morpheus" );
        persons.add( morpheus, "rank", "Captain" );
        persons.add( trinity, "name", "Trinity" );
        persons.add( neo, "name", "Neo" );
        persons.add( neo, "title", "The One" );
        // END SNIPPET: add
    }
    
    @Test
    public void doSomeGets()
    {
        Index<Node> persons = graphDb.index().forNodes( "persons" );
        
        // START SNIPPET: get
        Node morpheus = persons.get( "name", "Morpheus" ).getSingle();
        // END SNIPPET: get
    }

    @Test
    public void doSomeQueries()
    {
        Index<Node> persons = graphDb.index().forNodes( "persons" );
        
        // START SNIPPET: query
        for ( Node person : persons.query( "name", "*e*" ) )
        {
            // It will get Morpheus and Neo
        }
        Node neo = persons.query( "name:*e* AND title:\"The One\"" ).getSingle();
        // END SNIPPET: query
    }
}
