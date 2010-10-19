package org.neo4j.examples;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.EmbeddedGraphDatabase;

import java.io.File;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * An example of unit testing with Neo4j.
 */
public class Neo4jBaseTest
{
    /**
     * Base directory for temporary database.
     */
    protected File testDirectory = new File( "target/var" );

    /**
     * Full path to the temporary database.
     */
    protected File testDatabasePath = new File( testDirectory, "testdb" );
    protected GraphDatabaseService graphDb;


    /**
     * Create temporary database for each unit test.
     * <p/>
     * This will delete any existing database prior to creating a new one.
     */
    @Before
    public void prepareTestDatabase()
    {
        deleteFileOrDirectory( testDatabasePath );
        graphDb = new EmbeddedGraphDatabase( testDatabasePath.getAbsolutePath() );
    }

    /**
     * Shutdown the database.
     */
    @After
    public void destroyTestDatabase()
    {
        graphDb.shutdown();
    }

    protected void deleteFileOrDirectory( File path )
    {
        if ( path.exists() )
        {
            if ( path.isDirectory() )
            {
                for ( File child : path.listFiles() )
                {
                    deleteFileOrDirectory( child );
                }
            }
            path.delete();
        }
    }


    @Test
    public void shouldCreateNode()
    {
        Transaction tx = graphDb.beginTx();
        Node n = null;
        try
        {
            n = graphDb.createNode();
            n.setProperty( "name", "Nancy" );
            tx.success();
        } catch ( Exception e )
        {
            tx.failure();
        } finally
        {
            tx.finish();
        }

        // The node should have an id greater than 0, which is the id of the reference node.
        assertThat( n.getId(), is( greaterThan( 0l ) ) );

        // Retrieve a node by using the id of the created node. The id's and property should match.
        Node foundNode = graphDb.getNodeById( n.getId() );
        assertThat( foundNode.getId(), is( n.getId() ) );
        assertThat( (String) foundNode.getProperty( "name" ), is( "Nancy" ) );

    }

}
