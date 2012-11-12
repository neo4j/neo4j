/**
 * Licensed to Neo Technology under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Neo Technology licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.neo4j.examples;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.EmbeddedGraphDatabase;

/**
 * An example of unit testing with Neo4j.
 */
public class Neo4jBasicTest
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
        // START SNIPPET: beforeTest
        deleteFileOrDirectory( testDatabasePath );
        graphDb = new EmbeddedGraphDatabase( testDatabasePath.getAbsolutePath() );
        // END SNIPPET: beforeTest
    }

    /**
     * Shutdown the database.
     */
    @After
    public void destroyTestDatabase()
    {
        // START SNIPPET: afterTest
        graphDb.shutdown();
        // END SNIPPET: afterTest
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
    public void startWithConfiguration()
    {
        // START SNIPPET: startDbWithConfig
        Map<String, String> config = new HashMap<String, String>();
        config.put( "neostore.nodestore.db.mapped_memory", "10M" );
        config.put( "string_block_size", "60" );
        config.put( "array_block_size", "300" );
        EmbeddedGraphDatabase db = new EmbeddedGraphDatabase( "target/mydb", config  );
        // END SNIPPET: startDbWithConfig
        db.shutdown();
    }
    @Test
    public void shouldCreateNode()
    {
        // START SNIPPET: unitTest
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

        // END SNIPPET: unitTest
    }

}
