/**
 * Copyright (c) 2002-2010 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

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
