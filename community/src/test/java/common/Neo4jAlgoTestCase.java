/*
 * Copyright 2008 Network Engine for Objects in Lund AB [neotechnology.com]
 * 
 * This program is free software: you can redistribute it and/or modify
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package common;

import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.EmbeddedGraphDatabase;

/**
 * Base class for test cases working on a NeoService. It sets up a NeoService
 * and a transaction.
 * @author Patrik Larsson
 */
public abstract class Neo4jAlgoTestCase
{
    protected static GraphDatabaseService graphDb;
    protected static SimpleGraphBuilder graph = null;
    protected Transaction tx;

    protected static enum MyRelTypes implements RelationshipType
    {
        R1, R2, R3
    }

    @BeforeClass
    public static void setUpGraphDb() throws Exception
    {
        graphDb = new EmbeddedGraphDatabase( "target/var/algotest" );
        graph = new SimpleGraphBuilder( graphDb, MyRelTypes.R1 );
    }
    
    @Before
    public void setUpTransaction()
    {
        tx = graphDb.beginTx();
    }

    @AfterClass
    public static void tearDownGraphDb() throws Exception
    {
        graphDb.shutdown();
    }
    
    @After
    public void tearDownTransactionAndGraph()
    {
        graph.clear();
        tx.finish();
    }
    
    protected void restartTx()
    {
        tx.success();
        tx.finish();
        tx = graphDb.beginTx();
    }

    protected void assertPath( Path path, Node... nodes )
    {
        int i = 0;
        for ( Node node : path.nodes() )
        {
            assertEquals( nodes[i++], node );
        }
        assertEquals( nodes.length, i );
    }
}
