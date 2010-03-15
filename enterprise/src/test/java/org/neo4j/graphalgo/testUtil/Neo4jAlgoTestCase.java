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
package org.neo4j.graphalgo.testUtil;

import junit.framework.TestCase;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.EmbeddedGraphDatabase;

/**
 * Base class for test cases working on a NeoService. It sets up a NeoService
 * and a transaction.
 * @author Patrik Larsson
 */
public class Neo4jAlgoTestCase extends TestCase
{
    protected GraphDatabaseService graphDb;
    protected Transaction tx;
    protected SimpleGraphBuilder graph = null;

    protected static enum MyRelTypes implements RelationshipType
    {
        R1, R2, R3
    }

    @Override
    protected void setUp() throws Exception
    {
        super.setUp();
        graphDb = new EmbeddedGraphDatabase( "target/var/algotest" );
        tx = graphDb.beginTx();
        graph = new SimpleGraphBuilder( graphDb, MyRelTypes.R1 );
    }

    @Override
    protected void tearDown() throws Exception
    {
        super.tearDown();
        if ( graph != null )
        {
            graph.clear();
        }
        tx.finish();
        graphDb.shutdown();
    }

    /**
     * Dummy testcase since an assertion fails if no test methods exists.
     */
    public void testRun()
    {
    }
}
