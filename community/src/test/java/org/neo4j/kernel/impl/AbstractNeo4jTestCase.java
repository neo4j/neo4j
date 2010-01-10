/*
 * Copyright (c) 2002-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.impl;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.core.TestNeo4j;

public abstract class AbstractNeo4jTestCase extends TestCase
{
    protected static final String NEO4J_BASE_PATH = "target/var/";
    
    public AbstractNeo4jTestCase( String testName )
    {
        super( testName );
    }

    private GraphDatabaseService graphDb;
    private Transaction tx;

    public GraphDatabaseService getGraphDb()
    {
        return graphDb;
    }

    public EmbeddedGraphDatabase getEmbeddedGraphDb()
    {
        return (EmbeddedGraphDatabase) graphDb;
    }

    public Transaction getTransaction()
    {
        return tx;
    }

    public static Test suite()
    {
        TestSuite suite = new TestSuite( TestNeo4j.class );
        return suite;
    }
    
    public static String getStorePath( String endPath )
    {
        return NEO4J_BASE_PATH + endPath;
    }

    public void setUp()
    {
        graphDb = new EmbeddedGraphDatabase( getStorePath( "neo-test" ) );
        tx = graphDb.beginTx();
    }

    public void tearDown()
    {
        tx.finish();
        graphDb.shutdown();
    }

    public void setTransaction( Transaction tx )
    {
        this.tx = tx;
    }

    public void newTransaction()
    {
        if ( tx != null )
        {
            tx.success();
            tx.finish();
        }
        tx = graphDb.beginTx();
    }
    
    public void commit()
    {
        if ( tx != null )
        {
            tx.success();
            tx.finish();
        }
    }
    
    public NodeManager getNodeManager()
    {
        return ((EmbeddedGraphDatabase) graphDb).getConfig().getNeoModule().getNodeManager();
    }
}