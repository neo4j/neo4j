/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.index.impl.lucene;

import java.util.Arrays;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;
import static org.neo4j.index.Neo4jTestCase.assertContains;

public class TestIndexNames
{
    private static GraphDatabaseService graphDb;
    private Transaction tx;
    
    @BeforeClass
    public static void setUpStuff()
    {
        graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase();
    }

    @AfterClass
    public static void tearDownStuff()
    {
        graphDb.shutdown();
    }

    @After
    public void commitTx()
    {
        finishTx( true );
    }

    public void finishTx( boolean success )
    {
        if ( tx != null )
        {
            if ( success )
            {
                tx.success();
            }
            tx.close();
            tx = null;
        }
    }

    public void beginTx()
    {
        if ( tx == null )
        {
            tx = graphDb.beginTx();
        }
    }

    void restartTx()
    {
        finishTx( true );
        beginTx();
    }
    
    @Test
    public void makeSureIndexNamesCanBeRead()
    {
        beginTx();
        assertEquals( 0, graphDb.index().nodeIndexNames().length );
        String name1 = "my-index-1";
        Index<Node> nodeIndex1 = graphDb.index().forNodes( name1 );
        assertContains( Arrays.asList( graphDb.index().nodeIndexNames() ), name1 );
        String name2 = "my-index-2";
        graphDb.index().forNodes( name2 );
        assertContains( Arrays.asList( graphDb.index().nodeIndexNames() ), name1, name2 );
        graphDb.index().forRelationships( name1 );
        assertContains( Arrays.asList( graphDb.index().nodeIndexNames() ), name1, name2 );
        assertContains( Arrays.asList( graphDb.index().relationshipIndexNames() ), name1 );
        finishTx( true );

        restartTx();
        assertContains( Arrays.asList( graphDb.index().nodeIndexNames() ), name1, name2 );
        assertContains( Arrays.asList( graphDb.index().relationshipIndexNames() ), name1 );
        nodeIndex1.delete();
        assertContains( Arrays.asList( graphDb.index().nodeIndexNames() ), name1, name2 );
        assertContains( Arrays.asList( graphDb.index().relationshipIndexNames() ), name1 );
        finishTx( true );
        beginTx();
        assertContains( Arrays.asList( graphDb.index().nodeIndexNames() ), name2 );
        assertContains( Arrays.asList( graphDb.index().relationshipIndexNames() ), name1 );
        finishTx( false );
    }
}
