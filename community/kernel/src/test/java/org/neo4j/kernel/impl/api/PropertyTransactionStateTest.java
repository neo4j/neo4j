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
package org.neo4j.kernel.impl.api;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.*;

public class PropertyTransactionStateTest
{
    private GraphDatabaseService db;

    @Before
    public void setUp() throws Exception
    {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
    }

    @After
    public void shutDown()
    {
        db.shutdown();
    }


    @Test
    public void testUpdateDoubleArrayProperty() throws Exception
    {
        Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = db.createNode();
            node.setProperty( "foo", new double[] { 0, 0, 0, 0 } );
            tx.success();
        }

        try ( Transaction ignore = db.beginTx() )
        {
            for ( int i = 0; i < 100; i++ )
            {
                double[] data = (double[]) node.getProperty( "foo" );
                data[2] = i;
                data[3] = i;
                node.setProperty( "foo", data );
                assertArrayEquals( new double[] { 0, 0, i, i }, (double[]) node.getProperty( "foo" ), 0.1D );
            }
        }
    }

    @Test
    public void testStringPropertyUpdate() throws Exception
    {
        String key = "foo";
        Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = db.createNode();
            node.setProperty( key, "one" );
            tx.success();
        }

        try ( Transaction ignore = db.beginTx() )
        {
            node.setProperty( key, "one" );
            node.setProperty( key, "two" );
            assertEquals( "two", node.getProperty( key ) );
        }
    }

    @Test
    public void testSetDoubleArrayProperty() throws Exception
    {
        try ( Transaction ignore = db.beginTx() )
        {
            Node node = db.createNode();
            for ( int i = 0; i < 100; i++ )
            {
                node.setProperty( "foo", new double[] { 0, 0, i, i } );
                assertArrayEquals( new double[] { 0, 0, i, i }, (double[]) node.getProperty( "foo" ), 0.1D );
            }
        }
    }
}
