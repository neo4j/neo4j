/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.store;

import org.junit.Ignore;
import org.junit.Test;

import java.util.Random;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;

import static java.lang.Math.max;
import static java.lang.System.currentTimeMillis;
import static org.neo4j.test.TargetDirectory.forTest;

@Ignore( "Written as a reaction to an observed bug, but it doesn't seem to trigger it though" )
public class LargeByteArraysIT
{
    private static final Random RANDOM = new Random();
    
    @Test
    public void largeByteArrays() throws Exception
    {
        String storeDir = forTest( getClass() ).cleanDirectory( "bytearrays" ).getAbsolutePath();
        GraphDatabaseService db = new TestGraphDatabaseFactory().newEmbeddedDatabase( storeDir );
        try
        {
            for ( int i = 0; i < 100000; i++ )
            {
                createNodeWithBigArray( db );
                if ( i > 0 && i%100 == 0 ) System.out.println( i );
            }
        }
        finally
        {
            db.shutdown();
        }
    }

    private void createNodeWithBigArray( GraphDatabaseService db )
    {
        Transaction tx = db.beginTx();
        try
        {
            Node node = db.createNode();
            node.setProperty( "prop", randomBigByteArray() );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    private byte[] randomBigByteArray()
    {
        byte[] array = new byte[max( 248, RANDOM.nextInt( 248*1024 ) )];
        for ( int i = 0; i < array.length; i++ ) array[i] = (byte) currentTimeMillis();
        return array;
    }
}
