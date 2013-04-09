/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.TransactionBuilder;

import static java.lang.System.*;
import static org.neo4j.test.TargetDirectory.*;

@Ignore( "It doesn't assert anything yet" )
public class TestUnforcedTransaction
{
    private static GraphDatabaseAPI db;
    
    @BeforeClass
    public static void setupDb()
    {
        db = (GraphDatabaseAPI)
                new GraphDatabaseFactory()
                .newEmbeddedDatabase( forTest( TestUnforcedTransaction.class ).directory( "d",
                        true ).getAbsolutePath() );
    }
    
    @AfterClass
    public static void teardownDb()
    {
        db.shutdown();
    }
    
    @Test
    public void relaxedForce() throws Exception
    {
        // TODO: Measure somehow
        long t = currentTimeMillis();
        for ( int i = 0; i < 10000; i++ )
        {
            TransactionBuilder builder = db.tx();
            if ( (i/1000)%2 == 1 ) builder = builder.unforced();
            Transaction tx = builder.begin();
            Node node = db.createNode();
            node.setProperty( "name", "Mattias" );
            tx.success();
            tx.finish();
            if ( i % 1000 == 0 && i > 0 ) System.out.println( i );
        }
        System.out.println( (currentTimeMillis()-t) + "ms" );
    }
}
