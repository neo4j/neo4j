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

import org.apache.lucene.index.CorruptIndexException;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.helpers.Exceptions;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class LuceneRecoveryIT
{
    @Test
    public void testHardCoreRecovery() throws Exception
    {
        String path = "target/hcdb";
        FileUtils.deleteRecursively( new File( path ) );
        Process process = Runtime.getRuntime().exec( new String[]{
                "java", "-cp", System.getProperty( "java.class.path" ),
                Inserter.class.getName(), path
        } );

        // Let it run for a while and then kill it, and wait for it to die
        awaitFile( new File( path, "started" ) );
        Thread.sleep( 5000 );
        process.destroy();
        process.waitFor();

        final GraphDatabaseService db = new TestGraphDatabaseFactory().newEmbeddedDatabase( path );
        try ( Transaction transaction = db.beginTx() )
        {
            assertTrue( db.index().existsForNodes( "myIndex" ) );
            Index<Node> index = db.index().forNodes( "myIndex" );
            for ( Node node : GlobalGraphOperations.at( db ).getAllNodes() )
            {
                for ( String key : node.getPropertyKeys() )
                {
                    String value = (String) node.getProperty( key );
                    boolean found = false;
                    for ( Node indexedNode : index.get( key, value ) )
                    {
                        if ( indexedNode.equals( node ) )
                        {
                            found = true;
                            break;
                        }
                    }
                    if ( !found )
                    {
                        throw new IllegalStateException( node + " has property '" + key + "'='" +
                                value + "', but not in index" );
                    }
                }
            }
        }
        catch ( Exception e )
        {
            if ( Exceptions.contains( e, CorruptIndexException.class ) )
            {
                // On some machines and during some circumstances a lucene index may become
                // corrupted during a crash. This is out of our control and since this test
                // is about a legacy (a.k.a. manual index) the db cannot just re-populate the
                // index automatically. We have to consider this an OK scenario and we cannot
                // verify the index any further if it happens.
                return;
            }

            // This was another unknown exception, throw it so that the test fails with it
            throw e;
        }

        // Added due to a recovery issue where the lucene data source write wasn't released properly after recovery.
        Thread t = new Thread()
        {
            @Override
            public void run()
            {
                try ( Transaction tx = db.beginTx() )
                {
                    Index<Node> index = db.index().forNodes( "myIndex" );
                    index.add( db.createNode(), "one", "two" );
                    tx.success();
                }
            }
        };
        t.start();
        t.join();

        db.shutdown();
    }

    private void awaitFile( File file ) throws InterruptedException
    {
        long end = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis( 30 );
        while ( !file.exists() && System.currentTimeMillis() < end )
        {
            Thread.sleep( 100 );
        }
        if ( !file.exists() )
        {
            fail( "The inserter doesn't seem to have run properly" );
        }
    }
}
