/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.kernel;

import static org.neo4j.helpers.collection.IteratorUtil.addToCollection;
import static org.neo4j.helpers.collection.IteratorUtil.count;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.nioneo.store.InvalidRecordException;
import org.neo4j.kernel.impl.util.FileUtils;

public class SmallGraphManyRelsAndThreads
{
    private static enum Types implements RelationshipType
    {
        t1,
        t2;
    }
    
    public static void main( String[] args ) throws IOException
    {
        GraphDatabaseService db = startGraphDb();
        System.out.println( "Creating graph..." );
        Node[] nodes = createGraph( db );
        System.out.println( "Running workers..." );
        spawnTheThreadsAndDoAllThatShit( db, nodes );
    }
    
    static RelationshipType randomRelationshipType()
    {
        return Types.values()[(int)(System.currentTimeMillis()%Types.values().length)];
    }

    private static void spawnTheThreadsAndDoAllThatShit( final GraphDatabaseService db, Node[] nodes )
    {
        List<Worker> threads = new ArrayList<Worker>();
        for ( int i = 0; i < 10; i++ )
        {
            Worker worker = new Worker( nodes );
            worker.start();
            threads.add( worker );
        }
        
        Thread cacheClearer = new Thread()
        {
            @Override
            public void run()
            {
                while ( true )
                {
                    try
                    {
                        Thread.sleep( 4000 );
                    }
                    catch ( InterruptedException e )
                    { // OK
                    }
                    ((AbstractGraphDatabase)db).getConfig().getGraphDbModule().getNodeManager().clearCache();
//                    System.out.println( "Cache cleared" );
                }
            }
        };
        cacheClearer.start();
    }
    
    private static class Worker extends Thread
    {
        private final Random random = new Random();
        private final Node[] nodes;
        private int totalCount;

        Worker( Node[] nodes )
        {
            this.nodes = nodes;
        }
        
        @Override
        public void run()
        {
            while ( true )
            {
                try
                {
                    doOneThing();
                }
                catch ( DeadlockDetectedException e )
                {
//                    System.out.println( "deadlock... continuing" );
                }
                catch ( InvalidRecordException e )
                {
                    System.out.println( e.toString() );
                }
                catch ( Throwable t )
                {
                    t.printStackTrace();
                }
            }
        }

        private void doOneThing()
        {
            Transaction tx = nodes[0].getGraphDatabase().beginTx();
            try
            {
                Node node = nodes[random.nextInt( nodes.length )];
                int count = random.nextInt( 3 ) + 3;
                switch ( (int)(System.currentTimeMillis()%3) )
                {
                case 0: // Grab a node and loop through all its relationships
                    totalCount += count( node.getRelationships() );
                    break;
                case 1: // Create some relationships on a node
                    for ( int i = 0; i < count; i++ )
                    {
                        Node otherNode = nodes[random.nextInt( nodes.length )];
                        node.createRelationshipTo( otherNode, randomRelationshipType() );
                    }
                    break;
                case 2: // Delete some relationships from a node
                    List<Relationship> rels = addToCollection( node.getRelationships(), new ArrayList<Relationship>() );
                    for ( int i = 0; i < count; i++ )
                    {
                        Relationship rel = null;
                        int pos = 0;
                        while ( rel == null )
                        {
                            pos = random.nextInt( rels.size() );
                            rel = rels.get( pos );
                        }
                        try
                        {
                            rel.delete();
                        }
                        catch ( NotFoundException e )
                        {
                            // OK
                        }
                        rels.set( pos, null );
                    }
                    break;
                }
                tx.success();
            }
            finally
            {
                tx.finish();
            }
        }
    }

    private static GraphDatabaseService startGraphDb() throws IOException
    {
        String path = "target/var/relload";
        FileUtils.deleteRecursively( new File( path ) );
        GraphDatabaseService db = new EmbeddedGraphDatabase( path );
        return db;
    }

    private static Node[] createGraph( GraphDatabaseService db )
    {
        Transaction tx = db.beginTx();
        Node[] nodes = createNodes( db, 100 );
        tx.success(); tx.finish();
        
        tx = db.beginTx();
        int relsPerNode = 250;
        int relsCount = nodes.length*relsPerNode/2;
        Random random = new Random();
        for ( int i = 0; i < relsCount; i++ )
        {
            int node1 = random.nextInt( nodes.length );
            // No need to check same node id, we support loops... HA!
            int node2 = random.nextInt( nodes.length );
            nodes[node1].createRelationshipTo( nodes[node2], randomRelationshipType() );
            if ( i % 10000 == 0 )
            {
                tx.success(); tx.finish(); tx = db.beginTx();
            }
        }
        tx.success(); tx.finish();
        return nodes;
    }

    private static Node[] createNodes( GraphDatabaseService db, int count )
    {
        Node[] nodes = new Node[count];
        for ( int i = 0; i < count; i++ )
        {
            nodes[i] = db.createNode();
        }
        return nodes;
    }
}
