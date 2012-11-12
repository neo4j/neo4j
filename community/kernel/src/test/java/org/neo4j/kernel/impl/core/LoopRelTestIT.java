/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.kernel.impl.core;

import java.io.File;
import java.util.ArrayList;
import java.util.Random;

import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.test.TargetDirectory;
import org.neo4j.tooling.GlobalGraphOperations;

/**
 *
 * @author johan
 */
public class LoopRelTestIT
{
    @Test
    public void runTheTests() throws Exception
    {
        TargetDirectory.recursiveDelete( new File( "target/looprels" ) );
        main();
    }

    public static void main( String... args )
    {
        for ( int i = 0; i < 10; i++ )
        {
            GraphDatabaseService gdb = new EmbeddedGraphDatabase( "target/looprels" );
            ArrayList<Node> nodes = verifyOkDbAndSetup( gdb );
            doRandomStuff( gdb, 1000, nodes );
            gdb.shutdown();
        }
    }

    private static ArrayList<Node> verifyOkDbAndSetup( GraphDatabaseService gdb )
    {
        ArrayList<Node> allNodes = new ArrayList<Node>();
        for ( Node node : GlobalGraphOperations.at( gdb ).getAllNodes() )
        {
            allNodes.add( node );
            for ( Relationship rel : node.getRelationships( Direction.OUTGOING) )
            {
                rel.getOtherNode( node );
            }
        }
        Transaction tx = gdb.beginTx();
        try
        {
            while ( allNodes.size() < 1000 )
            {
                allNodes.add( gdb.createNode() );
            }
            tx.success();
        }
        finally
        {
            tx.finish();
        }
        return allNodes;
    }

    private static final Random r = new Random( System.currentTimeMillis() );

    private static RelationshipType getRandomRelationshipType()
    {
        float f = r.nextFloat();
        if ( f < 0.33 )
        {
            return DynamicRelationshipType.withName( "TEST1" );
        }
        else if ( f < 0.66 )
        {
            return DynamicRelationshipType.withName( "TEST2" );
        }
        else
        {
            return DynamicRelationshipType.withName( "TEST3" );
        }
    }

    private static void doRandomStuff( GraphDatabaseService gdb, int count, ArrayList<Node> nodes )
    {
        Transaction tx = gdb.beginTx();
        try
        {
            for ( int i = 0; i < count; i++ )
            {
                int index = r.nextInt( nodes.size() );
                Node node = nodes.get( index );
                float op = r.nextFloat();
                if ( op < 0.1f )
                {
                    node.createRelationshipTo( gdb.createNode(), getRandomRelationshipType() );
                }
                else if ( op < 0.25f )
                {
                    node.createRelationshipTo( node, getRandomRelationshipType() );
                }
                else if ( op < 0.75f )
                {
                    Node otherNode = nodes.get( r.nextInt( nodes.size() ) );
                    node.createRelationshipTo( otherNode, getRandomRelationshipType() );
                }
                else if ( op < 0.95 )
                {
                    for ( Relationship rel : node.getRelationships() )
                    {
                        if ( r.nextFloat() < 0.25f )
                        {
                            rel.delete();
                        }
                    }
                }
                else if ( nodes.size() > 100 )
                {
                    for ( Relationship rel : node.getRelationships() )
                    {
                        rel.delete();
                    }
                    nodes.remove( index );
                    node.delete();
                }
                if ( r.nextFloat() < 0.2f )
                {
                    tx.success();
                    tx.finish();
                    tx = gdb.beginTx();
                }
            }
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }
}
