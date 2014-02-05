/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.store;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.test.ProcessStreamHandler;
import org.neo4j.test.TargetDirectory;

import static java.util.Arrays.asList;

import static org.junit.Assert.assertEquals;

public class TestIdJumping
{
    @Test
    public void shouldNotAllowHighIdsToJumpAfterCrash() throws Exception
    {
        // GIVEN an almost empty db that crashed
        executeSubProcess( storeDir );

        // WHEN
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( storeDir );
        try
        {
            long newNodeId, newRelationshipId;
            Transaction tx = db.beginTx();
            try
            {
                Node node = db.createNode();
                newNodeId = node.getId();
                newRelationshipId = node.createRelationshipTo( node, MyRelTypes.TEST2 ).getId();
                tx.success();
            }
            finally
            {
                tx.finish();
            }

            // THEN
            assertEquals( 3, newNodeId );
            assertEquals( 1, newRelationshipId );
        }
        finally
        {
            db.shutdown();
        }
    }

    private void executeSubProcess( String... mainArguments ) throws Exception
    {
        List<String> args = new ArrayList<String>();
        args.addAll( asList( "java", "-cp", System.getProperty( "java.class.path" ), TestIdJumping.class.getName() ) );
        args.addAll( asList( mainArguments ) );
        Process process = Runtime.getRuntime().exec( args.toArray( new String[args.size()] ) );
        ProcessStreamHandler processOutput = new ProcessStreamHandler( process, false );
        processOutput.launch();
        if ( process.waitFor() != 0 )
        {
            throw new RuntimeException( "Failed executing sub process, exit value:" + process.exitValue() );
        }
    }

    public static void main( String[] args )
    {
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( args[0] );
        Transaction tx = db.beginTx();
        try
        {
            Node node1 = db.createNode();
            Node node2 = db.createNode();
            node1.setProperty( "name", "Long long long long name that expands into a dynamic record !#¤%&/" );
            node1.createRelationshipTo( node2, MyRelTypes.TEST );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
        // Crash the database
        System.exit( 0 );
    }

    private final String storeDir = TargetDirectory.forTest( getClass() ).graphDbDir( true ).getAbsolutePath();
}