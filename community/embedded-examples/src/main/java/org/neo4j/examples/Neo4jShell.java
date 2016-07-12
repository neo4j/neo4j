/*
 * Licensed to Neo Technology under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Neo Technology licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.neo4j.examples;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.Index;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.shell.ShellLobby;
import org.neo4j.shell.ShellServer;
import org.neo4j.shell.ShellSettings;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;

public class Neo4jShell
{
    private static final String DB_PATH = "neo4j-store";
    private static final String USERNAME_KEY = "username";
    private static GraphDatabaseService graphDb;

    private static enum RelTypes implements RelationshipType
    {
        USERS_REFERENCE, USER, KNOWS,
    }

    public static void main( final String[] args ) throws Exception
    {
        registerShutdownHookForNeo();
        boolean trueForLocal = waitForUserInput(
                "Would you like to start a "
                        + "local shell instance or enable neo4j to accept remote "
                        + "connections [l/r]? " ).equalsIgnoreCase( "l" );
        if ( trueForLocal )
        {
            startLocalShell();
        }
        else
        {
            startRemoteShellAndWait();
        }
        shutdown();
    }

    private static void startLocalShell() throws Exception
    {
        graphDb = new GraphDatabaseFactory().newEmbeddedDatabase( DB_PATH );
        createExampleGraph();
        ShellServer shellServer = new GraphDatabaseShellServer( (org.neo4j.kernel.GraphDatabaseAPI) graphDb );
        ShellLobby.newClient( shellServer ).grabPrompt();
        shellServer.shutdown();
    }

    private static void startRemoteShellAndWait() throws Exception
    {
        graphDb = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( DB_PATH )
                .
                setConfig( ShellSettings.remote_shell_enabled, Settings.TRUE ).
                newGraphDatabase();

        createExampleGraph();
        waitForUserInput( "Remote shell enabled, connect to it by executing\n"
                + "the shell-client script in a separate terminal."
                + "The script is located in the bin directory.\n"
                + "\nWhen you're done playing around, just press [Enter] "
                + "in this terminal " );
    }

    private static String waitForUserInput( final String textToSystemOut )
            throws Exception
    {
        System.out.print( textToSystemOut );
        return new BufferedReader( new InputStreamReader( System.in, "UTF-8" ) )
                .readLine();
    }

    private static void createExampleGraph()
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            // Create users type node
            System.out.println( "Creating example graph ..." );
            Random random = new Random();
            Node usersReferenceNode = graphDb.createNode();
            Index<Node> references = graphDb.index().forNodes( "references" );
            usersReferenceNode.setProperty( "reference", "users" );
            references.add( usersReferenceNode, "reference", "users" );
            // Create some users and index their names with the IndexService
            List<Node> users = new ArrayList<Node>();
            for ( int id = 0; id < 100; id++ )
            {
                Node userNode = createUser( formUserName( id ) );
                usersReferenceNode.createRelationshipTo( userNode,
                        RelTypes.USER );
                if ( id > 10 )
                {
                    int numberOfFriends = random.nextInt( 5 );
                    Set<Node> knows = new HashSet<Node>();
                    for ( int i = 0; i < numberOfFriends; i++ )
                    {
                        Node friend = users
                                .get( random.nextInt( users.size() ) );
                        if ( knows.add( friend ) )
                        {
                            userNode.createRelationshipTo( friend,
                                    RelTypes.KNOWS );
                        }
                    }
                }
                users.add( userNode );
            }
            tx.success();
        }
    }

    private static void deleteExampleNodeSpace()
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            // Delete the persons and remove them from the index
            System.out.println( "Deleting example graph ..." );
            Node usersReferenceNode = graphDb.index().forNodes( "references" ).get( "reference", "users" ).getSingle();
            for ( Relationship relationship : usersReferenceNode
                    .getRelationships( RelTypes.USER, Direction.OUTGOING ) )
            {
                Node user = relationship.getEndNode();
                for ( Relationship knowsRelationship : user
                        .getRelationships( RelTypes.KNOWS ) )
                {
                    knowsRelationship.delete();
                }
                user.delete();
                relationship.delete();
            }
            usersReferenceNode.getSingleRelationship( RelTypes.USERS_REFERENCE,
                    Direction.INCOMING ).delete();
            usersReferenceNode.delete();
            tx.success();
        }
    }

    private static void shutdownGraphDb()
    {
        System.out.println( "Shutting down database ..." );
        graphDb.shutdown();
        graphDb = null;
    }

    private static void shutdown()
    {
        if ( graphDb != null )
        {
            deleteExampleNodeSpace();
            shutdownGraphDb();
        }
    }

    private static void registerShutdownHookForNeo()
    {
        // Registers a shutdown hook for the Neo4j instance so that it
        // shuts down nicely when the VM exits (even if you "Ctrl-C" the
        // running example before it's completed)
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            @Override
            public void run()
            {
                shutdown();
            }
        } );
    }

    private static String formUserName( final int id )
    {
        return "user" + id + "@neo4j.org";
    }

    private static Node createUser( final String username )
    {
        Node node = graphDb.createNode();
        node.setProperty( USERNAME_KEY, username );
        return node;
    }
}
