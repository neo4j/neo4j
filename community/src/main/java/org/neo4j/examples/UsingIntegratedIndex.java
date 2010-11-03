/** 
 * Licensed to Neo Technology under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Neo Technology licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.neo4j.examples;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.index.IndexService;
import org.neo4j.kernel.EmbeddedGraphDatabase;

/**
 * As of neo4j-1.2.M02, the {@link org.neo4j.graphdb.GraphDatabaseService} is
 * always paired with an {@link org.neo4j.graphdb.index.IndexManager} for
 * creating and managing indexes on Nodes and Relationships. It is a new index
 * framework set to replace the {@link IndexService} interface and is tighter
 * integrated with the graph database.
 */
public class UsingIntegratedIndex
{
    private static final String DB_PATH = "neo4j-store";
    private static final String USERNAME_KEY = "username";
    private static GraphDatabaseService graphDb;
    private static final String INDEX_NAME = "user-index";
    private static Index<Node> nodeIndex;

    private static enum RelTypes implements RelationshipType
    {
        USERS_REFERENCE,
        USER
    }

    public static void main( final String[] args )
    {
        // START SNIPPET: createIndex
        graphDb = new EmbeddedGraphDatabase( DB_PATH );
        nodeIndex = graphDb.index().forNodes( INDEX_NAME );
        registerShutdownHook();
        // END SNIPPET: createIndex

        Transaction tx = graphDb.beginTx();
        try
        {
            // Create users sub reference node (see design guidelines on
            // http://wiki.neo4j.org/ )
            Node usersReferenceNode = graphDb.createNode();
            graphDb.getReferenceNode().createRelationshipTo(
                usersReferenceNode, RelTypes.USERS_REFERENCE );
            // Create some users and index their names with the IndexService
            for ( int id = 0; id < 100; id++ )
            {
                Node userNode = createAndIndexUser( idToUserName( id ) );
                usersReferenceNode.createRelationshipTo( userNode,
                    RelTypes.USER );
            }
            System.out.println( "Users created" );

            // Find a user through the search index
            int idToFind = 45;
            Node foundUser = nodeIndex.get( USERNAME_KEY, idToUserName( idToFind) ).getSingle();

            System.out.println( "The username of user " + idToFind + " is "
                + foundUser.getProperty( USERNAME_KEY ) );

            // Delete the persons and remove them from the index
            for ( Relationship relationship : usersReferenceNode.getRelationships(
                    RelTypes.USER, Direction.OUTGOING ) )
            {
                Node user = relationship.getEndNode();
                nodeIndex.remove(  user, USERNAME_KEY,
                        user.getProperty( USERNAME_KEY ) );
                user.delete();
                relationship.delete();
            }
            usersReferenceNode.getSingleRelationship( RelTypes.USERS_REFERENCE,
                    Direction.INCOMING ).delete();
            usersReferenceNode.delete();
            tx.success();
        }
        finally
        {
            tx.finish();
        }
        System.out.println( "Shutting down database ..." );
        shutdown();
    }

    static void shutdown()
    {
        // START SNIPPET: shutdownDatabase
        graphDb.shutdown();
        // END SNIPPET: shutdownDatabase
    }

    private static String idToUserName( final int id )
    {
        return "user" + id + "@neo4j.org";
    }

    private static Node createAndIndexUser( final String username )
    {
        // START SNIPPET: indexNode
        Node node = graphDb.createNode();
        node.setProperty( USERNAME_KEY, username );
        nodeIndex.add( node, USERNAME_KEY, username );
        // END SNIPPET: indexNode
        return node;
    }

    private static void registerShutdownHook()
    {
        // Registers a shutdown hook for the Neo4j and index service instances
        // so that it shuts down nicely when the VM exits (even if you
        // "Ctrl-C" the running example before it's completed)
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            @Override
            public void run()
            {
                shutdown();
            }
        } );
    }
}
