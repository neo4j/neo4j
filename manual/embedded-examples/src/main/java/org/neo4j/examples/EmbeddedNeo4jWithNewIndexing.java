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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.io.fs.FileUtils;

import static org.neo4j.helpers.collection.Iterators.loop;

public class EmbeddedNeo4jWithNewIndexing
{
    private static final File DB_PATH = new File( "target/neo4j-store-with-new-indexing" );

    public static void main( final String[] args ) throws IOException
    {
        System.out.println( "Starting database ..." );
        FileUtils.deleteRecursively( DB_PATH );

        // START SNIPPET: startDb
        GraphDatabaseService graphDb = new GraphDatabaseFactory().newEmbeddedDatabase( DB_PATH );
        // END SNIPPET: startDb

        {
            // START SNIPPET: createIndex
            IndexDefinition indexDefinition;
            try ( Transaction tx = graphDb.beginTx() )
            {
                Schema schema = graphDb.schema();
                indexDefinition = schema.indexFor( Label.label( "User" ) )
                        .on( "username" )
                        .create();
                tx.success();
            }
            // END SNIPPET: createIndex
            // START SNIPPET: wait
            try ( Transaction tx = graphDb.beginTx() )
            {
                Schema schema = graphDb.schema();
                schema.awaitIndexOnline( indexDefinition, 10, TimeUnit.SECONDS );
            }
            // END SNIPPET: wait
            // START SNIPPET: progress
            try ( Transaction tx = graphDb.beginTx() )
            {
                Schema schema = graphDb.schema();
                System.out.println( String.format( "Percent complete: %1.0f%%",
                        schema.getIndexPopulationProgress( indexDefinition ).getCompletedPercentage() ) );
            }
            // END SNIPPET: progress
        }

        {
            // START SNIPPET: addUsers
            try ( Transaction tx = graphDb.beginTx() )
            {
                Label label = Label.label( "User" );

                // Create some users
                for ( int id = 0; id < 100; id++ )
                {
                    Node userNode = graphDb.createNode( label );
                    userNode.setProperty( "username", "user" + id + "@neo4j.org" );
                }
                System.out.println( "Users created" );
                tx.success();
            }
            // END SNIPPET: addUsers
        }

        {
            // START SNIPPET: findUsers
            Label label = Label.label( "User" );
            int idToFind = 45;
            String nameToFind = "user" + idToFind + "@neo4j.org";
            try ( Transaction tx = graphDb.beginTx() )
            {
                try ( ResourceIterator<Node> users =
                              graphDb.findNodes( label, "username", nameToFind ) )
                {
                    ArrayList<Node> userNodes = new ArrayList<>();
                    while ( users.hasNext() )
                    {
                        userNodes.add( users.next() );
                    }

                    for ( Node node : userNodes )
                    {
                        System.out.println(
                                "The username of user " + idToFind + " is " + node.getProperty( "username" ) );
                    }
                }
            }
            // END SNIPPET: findUsers
        }

        {
            // START SNIPPET: resourceIterator
            Label label = Label.label( "User" );
            int idToFind = 45;
            String nameToFind = "user" + idToFind + "@neo4j.org";
            try ( Transaction tx = graphDb.beginTx();
                  ResourceIterator<Node> users = graphDb.findNodes( label, "username", nameToFind ) )
            {
                Node firstUserNode;
                if ( users.hasNext() )
                {
                    firstUserNode = users.next();
                }
                users.close();
            }
            // END SNIPPET: resourceIterator
        }

        {
            // START SNIPPET: updateUsers
            try ( Transaction tx = graphDb.beginTx() )
            {
                Label label = Label.label( "User" );
                int idToFind = 45;
                String nameToFind = "user" + idToFind + "@neo4j.org";

                for ( Node node : loop( graphDb.findNodes( label, "username", nameToFind ) ) )
                {
                    node.setProperty( "username", "user" + (idToFind + 1) + "@neo4j.org" );
                }
                tx.success();
            }
            // END SNIPPET: updateUsers
        }

        {
            // START SNIPPET: deleteUsers
            try ( Transaction tx = graphDb.beginTx() )
            {
                Label label = Label.label( "User" );
                int idToFind = 46;
                String nameToFind = "user" + idToFind + "@neo4j.org";

                for ( Node node : loop( graphDb.findNodes( label, "username", nameToFind ) ) )
                {
                    node.delete();
                }
                tx.success();
            }
            // END SNIPPET: deleteUsers
        }

        {
            // START SNIPPET: dropIndex
            try ( Transaction tx = graphDb.beginTx() )
            {
                Label label = Label.label( "User" );
                for ( IndexDefinition indexDefinition : graphDb.schema()
                        .getIndexes( label ) )
                {
                    // There is only one index
                    indexDefinition.drop();
                }

                tx.success();
            }
            // END SNIPPET: dropIndex
        }

        System.out.println( "Shutting down database ..." );
        // START SNIPPET: shutdownDb
        graphDb.shutdown();
        // END SNIPPET: shutdownDb
    }
}
