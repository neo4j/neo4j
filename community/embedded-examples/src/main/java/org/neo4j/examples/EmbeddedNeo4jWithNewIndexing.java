/**
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

import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;

public class EmbeddedNeo4jWithNewIndexing
{
    private static final String DB_PATH = "target/neo4j-store";

    public static void main( final String[] args )
    {
        System.out.println( "Starting database ..." );

        // START SNIPPET: startDb
        GraphDatabaseService graphDb = new GraphDatabaseFactory().newEmbeddedDatabase( DB_PATH );
        // END SNIPPET: startDb

        {
            // START SNIPPET: createIndex
            Schema schema = graphDb.schema();
            IndexDefinition indexDefinition;
            Transaction tx = graphDb.beginTx();
            try
            {
                indexDefinition = schema.indexFor( DynamicLabel.label( "User" ) )
                        .on( "username" )
                        .create();
                tx.success();
            }
            finally
            {
                tx.finish();
            }
            // END SNIPPET: createIndex
            // START SNIPPET: wait
            Transaction transaction = graphDb.beginTx();
            try
            {
                schema.awaitIndexOnline( indexDefinition, 10, TimeUnit.SECONDS );
            }
            finally
            {
                transaction.finish();
            }
            // END SNIPPET: wait
        }

        {
            // START SNIPPET: addUsers
            Transaction tx = graphDb.beginTx();
            try
            {
                Label label = DynamicLabel.label( "User" );

                // Create some users and index their names with the new
                // IndexingService
                for ( int id = 0; id < 100; id++ )
                {
                    Node userNode = graphDb.createNode( label );
                    userNode.setProperty( "username", "user" + id + "@neo4j.org" );
                }
                System.out.println( "Users created" );
                tx.success();
            }
            finally
            {
                tx.finish();
            }
            // END SNIPPET: addUsers
        }

        {
            // START SNIPPET: findUsers
            Label label = DynamicLabel.label( "User" );
            int idToFind = 45;
            String nameToFind = "user" + idToFind + "@neo4j.org";
            Transaction transaction = graphDb.beginTx();
            try
            {
                ResourceIterator<Node> users = graphDb.findNodesByLabelAndProperty( label, "username", nameToFind )
                        .iterator();
                while ( users.hasNext() )
                {
                    Node node = users.next();
                    System.out.println( "The username of user " + idToFind + " is " + node.getProperty( "username" ) );
                }
            }
            finally
            {
                // alternatively use a transaction
                transaction.finish();
            }
            // END SNIPPET: findUsers
        }

        {
            // START SNIPPET: updateUsers
            Transaction tx = graphDb.beginTx();
            try
            {
                Label label = DynamicLabel.label( "User" );
                int idToFind = 45;
                String nameToFind = "user" + idToFind + "@neo4j.org";

                for ( Node node : graphDb.findNodesByLabelAndProperty( label, "username", nameToFind ) )
                {
                    node.setProperty( "username", "user" + ( idToFind + 1 ) + "@neo4j.org" );
                }
                tx.success();
            }
            finally
            {
                tx.finish();
            }
            // END SNIPPET: updateUsers
        }

        {
            // START SNIPPET: deleteUsers
            Transaction tx = graphDb.beginTx();
            try
            {
                Label label = DynamicLabel.label( "User" );
                int idToFind = 46;
                String nameToFind = "user" + idToFind + "@neo4j.org";

                for ( Node node : graphDb.findNodesByLabelAndProperty( label, "username", nameToFind ) )
                {
                    node.delete();
                }
                tx.success();
            }
            finally
            {
                tx.finish();
            }
            // END SNIPPET: deleteUsers
        }

        {
            // START SNIPPET: dropIndex
            Transaction tx = graphDb.beginTx();
            try
            {
                Label label = DynamicLabel.label( "User" );
                for ( IndexDefinition indexDefinition : graphDb.schema()
                        .getIndexes( label ) )
                {
                    // There is only one index
                    indexDefinition.drop();
                }

                tx.success();
            }
            finally
            {
                tx.finish();
            }
            // END SNIPPET: dropIndex
        }

        System.out.println( "Shutting down database ..." );
        // START SNIPPET: shutdownDb
        graphDb.shutdown();
        // END SNIPPET: shutdownDb
    }
}
