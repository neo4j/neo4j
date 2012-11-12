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

import static org.neo4j.visualization.asciidoc.AsciidocHelper.createGraphViz;
import static org.neo4j.visualization.asciidoc.AsciidocHelper.createOutputSnippet;

import java.io.File;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.annotations.Documented;

public class HelloWorldTest extends AbstractJavaDocTestbase
{
    // START SNIPPET: createReltype
    private static enum RelTypes implements RelationshipType
    {
        KNOWS
    }
    // END SNIPPET: createReltype

    private static final String DB_PATH = "target/neo4jdb-helloworld";

    /**
     * Hello World.
     * 
     * Learn how to create and access nodes, relationships and properties.
     * For information on project setup, see <<tutorials-java-embedded-setup>>.
     * 
     * Remember, from <<what-is-a-graphdb>>, that a Neo4j graph consist of:
     * 
     * * Nodes that are connected by
     * * Relationships, with
     * * Properties on both nodes and relationships.
     * 
     * All relationships have a type.
     * For example, if the graph represents a social network, a relationship type could be +KNOWS+.
     * If a relationship of the type +KNOWS+ connects two nodes, that probably represents two people that know each other.
     * A lot of the semantics (that is the meaning) of a graph is encoded in the relationship types of the application.
     * And although relationships are directed they are equally well traversed regardless of which direction they are traversed. 
     * 
     * == Prepare the database ==
     * 
     * Relationship types can be created by using an +enum+.
     * In this example we only need a single relationship type. This is how to
     * define it:
     * 
     * @@createReltype
     * 
     * The next step is to start the database server. Note that if the directory
     * given for the database doesn't already exist, it will be created.
     * 
     * @@startDb
     * 
     * Note that starting a server is an expensive operation, so don't start up
     * a new instance every time you need to interact with the
     * database! The instance can be shared by multiple threads.
     * Transactions are thread confined.
     * 
     * As seen, we register a shutdown hook that will make sure the database
     * shuts down when the JVM exits. Now it's time to interact with the
     * database.
     * 
     * == Wrap mutating operations in a transaction ==
     * 
     * All mutating transactions have to be performed in a transaction.
     * This is a conscious design decision, since we believe transaction demarcation to be an important part of working with a real enterprise database.
     * Now, transaction handling in Neo4j is very easy:
     * 
     * @@transaction
     * 
     * For more information on transactions, see <<transactions>> and
     * http://components.neo4j.org/neo4j/{neo4j-version}/apidocs/org/neo4j/graphdb/Transaction.html[Java API for +Transaction+].
     * 
     * == Create a small graph ==
     * 
     * Now, let's create a few nodes.
     * The API is very intuitive.
     * Feel free to have a look at the JavaDocs at http://components.neo4j.org/neo4j/{neo4j-version}/apidocs/.
     * They're included in the distribution, as well.
     * Here's how to create a small graph consisting of two nodes, connected with one relationship and some properties: 
     * 
     * @@addData
     * 
     * We now have a graph that looks like this: 
     * 
     * @@graph
     * 
     * == Print the result ==
     * 
     * After we've created our graph, let's read from it and print the result. 
     * 
     * @@readData
     * 
     * Which will output:
     * 
     * @@output
     * 
     * == Remove the data ==
     * 
     * In this case we'll remove the data afterwards:
     * 
     * @@removingData
     * 
     * Note that deleting a node which still has relationships when the transaction commits will fail.
     * This is to make sure relationships always have a start node and an end node.
     * 
     * == Shut down the database server ==
     * 
     * Finally, shut down the database server _when the application finishes:_
     * 
     * @@shutdownServer
     * 
     * Full source code:
     * @@github
     */
    @Test
    @Documented
    public void helloWorldExample()
    {
        gen.get()
                .addSourceSnippets( this.getClass(), "createReltype",
                        "startDb", "transaction", "addData", "removingData",
                        "shutdownServer", "readData" );
        gen.get()
                .addGithubLink( "github", this.getClass(), "neo4j/community",
                        "embedded-examples" );

        // START SNIPPET: startDb
        GraphDatabaseService graphDb = new EmbeddedGraphDatabase( DB_PATH );
        registerShutdownHook( graphDb );
        // END SNIPPET: startDb

        // START SNIPPET: transaction
        Transaction tx = graphDb.beginTx();
        try
        {
            // Mutating operations go here
            // END SNIPPET: transaction
            graphDb.getReferenceNode()
                    .delete();
            // START SNIPPET: addData
            Node firstNode = graphDb.createNode();
            Node secondNode = graphDb.createNode();
            
            Relationship relationship = firstNode.createRelationshipTo( secondNode, RelTypes.KNOWS );

            firstNode.setProperty( "message", "Hello, " );
            secondNode.setProperty( "message", "world!" );
            relationship.setProperty( "message", "brave Neo4j " );
            // END SNIPPET: addData

            // START SNIPPET: readData
            System.out.print( firstNode.getProperty( "message" ) );
            System.out.print( relationship.getProperty( "message" ) );
            System.out.print( secondNode.getProperty( "message" ) );
            // END SNIPPET: readData

            String greeting = ( (String) firstNode.getProperty( "message" ) )
                              + ( (String) relationship.getProperty( "message" ) )
                              + ( (String) secondNode.getProperty( "message" ) );
            gen.get()
                    .addSnippet( "output", createOutputSnippet( greeting ) );
            // START SNIPPET: transaction
            tx.success();
        }
        finally
        {
            tx.finish();
        }
        // END SNIPPET: transaction

        gen.get()
                .addSnippet( "graph",
                        createGraphViz( "Hello World Graph", graphDb, gen.get()
                                .getTitle() ) );

        tx = graphDb.beginTx();
        try
        {
            // START SNIPPET: removingData
            // let's remove the data
            for ( Node node : graphDb.getAllNodes() )
            {
                for ( Relationship rel : node.getRelationships() )
                {
                    rel.delete();
                }
                node.delete();
            }
            // END SNIPPET: removingData
            tx.success();
        }
        finally
        {
            tx.finish();
        }

        System.out.println( "Shutting down database ..." );
        // START SNIPPET: shutdownServer
        graphDb.shutdown();
        // END SNIPPET: shutdownServer
    }

    // START SNIPPET: shutdownHook
    private static void registerShutdownHook( final GraphDatabaseService graphDb )
    {
        // Registers a shutdown hook for the Neo4j instance so that it
        // shuts down nicely when the VM exits (even if you "Ctrl-C" the
        // running example before it's completed)
        Runtime.getRuntime()
                .addShutdownHook( new Thread()
                {
                    @Override
                    public void run()
                    {
                        graphDb.shutdown();
                    }
                } );
    }
    // END SNIPPET: shutdownHook

    /**
     * Make sure the DB directory doesn't exist.
     */
    @Before
    public void setup()
    {
        deleteRecursively( new File( DB_PATH ) );
    }

    private static void deleteRecursively( File file )
    {
        if ( !file.exists() )
        {
            return;
        }

        if ( file.isDirectory() )
        {
            for ( File child : file.listFiles() )
            {
                deleteRecursively( child );
            }
        }
        if ( !file.delete() )
        {
            throw new RuntimeException(
                    "Couldn't empty database. Offending file:" + file );
        }
    }
}
