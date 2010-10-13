/**
 * Copyright (c) 2002-2010 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package org.neo4j.examples;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.EmbeddedGraphDatabase;

public class EmbeddedNeo4j
{
    private static final String DB_PATH = "neo4j-store";
    private static final String NAME_KEY = "name";

    // START SNIPPET: createReltype
    private static enum ExampleRelationshipTypes implements RelationshipType
    {
        EXAMPLE
    }
    // END SNIPPET: createReltype

    public static void main( final String[] args )
    {
        // START SNIPPET: startDb
        GraphDatabaseService graphDb = new EmbeddedGraphDatabase( DB_PATH );
        registerShutdownHook( graphDb );
        // END SNIPPET: startDb

        // START SNIPPET: operationsInATransaction
        // Encapsulate operations in a transaction
        Transaction tx = graphDb.beginTx();
        try
        {
            Node firstNode = graphDb.createNode();
            firstNode.setProperty( NAME_KEY, "Hello" );
            Node secondNode = graphDb.createNode();
            secondNode.setProperty( NAME_KEY, "World" );

            firstNode.createRelationshipTo( secondNode,
                ExampleRelationshipTypes.EXAMPLE );

            String greeting = firstNode.getProperty( NAME_KEY ) + " "
                + secondNode.getProperty( NAME_KEY );
            System.out.println( greeting );
            // END SNIPPET: operationsInATransaction

            // START SNIPPET: removingData
            // let's remove the data before committing
            firstNode.getSingleRelationship( ExampleRelationshipTypes.EXAMPLE,
                    Direction.OUTGOING ).delete();
            firstNode.delete();
            secondNode.delete();

            tx.success();
        }
        finally
        {
            tx.finish();
        }
        // END SNIPPET: removingData

        System.out.println( "Shutting down database ..." );
        // START SNIPPET: shutdownServer
        graphDb.shutdown();
        // END SNIPPET: shutdownServer
    }

    private static void registerShutdownHook( final GraphDatabaseService graphDb )
    {
        // Registers a shutdown hook for the Neo4j instance so that it
        // shuts down nicely when the VM exits (even if you "Ctrl-C" the
        // running example before it's completed)
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            @Override
            public void run()
            {
                graphDb.shutdown();
            }
        } );
    }
}
