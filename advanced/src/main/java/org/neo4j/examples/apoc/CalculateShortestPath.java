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

package org.neo4j.examples.apoc;

import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.index.IndexService;
import org.neo4j.index.lucene.LuceneIndexService;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.Traversal;

import java.io.File;

public class CalculateShortestPath
{
    private static final String DB_PATH = "neo4j-shortest-path";
    private static final String NAME_KEY = "name";
    private static RelationshipType KNOWS = DynamicRelationshipType.withName( "KNOWS" );

    private static GraphDatabaseService graphDb;
    private static IndexService indexService;

    public static void main( final String[] args )
    {
        deleteFileOrDirectory( new File( DB_PATH ) );
        graphDb = new EmbeddedGraphDatabase( DB_PATH );
        indexService = new LuceneIndexService( graphDb );
        registerShutdownHook();
        Transaction tx = graphDb.beginTx();
        try
        {
            /*
             *  (Neo) --> (Trinity)
             *     \       ^
             *      v     /
             *    (Morpheus) --> (Cypher)
             *            \         |
             *             v        v
             *            (Agent Smith)
             */
            createChain( "Neo", "Trinity" );
            createChain( "Neo", "Morpheus", "Trinity" );
            createChain( "Morpheus", "Cypher", "Agent Smith" );
            createChain( "Morpheus", "Agent Smith" );
            tx.success();
        }
        finally
        {
            tx.finish();
        }

        // So let's find the shortest path between Neo and Agent Smith
        Node neo = getOrCreateNode( "Neo" );
        Node agentSmith = getOrCreateNode( "Agent Smith" );
        // START SNIPPET: shortestPathUsage
        PathFinder<Path> finder = GraphAlgoFactory.shortestPath(
                Traversal.expanderForTypes( KNOWS, Direction.BOTH ), 4 );
        Path foundPath = finder.findSinglePath( neo, agentSmith );
        System.out.println( "Path from Neo to Agent Smith: " +
                Traversal.simplePathToString( foundPath, NAME_KEY ) );
        // END SNIPPET: shortestPathUsage

        System.out.println( "Shutting down database ..." );
        indexService.shutdown();
        graphDb.shutdown();
    }

    private static void createChain( String... names )
    {
        for ( int i = 0; i < names.length - 1; i++ )
        {
            Node firstNode = getOrCreateNode( names[ i ] );
            Node secondNode = getOrCreateNode( names[ i + 1 ] );
            firstNode.createRelationshipTo( secondNode, KNOWS );
        }
    }

    private static Node getOrCreateNode( String name )
    {
        Node node = indexService.getSingleNode( NAME_KEY, name );
        if ( node == null )
        {
            node = graphDb.createNode();
            node.setProperty( NAME_KEY, name );
            indexService.index( node, NAME_KEY, name );
        }
        return node;
    }

    private static void registerShutdownHook()
    {
        // Registers a shutdown hook for the Neo4j instance so that it
        // shuts down nicely when the VM exits (even if you "Ctrl-C" the
        // running example before it's completed)
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            @Override
            public void run()
            {
                indexService.shutdown();
                graphDb.shutdown();
            }
        } );
    }

    private static void deleteFileOrDirectory( File file )
    {
        if ( file.exists() )
        {
            if ( file.isDirectory() )
            {
                for ( File child : file.listFiles() )
                {
                    deleteFileOrDirectory( child );
                }
            }
            file.delete();
        }
    }
}
