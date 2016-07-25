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
package org.neo4j.examples.orderedpath;

import java.io.File;
import java.util.ArrayList;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.Paths;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.graphdb.traversal.Uniqueness;

import static org.neo4j.graphdb.RelationshipType.withName;

public class OrderedPath
{
    private static final RelationshipType REL1 = withName( "REL1" ), REL2 = withName( "REL2" ),
            REL3 = withName( "REL3" );
    static final File DB_PATH = new File( "target/neo4j-orderedpath-db" );
    GraphDatabaseService db;

    public OrderedPath( GraphDatabaseService db )
    {
        this.db = db;
    }

    public static void main( String[] args )
    {
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( DB_PATH );
        OrderedPath op = new OrderedPath( db );
        op.shutdownGraph();
    }

    public Node createTheGraph()
    {
        try ( Transaction tx = db.beginTx() )
        {
            // START SNIPPET: createGraph
            Node A = db.createNode();
            Node B = db.createNode();
            Node C = db.createNode();
            Node D = db.createNode();

            A.createRelationshipTo( C, REL2 );
            C.createRelationshipTo( D, REL3 );
            A.createRelationshipTo( B, REL1 );
            B.createRelationshipTo( C, REL2 );
            // END SNIPPET: createGraph
            A.setProperty( "name", "A" );
            B.setProperty( "name", "B" );
            C.setProperty( "name", "C" );
            D.setProperty( "name", "D" );
            tx.success();
            return A;
        }
    }

    public void shutdownGraph()
    {
        try
        {
            if ( db != null )
            {
                db.shutdown();
            }
        }
        finally
        {
            db = null;
        }
    }

    public TraversalDescription findPaths()
    {
        // START SNIPPET: walkOrderedPath
        final ArrayList<RelationshipType> orderedPathContext = new ArrayList<RelationshipType>();
        orderedPathContext.add( REL1 );
        orderedPathContext.add( withName( "REL2" ) );
        orderedPathContext.add( withName( "REL3" ) );
        TraversalDescription td = db.traversalDescription()
                .evaluator( new Evaluator()
                {
                    @Override
                    public Evaluation evaluate( final Path path )
                    {
                        if ( path.length() == 0 )
                        {
                            return Evaluation.EXCLUDE_AND_CONTINUE;
                        }
                        RelationshipType expectedType = orderedPathContext.get( path.length() - 1 );
                        boolean isExpectedType = path.lastRelationship()
                                .isType( expectedType );
                        boolean included = path.length() == orderedPathContext.size() && isExpectedType;
                        boolean continued = path.length() < orderedPathContext.size() && isExpectedType;
                        return Evaluation.of( included, continued );
                    }
                } )
                .uniqueness( Uniqueness.NODE_PATH );
        // END SNIPPET: walkOrderedPath
        return td;
    }

    String printPaths( TraversalDescription td, Node A )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            String output = "";
            // START SNIPPET: printPath
            Traverser traverser = td.traverse( A );
            PathPrinter pathPrinter = new PathPrinter( "name" );
            for ( Path path : traverser )
            {
                output += Paths.pathToString( path, pathPrinter );
            }
            // END SNIPPET: printPath
            output += "\n";
            return output;
        }
    }

    // START SNIPPET: pathPrinter
    static class PathPrinter implements Paths.PathDescriptor<Path>
    {
        private final String nodePropertyKey;

        public PathPrinter( String nodePropertyKey )
        {
            this.nodePropertyKey = nodePropertyKey;
        }

        @Override
        public String nodeRepresentation( Path path, Node node )
        {
            return "(" + node.getProperty( nodePropertyKey, "" ) + ")";
        }

        @Override
        public String relationshipRepresentation( Path path, Node from, Relationship relationship )
        {
            String prefix = "--", suffix = "--";
            if ( from.equals( relationship.getEndNode() ) )
            {
                prefix = "<--";
            }
            else
            {
                suffix = "-->";
            }
            return prefix + "[" + relationship.getType().name() + "]" + suffix;
        }
    }
    // END SNIPPET: pathPrinter
}
