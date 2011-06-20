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
package org.neo4j.examples.orderedpath;

import static org.neo4j.graphdb.DynamicRelationshipType.withName;

import java.util.ArrayList;

import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.Traversal;

public class OrderedPathTest
{
    private static EmbeddedGraphDatabase db;
    private static RelationshipType REL1 = withName( "REL1" ),
    REL2 = withName( "REL2" ), REL3 = withName( "REL3" );

    @BeforeClass
    public static void createTheGraph()
    {
        db = new EmbeddedGraphDatabase( "target/db" );
        Transaction tx = db.beginTx();
        // START SNIPPET: createGraph
        Node A = db.createNode();
        Node B = db.createNode();
        Node C = db.createNode();
        Node D = db.createNode();
        A.createRelationshipTo( B, REL1 );
        B.createRelationshipTo( C, REL2 );
        C.createRelationshipTo( D, REL3 );
        A.createRelationshipTo( C, REL2 );
        // END SNIPPET: createGraph
        A.setProperty( "name", "A" );
        B.setProperty( "name", "B" );
        C.setProperty( "name", "C" );
        D.setProperty( "name", "D" );
        tx.success();
        tx.finish();
    }

    @Test
    public void testPath()
    {
        // START SNIPPET: walkOrderedPath
        final ArrayList<RelationshipType> orderedPathContext = new ArrayList<RelationshipType>();
        orderedPathContext.add( REL1 );
        orderedPathContext.add( withName( "REL2" ) );
        orderedPathContext.add( withName( "REL3" ) );
        TraversalDescription td = Traversal.description().evaluator(
                new Evaluator()
                {
                    @Override
                    public Evaluation evaluate( final Path path )
                    {
                        if ( path.length() == 0 )
                        {
                            return Evaluation.EXCLUDE_AND_CONTINUE;
                        }
                        String currentName = path.lastRelationship().getType().name();
                        String relationshipType = orderedPathContext.get(
                                path.length() - 1 ).name();
                        if ( path.length() == orderedPathContext.size() )
                        {
                            if ( currentName.equals( relationshipType ) )
                            {
                                return Evaluation.INCLUDE_AND_PRUNE;
                            }
                            else
                            {
                                return Evaluation.EXCLUDE_AND_PRUNE;
                            }
                        }
                        else
                        {
                            if ( currentName.equals( relationshipType ) )
                            {
                                return Evaluation.EXCLUDE_AND_CONTINUE;
                            }
                            else
                            {
                                return Evaluation.EXCLUDE_AND_PRUNE;
                            }
                        }
                    }
                } );
        Traverser t = td.traverse( db.getNodeById( 1 ) );
        for ( Path path : t )
        {
            System.out.println( path );
        }
        // END SNIPPET: walkOrderedPath
    }
}
