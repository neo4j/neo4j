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

import static org.junit.Assert.assertEquals;
import static org.neo4j.visualization.asciidoc.AsciidocHelper.createGraphVizWithNodeId;
import static org.neo4j.visualization.asciidoc.AsciidocHelper.createOutputSnippet;

import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.graphdb.traversal.Uniqueness;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.test.GraphDescription.Graph;

public class UniquenessOfPathsDocTest extends ImpermanentGraphJavaDocTestBase
{
    /**
     * Uniqueness of Paths in traversals.
     * 
     * This example is demonstrating the use of node uniqueness.
     * Below an imaginary domain graph with Principals
     * that own pets that are descendant to other pets.
     * 
     * @@graph
     * 
     * In order to return all descendants
     * of +Pet0+ which have the relation +owns+ to +Principal1+ (+Pet1+ and +Pet3+),
     * the Uniqueness of the traversal needs to be set to
     * +NODE_PATH+ rather than the default +NODE_GLOBAL+ so that nodes
     * can be traversed more that once, and paths that have
     * different nodes but can have some nodes in common (like the
     * start and end node) can be returned.
     * 
     * @@traverser
     * 
     * This will return the following paths:
     * 
     * @@output
     * 
     * In the default `path.toString()` implementation, `(1)--[knows,2]-->(4)` denotes
     * a node with ID=1 having a relationship with ID 2 or type `knows` to a node with ID-4.
     * 
     * Let's create a new +TraversalDescription+ from the old one,
     * having +NODE_GLOBAL+ uniqueness to see the difference.
     * 
     * TIP: The +TraversalDescription+ object is immutable,
     *      so we have to use the new instance returned
     *      with the new uniqueness setting.
     * 
     * @@traverseNodeGlobal
     * 
     * Now only one path is returned:
     * 
     * @@outNodeGlobal
     */
    @Graph({"Pet0 descendant Pet1",
        "Pet0 descendant Pet2",
        "Pet0 descendant Pet3",
        "Principal1 owns Pet1",
        "Principal2 owns Pet2",
        "Principal1 owns Pet3"})
    @Test
    @Documented
    public void pathUniquenessExample()
    {
        Node start = data.get().get( "Pet0" );
        gen.get().addSnippet( "graph", createGraphVizWithNodeId("Descendants Example Graph", graphdb(), gen.get().getTitle()) );
        gen.get();
        gen.get().addTestSourceSnippets( this.getClass(), "traverser", "traverseNodeGlobal" );
        // START SNIPPET: traverser
        final Node target = data.get().get( "Principal1" );
        TraversalDescription td = db.traversalDescription()
                .uniqueness( Uniqueness.NODE_PATH )
                .evaluator( new Evaluator()
        {
            @Override
            public Evaluation evaluate( Path path )
            {
                boolean endNodeIsTarget = path.endNode().equals( target );
                return Evaluation.of( endNodeIsTarget, !endNodeIsTarget );
            }
        } );
        
        Traverser results = td.traverse( start );
        // END SNIPPET: traverser
        String output = "";
        int count = 0;
        //we should get two paths back, through Pet1 and Pet3
        try ( Transaction ignore = db.beginTx() )
        {
            for ( Path path : results )
            {
                count++;
                output += path.toString() + "\n";
            }
        }
        gen.get().addSnippet( "output", createOutputSnippet( output ) );
        assertEquals( 2, count );

        // START SNIPPET: traverseNodeGlobal
        TraversalDescription nodeGlobalTd = td.uniqueness( Uniqueness.NODE_GLOBAL );
        results = nodeGlobalTd.traverse( start );
        // END SNIPPET: traverseNodeGlobal
        String output2 = "";
        count = 0;
        // we should get two paths back, through Pet1 and Pet3
        try ( Transaction tx = db.beginTx() )
        {
            for ( Path path : results )
            {
                count++;
                output2 += path.toString() + "\n";
            }
        }
        gen.get().addSnippet( "outNodeGlobal", createOutputSnippet( output2 ) );
        assertEquals( 1, count );
    }
}
