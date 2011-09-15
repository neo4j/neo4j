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

import static org.junit.Assert.assertEquals;

import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.kernel.Traversal;
import org.neo4j.kernel.Uniqueness;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.test.GraphDescription;
import org.neo4j.test.GraphDescription.Graph;
import org.neo4j.test.GraphHolder;
import org.neo4j.test.ImpermanentGraphDatabase;
import org.neo4j.test.JavaTestDocsGenerator;
import org.neo4j.test.TestData;
import org.neo4j.visualization.asciidoc.AsciidocHelper;

public class ShortDocumentationExamplesTest implements GraphHolder
{
    public @Rule
    TestData<JavaTestDocsGenerator> gen = TestData.producedThrough( JavaTestDocsGenerator.PRODUCER );
    public @Rule
    TestData<Map<String, Node>> data = TestData.producedThrough( GraphDescription.createGraphFor(
            this, true ) );
 
    /**
     * Uniqueness of Paths in traversals.
     * 
     * This example is demonstrating the use of node uniqueness.
     * Below an imaginary domain graph with Principals
     * that own pets that are descendant to other pets.
     * 
     * @@graph
     * 
     * In order to return which all descendants 
     * of +Pet0+ which have the relation +owns+ to Principal1 (+Pet1+ and +Pet3+),
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
     * [source]
     * ----
     * @@output
     * ----
     */
    @Graph({"Pet0 descendant Pet1",
        "Pet0 descendant Pet2",
        "Pet0 descendant Pet3",
        "Principal1 owns Pet1",
        "Principal2 owns Pet2",
        "Principal1 owns Pet3"})
    @Test
    @Documented
    public void pathUniquenesExample()
    {
        Node start = data.get().get( "Pet0" );
        gen.get().addSnippet( "graph", AsciidocHelper.createGraphViz("descendants1", graphdb(), gen.get().getTitle()) );
        String tagName = "traverser";
        gen.get().addSnippet( tagName, gen.get().createSourceSnippet(tagName, this.getClass()) );
        // START SNIPPET: traverser
        final Node target = data.get().get( "Principal1" );
        TraversalDescription td = Traversal.description().uniqueness(Uniqueness.NODE_PATH ).evaluator( new Evaluator()
        {
            @Override
            public Evaluation evaluate( Path path )
            {
                if(path.endNode().equals( target )) {
                    return Evaluation.INCLUDE_AND_PRUNE;
                }
                return Evaluation.EXCLUDE_AND_CONTINUE;
            }
        } );
        
        Traverser results = td.traverse( start );
        // END SNIPPET: traverser
        String output = "";
        int count = 0;
        //we should get two paths back, through Pet1 and Pet3
        for(Path path : results)
        {       
            count++;
            output += path.toString()+"\n";
        }
        gen.get().addSnippet( "output", output );
        assertEquals(2, count);
    }
    
    

    private static ImpermanentGraphDatabase db;
    @BeforeClass
    public static void init()
    {
        db = new ImpermanentGraphDatabase("target/descendants");
    }
    
    @Before
    public void setUp() {
        db.cleanContent();
        gen.get().setGraph( db );
    }
    @After
    public void doc() {
        gen.get().document("target/docs","examples");
    }
    @Override
    public GraphDatabaseService graphdb()
    {
        return db;
    }

}
