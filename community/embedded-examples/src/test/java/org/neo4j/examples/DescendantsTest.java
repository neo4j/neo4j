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

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.kernel.Traversal;
import org.neo4j.kernel.Uniqueness;
import org.neo4j.test.GraphDescription;
import org.neo4j.test.GraphDescription.Graph;
import org.neo4j.test.GraphHolder;
import org.neo4j.test.ImpermanentGraphDatabase;
import org.neo4j.test.TestData;

public class DescendantsTest implements GraphHolder
{
    public @Rule
    TestData<Map<String, Node>> data = TestData.producedThrough( GraphDescription.createGraphFor(
            this, true ) );
 
    @Graph({"N0 descendant N1",
        "N0 descendant N2",
        "N0 descendant N3",
        "Principal1 owns N1",
        "Principal2 owns N2",
        "Principal1 owns N3",
                })
    @Test
    /**
     * 
     * This test is demonstrating the use of node uniqueness.
     * In order to return which all descendants (N1 and N3) 
     * from N0 which have the relation +owns to Principal1,
     * the Uniqueness of the traversal needs to be set to 
     * +NODE_PATH+ rather than the default +NODE_GLOBAL+.
     */
    public void testTraversal()
    {
        Node start = data.get().get( "N0" );
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
        
        org.neo4j.graphdb.traversal.Traverser results = td.traverse( start );
        int count = 0;
        for(Path path : results)
        {
            count ++;
            System.out.println(path.toString());
        }
        assertEquals(2, count);
    }
    private static GraphDatabaseService db;
    @BeforeClass
    public static void init()
    {
        db = new ImpermanentGraphDatabase("target/descendants");
    }
    @Override
    public GraphDatabaseService graphdb()
    {
        return db;
    }

}
