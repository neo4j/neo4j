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
// START SNIPPET: sampleDocumentation
// START SNIPPET: _sampleDocumentation
package org.neo4j.examples;

import java.io.IOException;

import org.junit.Test;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.test.GraphDescription.Graph;

import static org.neo4j.visualization.asciidoc.AsciidocHelper.createGraphVizWithNodeId;
import static org.neo4j.visualization.asciidoc.AsciidocHelper.createOutputSnippet;

public class TraversalDocTest extends ImpermanentGraphJavaDocTestBase
{
    /**
     * A
     * link:javadocs/org/neo4j/graphdb/traversal/TraversalDescription.html[traversal description] is built using a
     * fluent interface and such a description can then spawn
     * link:javadocs/org/neo4j/graphdb/traversal/Traverser.html[traversers].
     *
     * @@graph
     *
     * With the definition of the +RelationshipTypes+ as
     *
     * @@sourceRels
     *
     * The graph can be traversed with for example the following traverser, starting at the ``Joe'' node:
     *
     * @@knowslikestraverser
     *
     * The traversal will output:
     *
     * @@knowslikesoutput
     *
     * Since link:javadocs/org/neo4j/graphdb/traversal/TraversalDescription.html[+TraversalDescription+]s
     * are immutable it is also useful to create template descriptions which holds common
     * settings shared by different traversals. For example, let's start with this traverser:
     *
     * @@basetraverser
     *
     * This traverser would yield the following output (we will keep starting from the ``Joe'' node):
     *
     * @@baseoutput
     *
     * Now let's create a new traverser from it, restricting depth to three:
     *
     * @@depth3
     *
     * This will give us the following result:
     *
     * @@output3
     *
     * Or how about from depth two to four?
     * That's done like this:
     *
     * @@depth4
     *
     * This traversal gives us:
     *
     * @@output4
     *
     * For various useful evaluators, see the
     * link:javadocs/org/neo4j/graphdb/traversal/Evaluators.html[Evaluators] Java API
     * or simply implement the
     * link:javadocs/org/neo4j/graphdb/traversal/Evaluator.html[Evaluator] interface yourself.
     *
     * If you're not interested in the link:javadocs/org/neo4j/graphdb/Path.html[+Path+]s,
     * but the link:javadocs/org/neo4j/graphdb/Node.html[+Node+]s
     * you can transform the traverser into an iterable of link:javadocs/org/neo4j/graphdb/traversal/Traverser.html#nodes()[nodes]
     * like this:
     *
     * @@nodes
     *
     * In this case we use it to retrieve the names:
     *
     * @@nodeoutput
     *
     * link:javadocs/org/neo4j/graphdb/traversal/Traverser.html#relationships()[Relationships]
     * are fine as well, here's how to get them:
     *
     * @@relationships
     *
     * Here the relationship types are written, and we get:
     *
     * @@relationshipoutput
     *
     * TIP: The source code for the traversers in this example is available at:
     * @@github
     */
    @Test
    @Documented
    @Graph( { "Joe KNOWS Sara", "Lisa LIKES Joe", "Peter KNOWS Sara",
            "Dirk KNOWS Peter", "Lars KNOWS Dirk", "Ed KNOWS Lars",
            "Lisa KNOWS Lars" } )
    public void how_to_use_the_Traversal_framework()
    {
        Node joe = data.get().get( "Joe" );
        TraversalExample example = new TraversalExample( db );
        gen.get().addSnippet(
                "graph",
                        createGraphVizWithNodeId( "Traversal Example Graph", graphdb(),
                        gen.get().getTitle() ) );

        try ( Transaction tx = db.beginTx() )
        {
            String output = example.knowsLikesTraverser( joe );
            gen.get().addSnippet( "knowslikesoutput", createOutputSnippet( output ) );

            output = example.traverseBaseTraverser( joe );
            gen.get().addSnippet( "baseoutput", createOutputSnippet( output ) );

            output = example.depth3( joe );
            gen.get().addSnippet( "output3", createOutputSnippet( output ) );

            output = example.depth4( joe );
            gen.get().addSnippet( "output4", createOutputSnippet( output ) );

            output = example.nodes( joe );
            gen.get().addSnippet( "nodeoutput", createOutputSnippet( output ) );

            output = example.relationships( joe );
            gen.get().addSnippet( "relationshipoutput", createOutputSnippet( output ) );

            gen.get().addSourceSnippets( example.getClass(), "knowslikestraverser",
                    "sourceRels", "basetraverser", "depth3", "depth4",
                    "nodes", "relationships" );
            gen.get().addGithubSourceLink( "github", example.getClass(), "community/embedded-examples" );
        }
    }

    @Test
    public void runAll() throws IOException
    {
        TraversalExample.main( null );
    }
}
