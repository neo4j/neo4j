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
// START SNIPPET: sampleDocumentation
// START SNIPPET: _sampleDocumentation
package org.neo4j.examples;

import static org.neo4j.visualization.asciidoc.AsciidocHelper.createGraphViz;
import static org.neo4j.visualization.asciidoc.AsciidocHelper.createOutputSnippet;

import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.kernel.Traversal;
import org.neo4j.kernel.Uniqueness;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.test.GraphDescription.Graph;

public class TraversalTest extends AbstractJavaDocTestbase
{
    /**
     * In contrary to
     * http://components.neo4j.org/neo4j/{neo4j-version}/apidocs/org/neo4j/graphdb/Node.html#traverse[Node#traverse] a
     * http://components.neo4j.org/neo4j/{neo4j-version}/apidocs/org/neo4j/graphdb/traversal/TraversalDescription.html[traversal description] is built (using a
     * fluent interface) and such a description can spawn
     * http://components.neo4j.org/neo4j/{neo4j-version}/apidocs/org/neo4j/graphdb/traversal/Traverser.html[traversers].
     * 
     * @@graph
     * 
     * With the definition of the +RelationshipTypes+ as
     * 
     * @@sourceRels
     * 
     * The graph can be traversed with
     * 
     * @@source1
     * 
     * Since http://components.neo4j.org/neo4j/{neo4j-version}/apidocs/org/neo4j/graphdb/traversal/TraversalDescription.html[+TraversalDescription+]s 
     * are immutable it is also useful to create template descriptions which holds common 
     * settings shared by different traversals, for example:
     * 
     * @@source2
     * 
     * If you're not interested in the http://components.neo4j.org/neo4j/{neo4j-version}/apidocs/org/neo4j/graphdb/Path.html[+Path+]s, 
     * but f.ex. the http://components.neo4j.org/neo4j/{neo4j-version}/apidocs/org/neo4j/graphdb/Node.html[+Node+]s 
     * you can transform the traverser into an iterable of http://components.neo4j.org/neo4j/{neo4j-version}/apidocs/org/neo4j/graphdb/traversal/Traverser.html#nodes()[nodes] 
     * or http://components.neo4j.org/neo4j/{neo4j-version}/apidocs/org/neo4j/graphdb/traversal/Traverser.html#relationships()[relationships]:
     * 
     * @@source3
     * 
     * The full source for this example is available at
     * 
     * @@github
     */
    @Test
    @Documented
    @Graph( {"Joe KNOWS Sara", "Lisa LIKES Joe" })
    public void how_to_use_the_Traversal_framework()
    {
        Node joe = data.get().get( "Joe" );
        gen.get().addSnippet(
                "graph",
                createGraphViz( "Hello World Graph", graphdb(),
                        gen.get().getTitle() ) );
        // START SNIPPET: source1

        for ( Path position : Traversal.description()
                .depthFirst()
                .relationships( Rels.KNOWS )
                .relationships( Rels.LIKES, Direction.INCOMING )
                .prune( Traversal.pruneAfterDepth( 5 ) )
                .traverse( joe ) )
        {
          System.out.println( "Path from start node to current position is " + position );
        }
        // END SNIPPET: source1

        // START SNIPPET: source2
        final TraversalDescription FRIENDS_TRAVERSAL = Traversal.description()
                .relationships( Rels.KNOWS )
                .depthFirst()
                .uniqueness( Uniqueness.RELATIONSHIP_GLOBAL );
        
        // Don't go further than depth 3
        for ( Path position : FRIENDS_TRAVERSAL
                  .prune( Traversal.pruneAfterDepth( 3 ) )
                  .traverse( joe ) ) {}
        // Don't go further than depth 4
        for ( Path position : FRIENDS_TRAVERSAL
                  .prune( Traversal.pruneAfterDepth( 4 ) )
                  .traverse( joe ) ) {}
        // END SNIPPET: source2
        // START SNIPPET: source3
        for ( Node position : FRIENDS_TRAVERSAL.traverse( joe ).nodes() ) {}
        // END SNIPPET: source3

        gen.get().addSnippet( "output",
                createOutputSnippet( "Hello graphy world!" ) );
        gen.get().addSourceSnippets( this.getClass(), "source1","sourceRels","source2","source3" );
        gen.get().addGithubLink( "github", this.getClass(), "neo4j/community",
                "embedded-examples" );
    }
    
    // START SNIPPET: sourceRels
    private static enum Rels implements RelationshipType
    {
        LIKES, KNOWS
    }
    // END SNIPPET: sourceRels
}
