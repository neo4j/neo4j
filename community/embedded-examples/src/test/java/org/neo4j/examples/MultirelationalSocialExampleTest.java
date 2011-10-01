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

import static org.junit.Assert.assertTrue;
import static org.neo4j.visualization.asciidoc.AsciidocHelper.createCypherSnippet;
import static org.neo4j.visualization.asciidoc.AsciidocHelper.createGraphViz;
import static org.neo4j.visualization.asciidoc.AsciidocHelper.createQueryResultSnippet;

import org.junit.Test;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.test.GraphDescription.Graph;

public class MultirelationalSocialExampleTest extends AbstractJavaDocTestbase
{
    /**
     * Multi-relational social network.
     * 
     * This example shows a multi-relational
     * network between persons and things they like.
     * A multi-relational graph is a graph with more than
     * one kind of relationship between nodes.
     * 
     * @@graph1
     * 
     * Below are some typical questions asked of such a network:
     * 
     * == Who +FOLLOWS+ or +LIKES+ me back?
     * 
     * @@query1
     * 
     * @@result1
     */
//     * == What do my friends like that I do not (yet) like?
//     * 
//     * This is a basic recommendation scenario, ranking the preferred items of my social neighborhood
//     * for things that I am not preferring yet.
//     * 
//     * @@query2
//     * 
//     * @@result2
//     * 
//     * 
//     */
    @Test
    @Documented
    @Graph(value = {"Joe FOLLOWS Sara", 
            "Sara FOLLOWS Joe", 
            "Joe LOVES Maria",
            "Maria LOVES Joe",
            "Joe FOLLOWS Maria",
            "Maria FOLLOWS Joe",
            "Sara FOLLOWS Ben", 
            "Joe LIKES bikes",
            "Joe LIKES nature",
            "Sara LIKES bikes",
            "Sara LIKES cars",
            "Sara LIKES cats",
            "Maria LIKES cars"
            }, autoIndexNodes = true)
    public void social_network_with_multiple_relations()
    {
        data.get();
        gen.get().addSnippet( "graph1", createGraphViz("Multi-relational social network", graphdb(), gen.get().getTitle()) );
        String query = "START me=(node_auto_index,'name:Joe') " +
        		"MATCH me-[r1]->other-[r2]->me WHERE type(r1)=type(r2) AND type(r1) =~ /FOLLOWS|LOVES/ RETURN other, type(r1) ";
        String result = engine.execute( parser.parse( query ) ).toString();
        gen.get().addSnippet( "query1", createCypherSnippet( query ) );
        gen.get().addSnippet( "result1", createQueryResultSnippet( engine.execute( parser.parse( query ) ).toString() ) );
        assertTrue(result.contains( "Sara" ));
        assertTrue(result.contains( "Maria" ));
        
        query = "START joe=(node_auto_index,'name:Joe') " +
                "MATCH joe-[:FOLLOWS]->other-[:LIKES]->theirStuff, joe-[:LIKES]->joeStuff " +
                "WHERE NOT(ID(theirStuff) = ID(joeStuff)) RETURN theirStuff, COUNT(theirStuff)" +
                "ORDER BY COUNT(theirStuff) DESC ";
        result = engine.execute( parser.parse( query ) ).toString();
//        assertTrue(result.contains( "{name->\"cars\"}  | 2" ));
        gen.get().addSnippet( "query2", createCypherSnippet( query ) );
        gen.get().addSnippet( "result2", createQueryResultSnippet( engine.execute( parser.parse( query ) ).toString() ) );
    }    
}
