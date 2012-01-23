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

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.kernel.Traversal;
import org.neo4j.kernel.Uniqueness;

public class TraversalExample
{
    public String knowsLikesTraverser( Node node )
    {
        String output = "";
        // START SNIPPET: knowslikestraverser
        for ( Path position : Traversal.description()
                .depthFirst()
                .relationships( Rels.KNOWS )
                .relationships( Rels.LIKES, Direction.INCOMING )
                .evaluator( Evaluators.toDepth( 5 ) )
                .traverse( node ) )
        {
            output += position + "\n";
        }
        // END SNIPPET: knowslikestraverser
        return output;
    }

    // START SNIPPET: basetraverser
    final TraversalDescription FRIENDS_TRAVERSAL = Traversal.description()
            .depthFirst()
            .relationships( Rels.KNOWS )
            .uniqueness( Uniqueness.RELATIONSHIP_GLOBAL );
    // END SNIPPET: basetraverser

    public String traverseBaseTraverser( Node node )
    {
        String output = "";
        // START SNIPPET: traversebasetraverser
        for ( Path path : FRIENDS_TRAVERSAL.traverse( node ) )
        {
            output += path + "\n";
        }
        // END SNIPPET: traversebasetraverser
        return output;
    }

    public String depth3( Node node )
    {
        String output = "";
        // START SNIPPET: depth3
        for ( Path path : FRIENDS_TRAVERSAL
                .evaluator( Evaluators.toDepth( 3 ) )
                .traverse( node ) )
        {
            output += path + "\n";
        }
        // END SNIPPET: depth3
        return output;
    }

    public String depth4( Node node )
    {
        String output = "";
        // START SNIPPET: depth4
        for ( Path path : FRIENDS_TRAVERSAL
                .evaluator( Evaluators.fromDepth( 2 ) )
                .evaluator( Evaluators.toDepth( 4 ) )
                .traverse( node ) )
        {
            output += path + "\n";
        }
        // END SNIPPET: depth4
        return output;
    }

    public String nodes( Node node )
    {
        String output = "";
        // START SNIPPET: nodes
        for ( Node currentNode : FRIENDS_TRAVERSAL
                .traverse( node )
                .nodes() )
        {
            output += currentNode.getProperty( "name" ) + "\n";
        }
        // END SNIPPET: nodes
        return output;
    }

    public String relationships( Node node )
    {
        String output = "";
        // START SNIPPET: relationships
        for ( Relationship relationship : FRIENDS_TRAVERSAL
                .traverse( node )
                .relationships() )
        {
            output += relationship.getType() + "\n";
        }
        // END SNIPPET: relationships
        return output;
    }

    // START SNIPPET: sourceRels
    private enum Rels implements RelationshipType
    {
        LIKES, KNOWS
    }
    // END SNIPPET: sourceRels
}
