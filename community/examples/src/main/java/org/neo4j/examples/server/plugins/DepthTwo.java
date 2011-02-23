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
package org.neo4j.examples.server.plugins;

import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.traversal.PruneEvaluator;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.helpers.Predicate;
import org.neo4j.kernel.Traversal;
import org.neo4j.kernel.Uniqueness;
import org.neo4j.server.plugins.Description;
import org.neo4j.server.plugins.PluginTarget;
import org.neo4j.server.plugins.ServerPlugin;
import org.neo4j.server.plugins.Source;



/**
* An extension performaing a predefined graph traversal
*/
@Description( "Performs a depth two traversal along all relationship types." )
public class
        DepthTwo extends ServerPlugin
{
    @Description( "Traverse depth two and return the end nodes" )
    @PluginTarget( Node.class )
    public Iterable<Node> nodesOnDepthTwo( @Source Node node )
    {
        return traversal.traverse( node ).nodes();
    }

    @Description( "Traverse depth two and return the last relationships" )
    @PluginTarget( Node.class )
    public Iterable<Relationship> relationshipsOnDepthTwo( @Source Node node )
    {
        return traversal.traverse( node ).relationships();
    }

    @Description( "Traverse depth two and return the paths" )
    @PluginTarget( Node.class )
    public Iterable<Path> pathsOnDepthTwo( @Source Node node )
    {
        return traversal.traverse( node );
    }

    private static final TraversalDescription traversal = Traversal.description().uniqueness(
            Uniqueness.RELATIONSHIP_PATH ).prune( new PruneEvaluator()
    {
        public boolean pruneAfter( Path position )
        {
            return position.length() >= 2;
        }
    } ).filter( new Predicate<Path>()
    {
        public boolean accept( Path item )
        {
            return item.length() == 2;
        }
    } );
}
