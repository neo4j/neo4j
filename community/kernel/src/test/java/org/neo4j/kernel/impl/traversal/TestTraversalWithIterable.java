/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.traversal;

import java.util.ArrayList;
import java.util.Collection;

import org.junit.Test;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.helpers.collection.IterableWrapper;

public class TestTraversalWithIterable extends TraversalTestBase
{
    @Test
    public void traverseWithIterableForStartNodes() throws Exception
    {
        /*
         * (a)-->(b)-->(c)
         * (d)-->(e)-->(f)
         *
         */
        
        createGraph( "a TO b", "b TO c", "d TO e", "e TO f" );

        try (Transaction tx = beginTx())
        {
            TraversalDescription basicTraverser = getGraphDb().traversalDescription().evaluator( Evaluators.atDepth(2) );

            Collection<Node> startNodes = new ArrayList<>(  );
            startNodes.add( getNodeWithName( "a" ) );
            startNodes.add( getNodeWithName( "d" ) );

            Iterable<Node> iterableStartNodes = startNodes;


            expectPaths( basicTraverser.traverse( iterableStartNodes ), "a,b,c", "d,e,f");
            tx.success();
        }
    }

    @Test
    public void useTraverserInsideTraverser() throws Exception
    {
        /*
         * (a)-->(b)-->(c)
         *  |
         * \/
         * (d)-->(e)-->(f)
         *
         */

        createGraph( "a FIRST d", "a TO b", "b TO c", "d TO e", "e TO f" );

        try (Transaction tx = beginTx())
        {
            TraversalDescription firstTraverser = getGraphDb().traversalDescription()
                    .relationships( DynamicRelationshipType.withName( "FIRST" ) )
                    .evaluator( Evaluators.toDepth( 1 ) );
            final Iterable<Path> firstResult = firstTraverser.traverse( getNodeWithName( "a" ) );

            Iterable<Node> startNodesForNestedTraversal = new IterableWrapper<Node, Path>(firstResult) {
                @Override
                protected Node underlyingObjectToObject( Path path )
                {
                    return path.endNode();
                }
            };

            TraversalDescription nestedTraversal = getGraphDb().traversalDescription().evaluator( Evaluators.atDepth( 2 ) );
            expectPaths( nestedTraversal.traverse( startNodesForNestedTraversal ), "a,b,c", "d,e,f");
            tx.success();
        }
    }

}
