/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.internal.helpers.collection.IterableWrapper;

class TestTraversalWithIterable extends TraversalTestBase
{
    @Test
    void traverseWithIterableForStartNodes()
    {
        /*
         * (a)-->(b)-->(c)
         * (d)-->(e)-->(f)
         *
         */

        createGraph( "a TO b", "b TO c", "d TO e", "e TO f" );

        try ( Transaction tx = beginTx() )
        {
            TraversalDescription basicTraverser = tx.traversalDescription().evaluator( Evaluators.atDepth(2) );

            Collection<Node> startNodes = new ArrayList<>(  );
            startNodes.add( getNodeWithName( tx, "a" ) );
            startNodes.add( getNodeWithName( tx, "d" ) );

            Iterable<Node> iterableStartNodes = startNodes;

            expectPaths( basicTraverser.traverse( iterableStartNodes ), "a,b,c", "d,e,f");
            tx.commit();
        }
    }

    @Test
    void useTraverserInsideTraverser()
    {
        /*
         * (a)-->(b)-->(c)
         *  |
         * \/
         * (d)-->(e)-->(f)
         *
         */

        createGraph( "a FIRST d", "a TO b", "b TO c", "d TO e", "e TO f" );

        try ( Transaction tx = beginTx() )
        {
            TraversalDescription firstTraverser = tx.traversalDescription()
                    .relationships( RelationshipType.withName( "FIRST" ) )
                    .evaluator( Evaluators.toDepth( 1 ) );
            final Iterable<Path> firstResult = firstTraverser.traverse( getNodeWithName( tx, "a" ) );

            Iterable<Node> startNodesForNestedTraversal = new IterableWrapper<Node,Path>( firstResult )
            {
                @Override
                protected Node underlyingObjectToObject( Path path )
                {
                    return path.endNode();
                }
            };

            TraversalDescription nestedTraversal = tx.traversalDescription().evaluator( Evaluators.atDepth( 2 ) );
            expectPaths( nestedTraversal.traverse( startNodesForNestedTraversal ), "a,b,c", "d,e,f");
            tx.commit();
        }
    }

}
