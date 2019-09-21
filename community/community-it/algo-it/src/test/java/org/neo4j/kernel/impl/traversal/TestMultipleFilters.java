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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.function.Predicate;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.internal.helpers.collection.Iterables;

import static org.neo4j.graphdb.traversal.Evaluators.includeIfAcceptedByAny;

class TestMultipleFilters extends TraversalTestBase
{

    private Transaction tx;

    @BeforeEach
    void setupGraph()
    {
        //
        //                     (a)--------
        //                     /          \
        //                    v            v
        //                  (b)-->(k)<----(c)-->(f)
        //                  / \
        //                 v   v
        //                (d)  (e)
        createGraph( "a TO b", "b TO d", "b TO e", "b TO k", "a TO c", "c TO f", "c TO k" );
    }

    @AfterEach
    void tearDown()
    {
        if ( tx != null )
        {
            tx.close();
        }
    }

    private static class MustBeConnectedToNodeFilter implements Predicate<Path>, Evaluator
    {
        private final Node node;

        MustBeConnectedToNodeFilter( Node node )
        {
            this.node = node;
        }

        @Override
        public boolean test( Path item )
        {
            ResourceIterable<Relationship> relationships = (ResourceIterable<Relationship>) item.endNode()
                    .getRelationships( Direction.OUTGOING );
            try ( ResourceIterator<Relationship> iterator = relationships.iterator() )
            {
                while ( iterator.hasNext() )
                {
                    Relationship rel = iterator.next();
                    if ( rel.getEndNode().equals( node ) )
                    {
                        return true;
                    }
                }
                return false;
            }
        }

        @Override
        public Evaluation evaluate( Path path )
        {
            return test( path ) ? Evaluation.INCLUDE_AND_CONTINUE : Evaluation.EXCLUDE_AND_CONTINUE;
        }
    }

    @Test
    void testNarrowingFilters()
    {
        try ( Transaction transaction = beginTx() )
        {
            Evaluator mustBeConnectedToK = new MustBeConnectedToNodeFilter( getNodeWithName( transaction, "k" ) );
            Evaluator mustNotHaveMoreThanTwoOutRels =
                    path -> Evaluation.ofIncludes( Iterables
                            .count( path.endNode().getRelationships( Direction.OUTGOING ) ) <= 2 );

            TraversalDescription description = transaction.traversalDescription().evaluator( mustBeConnectedToK );
            expectNodes( description.traverse( transaction.getNodeById( node( "a" ).getId() ) ), "b", "c" );
            expectNodes( description.evaluator( mustNotHaveMoreThanTwoOutRels )
                    .traverse( transaction.getNodeById( node( "a" ).getId() ) ), "c" );
        }
    }

    @Test
    void testBroadeningFilters()
    {
        try ( Transaction transaction = beginTx() )
        {
            MustBeConnectedToNodeFilter mustBeConnectedToC = new MustBeConnectedToNodeFilter( getNodeWithName( transaction, "c" ) );
            MustBeConnectedToNodeFilter mustBeConnectedToE = new MustBeConnectedToNodeFilter( getNodeWithName( transaction, "e" ) );
            // Nodes connected (OUTGOING) to c (which "a" is)
            var aNode = transaction.getNodeById( node( "a" ).getId() );
            expectNodes( transaction.traversalDescription()
                    .evaluator( mustBeConnectedToC )
                    .traverse( aNode ), "a" );
            // Nodes connected (OUTGOING) to c AND e (which none is)
            expectNodes( transaction.traversalDescription().evaluator( mustBeConnectedToC ).evaluator( mustBeConnectedToE )
                    .traverse( aNode ) );
            // Nodes connected (OUTGOING) to c OR e (which "a" and "b" is)
            expectNodes( transaction.traversalDescription()
                    .evaluator( includeIfAcceptedByAny( mustBeConnectedToC, mustBeConnectedToE ) )
                    .traverse( aNode ), "a", "b" );
        }
    }
}
