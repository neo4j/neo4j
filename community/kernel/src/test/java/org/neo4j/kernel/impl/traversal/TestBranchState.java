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

import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.InitialBranchState;
import org.neo4j.graphdb.traversal.PathEvaluator;

import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.graphdb.traversal.Evaluation.ofIncludes;
import static org.neo4j.helpers.collection.IteratorUtil.count;
import static org.neo4j.kernel.Traversal.initialState;
import static org.neo4j.kernel.Traversal.traversal;
import static org.neo4j.kernel.Uniqueness.NODE_PATH;

public class TestBranchState extends TraversalTestBase
{
    @Test
    public void depthAsState() throws Exception
    {
        /*
         * (a) -> (b) -> (c) -> (d)
         *          \           ^
         *           v         /
         *           (e) -> (f) -> (g) -> (h)
         */
        createGraph( "a to b", "b to c", "c to d", "b to e", "e to f", "f to d", "f to g", "g to h" );

        try (Transaction tx = beginTx())
        {
            DepthStateExpander expander = new DepthStateExpander();
            count( traversal().expand( expander, initialState( 0 ) ).traverse( getNodeWithName( "a" ) ) );
            tx.success();
        }
    }

    @Test
    public void everyOtherDepthAsState() throws Exception
    {
        /*
         * (a) -> (b) -> (c) -> (e)
         */
        createGraph( "a to b", "b to c", "c to d", "d to e" );
        try ( Transaction tx = beginTx() )
        {

        /*
         * Asserts that state continues down branches even when expander doesn't
         * set new state for every step.
         */
            IncrementEveryOtherDepthCountingExpander expander = new IncrementEveryOtherDepthCountingExpander();
            count( traversal().expand( expander, initialState( 0 ) ).traverse( getNodeWithName( "a" ) ) );
            tx.success();
        }
    }

    @Test
    public void evaluateState() throws Exception
    {
        /*
         * (a)-1->(b)-2->(c)-3->(d)
         *   \           ^
         *    4         6
         *    (e)-5->(f)
         */
        createGraph( "a TO b", "b TO c", "c TO d", "a TO e", "e TO f", "f TO c" );

        try ( Transaction tx = beginTx() )
        {
            PathEvaluator<Integer> evaluator = new PathEvaluator.Adapter<Integer>()
            {
                @Override
                public Evaluation evaluate( Path path, BranchState<Integer> state )
                {
                    return ofIncludes( path.endNode().getProperty( "name" ).equals( "c" ) && state.getState() == 3 );
                }
            };

            expectPaths( traversal( NODE_PATH ).expand( new RelationshipWeightExpander(), new InitialBranchState.State<Integer>( 1, 1 ) )
                    .evaluator( evaluator ).traverse( getNodeWithName( "a" ) ), "a,b,c" );
            tx.success();
        }
    }
    
    private static class DepthStateExpander implements PathExpander<Integer>
    {
        @Override
        public Iterable<Relationship> expand( Path path, BranchState<Integer> state )
        {
            assertEquals( path.length(), state.getState().intValue() );
            state.setState( state.getState()+1 );
            return path.endNode().getRelationships( Direction.OUTGOING );
        }

        @Override
        public PathExpander<Integer> reverse()
        {
            return this;
        }
    }

    private static class IncrementEveryOtherDepthCountingExpander implements PathExpander<Integer>
    {
        @Override
        public Iterable<Relationship> expand( Path path, BranchState<Integer> state )
        {
            assertEquals( path.length()/2, state.getState().intValue() );
            if ( path.length() % 2 == 1 )
                state.setState( state.getState() + 1 );
            return path.endNode().getRelationships( Direction.OUTGOING );
        }

        @Override
        public PathExpander<Integer> reverse()
        {
            return this;
        }
    }
    
    private static class RelationshipWeightExpander implements PathExpander<Integer>
    {
        @Override
        public Iterable<Relationship> expand( Path path, BranchState<Integer> state )
        {
            state.setState( state.getState() + 1 );
            return path.endNode().getRelationships( OUTGOING );
        }

        @Override
        public PathExpander<Integer> reverse()
        {
            return this;
        }
    }
}