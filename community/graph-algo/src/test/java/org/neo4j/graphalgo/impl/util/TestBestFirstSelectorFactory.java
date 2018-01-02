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
package org.neo4j.graphalgo.impl.util;

import common.Neo4jAlgoTestCase;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import org.neo4j.graphalgo.CommonEvaluators;
import org.neo4j.graphalgo.CostEvaluator;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.PathExpanders;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.traversal.TraversalBranch;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.graphdb.traversal.Uniqueness;
import org.neo4j.kernel.impl.traversal.MonoDirectionalTraversalDescription;

import static org.junit.Assert.assertEquals;

/**
 * @author Anton Persson
 */
@RunWith( Parameterized.class )
public class TestBestFirstSelectorFactory extends Neo4jAlgoTestCase
{
    /*
     * LAYOUT
     *
     *  (a) - 1 -> (b) - 2 -> (d)
     *   |          ^
     *   2 -> (c) - 4
     *
     */
    @Before
    public void buildGraph()
    {
        graph.makePathWithRelProperty( length, "a-1-b-2-d" );
        graph.makePathWithRelProperty( length, "a-2-c-4-b" );
    }

    @Test
    public void shouldDoWholeTraversalInCorrectOrder()
    {
        Node a = graph.getNode( "a" );

        Traverser traverser = new MonoDirectionalTraversalDescription(  )
                .expand( expander )
                .order( factory )
                .uniqueness( uniqueness )
                .traverse( a );

        ResourceIterator<Path> iterator = traverser.iterator();

        int i = 0;
        while ( iterator.hasNext() )
        {
            assertPath( iterator.next(), expectedResult[i] );
            i++;
        }
        assertEquals( String.format( "Not all expected paths where traversed. Missing paths are %s\n",
                Arrays.toString( Arrays.copyOfRange(expectedResult, i, expectedResult.length ) ) ),
                expectedResult.length, i );
    }


    @Parameterized.Parameters
    public static Collection<Object[]> data()
    {
        return Arrays.asList( new Object[][] {
                // Different PathInterests
                {
                        PathExpanders.allTypesAndDirections(),
                        PathInterestFactory.all(),
                        Uniqueness.NODE_PATH,
                        new String[]{ "a", "a,b", "a,c", "a,b,d", "a,b,c", "a,c,b", "a,c,b,d" }
                },
                {
                        PathExpanders.allTypesAndDirections(),
                        PathInterestFactory.allShortest(),
                        Uniqueness.NODE_PATH,
                        new String[]{ "a", "a,b", "a,c", "a,b,d" }
                },
                // Different PathExpanders
                {
                        PathExpanders.forDirection( Direction.OUTGOING ),
                        PathInterestFactory.all( ),
                        Uniqueness.NODE_PATH,
                        new String[]{ "a", "a,b", "a,c", "a,b,d", "a,c,b", "a,c,b,d" }
                },
                // Different uniqueness
                {
                        PathExpanders.allTypesAndDirections(),
                        PathInterestFactory.all(),
                        Uniqueness.NODE_GLOBAL,
                        new String[]{ "a", "a,b", "a,c", "a,b,d" }
                },
                {
                        PathExpanders.allTypesAndDirections(),
                        PathInterestFactory.all(),
                        Uniqueness.RELATIONSHIP_GLOBAL,
                        new String[]{ "a", "a,b", "a,c", "a,b,d", "a,b,c" }
                }
        } );
    }

    private final String length = "length";
    private final PathExpander expander;
    private final Uniqueness uniqueness;
    private final String[] expectedResult;
    private final BestFirstSelectorFactory<Integer, Integer> factory;

    public TestBestFirstSelectorFactory( PathExpander expander, PathInterest<Integer> interest, Uniqueness uniqueness,
            String[] expectedResult )
    {

        this.expander = expander;
        this.uniqueness = uniqueness;
        this.expectedResult = expectedResult;
        factory = new BestFirstSelectorFactory<Integer, Integer>( interest )
        {
            private final CostEvaluator<Integer> evaluator = CommonEvaluators.intCostEvaluator( length );

            @Override
            protected Integer getStartData()
            {
                return 0;
            }

            @Override
            protected Integer addPriority( TraversalBranch source, Integer currentAggregatedValue, Integer value )
            {
                return value + currentAggregatedValue;
            }

            @Override
            protected Integer calculateValue( TraversalBranch next )
            {
                return next.length() == 0 ? 0 : evaluator.getCost( next.lastRelationship(), Direction.BOTH );
            }
        };
    }
}
