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
package org.neo4j.graphalgo.impl.util;

import common.Neo4jAlgoTestCase;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.neo4j.graphalgo.CommonEvaluators;
import org.neo4j.graphalgo.CostEvaluator;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Transaction;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.helpers.MathUtil.DEFAULT_EPSILON;

class TestTopFetchingWeightedPathIterator extends Neo4jAlgoTestCase
{
    private static final String length = "length";
    private static final CostEvaluator<Double> evaluator = CommonEvaluators.doubleCostEvaluator( length );
    private TopFetchingWeightedPathIterator topFetcher;

    @Test
    void shouldHandleEmptySource()
    {
        topFetcher = new TopFetchingWeightedPathIterator( Collections.emptyIterator(), evaluator , DEFAULT_EPSILON );

        assertFalse( topFetcher.hasNext(), "Expected iterator to be empty" );
        assertNull( topFetcher.fetchNextOrNull(), "Expected null after report has no next" );
    }

    @Test
    void shouldHandleSinglePath()
    {
        try ( Transaction transaction = graphDb.beginTx() )
        {
            Path a = graph.makePathWithRelProperty( transaction, length, "a1-1-a2" );
            List<Path> list = new ArrayList<>();
            list.add( a );

            topFetcher = new TopFetchingWeightedPathIterator( list.iterator(), evaluator, DEFAULT_EPSILON );

            assertTrue( topFetcher.hasNext(), "Expected at least one element" );
            assertPathDef( a, topFetcher.next() );
            assertFalse( topFetcher.hasNext(), "Expected no more elements" );
            assertNull( topFetcher.fetchNextOrNull(), "Expected null after report has no next" );
            transaction.commit();
        }
    }

    @Test
    void shouldHandleMultipleShortest()
    {
        try ( Transaction transaction = graphDb.beginTx() )
        {
            Path a = graph.makePathWithRelProperty( transaction, length, "a1-1-a2" );
            Path b = graph.makePathWithRelProperty( transaction, length, "b1-0-b2-1-b3-0-b4" );
            List<Path> list = new ArrayList<>();
            list.add( a );
            list.add( b );

            topFetcher = new TopFetchingWeightedPathIterator( list.iterator(), evaluator, DEFAULT_EPSILON );
            List<Path> result = new ArrayList<>();
            while ( topFetcher.hasNext() )
            {
                result.add( topFetcher.next() );
            }

            assertPathsWithPaths( result, a, b );
            transaction.commit();
        }
    }

    @Test
    void shouldHandleUnsortedSource()
    {
        try ( Transaction transaction = graphDb.beginTx() )
        {
            Path a = graph.makePathWithRelProperty( transaction, length, "a1-1-a2-2-a3" );             // 3
            Path b = graph.makePathWithRelProperty( transaction, length, "b1-3-b2-3-b3" );             // 6
            Path c = graph.makePathWithRelProperty( transaction, length, "c1-0-c2-1-c3" );             // 1
            Path d = graph.makePathWithRelProperty( transaction, length, "d1-3-d2-0-d3" );             // 3
            Path e = graph.makePathWithRelProperty( transaction, length, "e1-0-e2-0-e3-0-e4-1-e5" );   // 1

            List<Path> list = Arrays.asList( a, b, c, d, e );
            topFetcher = new TopFetchingWeightedPathIterator( list.iterator(), evaluator, DEFAULT_EPSILON );

            List<Path> result = new ArrayList<>();
            while ( topFetcher.hasNext() )
            {
                result.add( topFetcher.next() );
            }

            assertPathsWithPaths( result, c, e );
            transaction.commit();
        }
    }
}
