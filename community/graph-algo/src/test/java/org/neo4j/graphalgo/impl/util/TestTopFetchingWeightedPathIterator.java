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
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.neo4j.graphalgo.CommonEvaluators;
import org.neo4j.graphalgo.CostEvaluator;
import org.neo4j.graphdb.Path;
import org.neo4j.kernel.impl.util.NoneStrictMath;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Anton Persson
 */
public class TestTopFetchingWeightedPathIterator extends Neo4jAlgoTestCase
{
    @Test
    public void shouldHandleEmptySource()
    {
        topFetcher = new TopFetchingWeightedPathIterator(
                Collections.<Path>emptyIterator(), evaluator );

        assertFalse( "Expected iterator to be empty", topFetcher.hasNext() );
        assertNull( "Expected null after report has no next", topFetcher.fetchNextOrNull() );
    }

    @Test
    public void shouldHandleSinglePath()
    {
        Path a = graph.makePathWithRelProperty( length, "a1-1-a2" );
        List<Path> list = new ArrayList<>(  );
        list.add( a );

        topFetcher = new TopFetchingWeightedPathIterator( list.iterator(), evaluator, epsilon );

        assertTrue( "Expected at least one element", topFetcher.hasNext() );
        assertPathDef( a, topFetcher.next() );
        assertFalse( "Expected no more elements", topFetcher.hasNext() );
        assertNull( "Expected null after report has no next", topFetcher.fetchNextOrNull() );
    }

    @Test
    public void shouldHandleMultipleShortest()
    {
        Path a = graph.makePathWithRelProperty(  length, "a1-1-a2" );
        Path b = graph.makePathWithRelProperty( length, "b1-0-b2-1-b3-0-b4" );
        List<Path> list = new ArrayList<>(  );
        list.add( a );
        list.add( b );

        topFetcher = new TopFetchingWeightedPathIterator( list.iterator(), evaluator, epsilon );
        List<Path> result = new ArrayList<>();
        while ( topFetcher.hasNext() )
        {
            result.add( topFetcher.next() );
        }

        assertPathsWithPaths( result, a, b );
    }

    @Test
    public void shouldHandleUnsortedSource()
    {
        Path a = graph.makePathWithRelProperty( length, "a1-1-a2-2-a3" );             // 3
        Path b = graph.makePathWithRelProperty( length, "b1-3-b2-3-b3" );             // 6
        Path c = graph.makePathWithRelProperty( length, "c1-0-c2-1-c3" );             // 1
        Path d = graph.makePathWithRelProperty( length, "d1-3-d2-0-d3" );             // 3
        Path e = graph.makePathWithRelProperty( length, "e1-0-e2-0-e3-0-e4-1-e5" );   // 1


        List<Path> list = Arrays.asList(
                new Path[]{
                        a,b,c,d,e
                }
        );
        topFetcher = new TopFetchingWeightedPathIterator( list.iterator(), evaluator, epsilon );

        List<Path> result = new ArrayList<>(  );
        while ( topFetcher.hasNext() )
        {
            result.add( topFetcher.next() );
        }

        assertPathsWithPaths( result, c, e );
    }

    private final double epsilon = NoneStrictMath.EPSILON;
    private final String length = "length";
    private CostEvaluator evaluator = CommonEvaluators.doubleCostEvaluator( length );
    private TopFetchingWeightedPathIterator topFetcher;
}
