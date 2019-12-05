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
package org.neo4j.graphalgo.impl.path;

import common.Neo4jAlgoTestCase;
import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import org.neo4j.graphalgo.BasicEvaluationContext;
import org.neo4j.graphalgo.EvaluationContext;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.PathExpanderBuilder;
import org.neo4j.graphdb.PathExpanders;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.impl.StandardExpander;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.internal.helpers.collection.Iterables;

import static common.Neo4jAlgoTestCase.MyRelTypes.R1;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphalgo.GraphAlgoFactory.shortestPath;
import static org.neo4j.graphdb.Direction.BOTH;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.graphdb.PathExpanders.allTypesAndDirections;
import static org.neo4j.graphdb.PathExpanders.forTypeAndDirection;
import static org.neo4j.internal.helpers.collection.Iterables.count;

class TestShortestPath extends Neo4jAlgoTestCase
{
    // Attempt at recreating this issue without cypher
    // https://github.com/neo4j/neo4j/issues/4160
    @Test
    void shouldAbortAsSoonAsPossible()
    {
        try ( Transaction transaction = graphDb.beginTx() )
        {
            final Label A = Label.label( "A" );
            final Label B = Label.label( "B" );
            final Label C = Label.label( "C" );
            final Label D = Label.label( "D" );
            final Label E = Label.label( "E" );
            final Label F = Label.label( "F" );
            final RelationshipType relType = RelationshipType.withName( "TO" );
            recursiveSnowFlake( transaction, null, 0, 4, 5, new Label[]{A, B, C, D, E}, relType );
            Node a = getNodeByLabel( transaction, A );
            try ( ResourceIterator<Node> allE = transaction.findNodes( E ) )
            {
                while ( allE.hasNext() )
                {
                    final Node e = allE.next();
                    final Node f = transaction.createNode( F );
                    f.createRelationshipTo( e, relType );
                }
            }
            final CountingPathExpander countingPathExpander = new CountingPathExpander( forTypeAndDirection( relType, OUTGOING ) );
            var context = new BasicEvaluationContext( transaction, graphDb );
            final ShortestPath shortestPath = new ShortestPath( context, Integer.MAX_VALUE, countingPathExpander, Integer.MAX_VALUE );
            try ( ResourceIterator<Node> allF = transaction.findNodes( F ) )
            {
                while ( allF.hasNext() )
                {
                    final Node f = allF.next();
                    shortestPath.findAllPaths( a, f );
                }
            }
            assertEquals( 1250, countingPathExpander.nodesVisited.intValue(),
                    "There are 625 different end nodes. The algorithm should start one traversal for each such node. " +
                            "That is 625*2 visited nodes if traversal is interrupted correctly." );
            transaction.commit();
        }
    }

    private static Node getNodeByLabel( Transaction transaction, Label label )
    {
        try ( ResourceIterator<Node> iterator = transaction.findNodes( label ) )
        {
            return iterator.next();
        }
    }

    private static void recursiveSnowFlake( Transaction transaction, Node parent, int level, final int desiredLevel, final int branchingFactor,
        final Label[] labels, final RelationshipType relType )
    {
        if ( level != 0 )
        {
            for ( int n = 0; n < branchingFactor; n++ )
            {
                final Node node = transaction.createNode( labels[level] );
                if ( parent != null )
                {
                    parent.createRelationshipTo( node, relType );
                }
                if ( level < desiredLevel )
                {
                    recursiveSnowFlake( transaction, node, level + 1, desiredLevel, branchingFactor, labels, relType );
                }
            }
        }
        else
        {
            final Node node = transaction.createNode( labels[level] );
            recursiveSnowFlake( transaction, node, level + 1, desiredLevel, branchingFactor, labels, relType );
        }
    }

    @Test
    void testSimplestGraph()
    {
        // Layout:
        //    __
        //   /  \
        // (s)  (t)
        //   \__/
        try ( Transaction transaction = graphDb.beginTx() )
        {
            graph.makeEdge( transaction, "s", "t" );
            graph.makeEdge( transaction, "s", "t" );
            var context = new BasicEvaluationContext( transaction, graphDb );
            testShortestPathFinder( context, finder ->
            {
                final Iterable<Path> paths = finder.findAllPaths( graph.getNode( transaction, "s" ), graph.getNode( transaction, "t" ) );
                assertPaths( paths, "s,t", "s,t" );
                assertPaths( asList( finder.findSinglePath( graph.getNode( transaction, "s" ),
                        graph.getNode( transaction, "t" ) ) ), "s,t" );
            }, forTypeAndDirection( R1, BOTH ), 1 );
            transaction.commit();
        }
    }

    @Test
    void testAnotherSimpleGraph()
    {
        // Layout:
        //   (m)
        //   /  \
        // (s)  (o)---(t)
        //   \  /       \
        //   (n)---(p)---(q)
        try ( Transaction transaction = graphDb.beginTx() )
        {
            graph.makeEdge( transaction, "s", "m" );
            graph.makeEdge( transaction, "m", "o" );
            graph.makeEdge( transaction, "s", "n" );
            graph.makeEdge( transaction, "n", "p" );
            graph.makeEdge( transaction, "p", "q" );
            graph.makeEdge( transaction, "q", "t" );
            graph.makeEdge( transaction, "n", "o" );
            graph.makeEdge( transaction, "o", "t" );

            var context = new BasicEvaluationContext( transaction, graphDb );
            testShortestPathFinder( context, finder ->
            {
                final Iterable<Path> paths = finder.findAllPaths( graph.getNode( transaction, "s" ), graph.getNode( transaction, "t" ) );
                assertPaths( paths, "s,m,o,t", "s,n,o,t" );
            }, forTypeAndDirection( R1, BOTH ), 6 );
            transaction.commit();
        }
    }

    @Test
    void testCrossedCircle()
    {
        // Layout:
        //    (s)
        //   /   \
        // (3)   (1)
        //  | \ / |
        //  | / \ |
        // (4)   (2)
        //   \   /
        //    (t)
        try ( Transaction transaction = graphDb.beginTx() )
        {
            graph.makeEdge( transaction, "s", "1" );
            graph.makeEdge( transaction, "s", "3" );
            graph.makeEdge( transaction, "1", "2" );
            graph.makeEdge( transaction, "1", "4" );
            graph.makeEdge( transaction, "3", "2" );
            graph.makeEdge( transaction, "3", "4" );
            graph.makeEdge( transaction, "2", "t" );
            graph.makeEdge( transaction, "4", "t" );
            var context = new BasicEvaluationContext( transaction, graphDb );
            testShortestPathFinder( context,
                    finder -> assertPaths( finder.findAllPaths( graph.getNode( transaction, "s" ), graph.getNode( transaction, "t" ) ),
                            "s,1,2,t", "s,1,4,t", "s,3,2,t", "s,3,4,t" ),
                    forTypeAndDirection( R1, BOTH ), 3 );
            transaction.commit();
        }
    }

    @Test
    void testDirectedFinder()
    {
        // Layout:
        //
        // (a)->(b)->(c)->(d)->(e)->(f)-------\
        //    \                                v
        //     >(g)->(h)->(i)->(j)->(k)->(l)->(m)
        //
        try ( Transaction transaction = graphDb.beginTx() )
        {
            graph.makeEdgeChain( transaction, "a,b,c,d,e,f,m" );
            graph.makeEdgeChain( transaction, "a,g,h,i,j,k,l,m" );
            var context = new BasicEvaluationContext( transaction, graphDb );

            testShortestPathFinder( context,
                    finder -> assertPaths( finder.findAllPaths( graph.getNode( transaction, "a" ), graph.getNode( transaction, "j" ) ),
                            "a,g,h,i,j" ),
                    forTypeAndDirection( R1, OUTGOING ), 4 );
            transaction.commit();
        }
    }

    @Test
    void makeSureShortestPathsReturnsNoLoops()
    {
        // Layout:
        //
        // (a)-->(b)==>(c)-->(e)
        //        ^    /
        //         \  v
        //         (d)
        //
        try ( Transaction transaction = graphDb.beginTx() )
        {
            graph.makeEdgeChain( transaction, "a,b,c,d,b,c,e" );
            var context = new BasicEvaluationContext( transaction, graphDb );
            testShortestPathFinder( context, finder ->
            {
                final Node a = graph.getNode( transaction, "a" );
                final Node e = graph.getNode( transaction, "e" );
                assertPaths( finder.findAllPaths( a, e ), "a,b,c,e", "a,b,c,e" );
            }, forTypeAndDirection( R1, BOTH ), 6 );
            transaction.commit();
        }
    }

    @Test
    void withFilters()
    {
        // Layout:
        //
        // (a)-->(b)-->(c)-->(d)
        //   \               ^
        //    -->(g)-->(h)--/
        //
        try ( Transaction transaction = graphDb.beginTx() )
        {
            graph.makeEdgeChain( transaction, "a,b,c,d" );
            graph.makeEdgeChain( transaction, "a,g,h,d" );
            final Node a = graph.getNode( transaction, "a" );
            final Node d = graph.getNode( transaction, "d" );
            final Node b = graph.getNode( transaction, "b" );
            b.setProperty( "skip", true );
            var context = new BasicEvaluationContext( transaction, graphDb );
            final Predicate<Node> filter = item ->
            {
                final boolean skip = (Boolean) item.getProperty( "skip", false );
                return !skip;
            };
            testShortestPathFinder( context, finder -> assertPaths( finder.findAllPaths( a, d ), "a,g,h,d" ),
                    ((StandardExpander) allTypesAndDirections()).addNodeFilter( filter ), 10 );
            transaction.commit();
        }
    }

    @Test
    void filtersTouchesAllIntermediateNodes()
    {
        // Layout:
        //
        // (a)-->(b)-->(c)-->(d)
        //
        try ( Transaction transaction = graphDb.beginTx() )
        {
            graph.makeEdgeChain( transaction, "a,b,c,d" );
            final Node a = graph.getNode( transaction, "a" );
            final Node d = graph.getNode( transaction, "d" );
            Collection<Node> touchedByFilter = new HashSet<>();
            final Predicate<Node> filter = item ->
            {
                touchedByFilter.add( item );
                return true;
            };
            final PathExpander expander = PathExpanderBuilder.empty().add( R1, OUTGOING ).addNodeFilter( filter ).build();
            //final PathExpander expander = ((StandardExpander) PathExpanders.forTypeAndDirection(R1, OUTGOING)).addNodeFilter( filter );
            var context = new BasicEvaluationContext( transaction, graphDb );
            Path path = Iterables.single( shortestPath( context, expander, 10 ).findAllPaths( a, d ) );
            assertEquals( 3, path.length() );

            List<Node> nodes = Iterables.asList( path.nodes() );
            List<Node> intermediateNodes = nodes.subList( 1, nodes.size() - 1 );
            assertTrue( touchedByFilter.containsAll( intermediateNodes ), "touchedByFilter: " + touchedByFilter );
            assertFalse( touchedByFilter.contains( a ) );
            assertFalse( touchedByFilter.contains( d ) );
            transaction.commit();
        }
    }

    @Test
    void testFinderShouldNotFindAnythingBeyondLimit()
    {
        // Layout:
        //
        // (a)-->(b)-->(c)-->(d)-->(e)
        //
        try ( Transaction transaction = graphDb.beginTx() )
        {
            graph.makeEdgeChain( transaction, "a,b,c,d,e" );
            var context = new BasicEvaluationContext( transaction, graphDb );
            testShortestPathFinder( context, finder -> assertPaths( finder.findAllPaths( graph.getNode( transaction, "a" ),
                    graph.getNode( transaction, "b" ) ) ),
                    allTypesAndDirections(), 0 );
            testShortestPathFinder( context, finder ->
            {
                assertPaths( finder.findAllPaths( graph.getNode( transaction, "a" ), graph.getNode( transaction, "c" ) ) );
                assertPaths( finder.findAllPaths( graph.getNode( transaction, "a" ), graph.getNode( transaction, "d" ) ) );
            }, allTypesAndDirections(), 1 );
            testShortestPathFinder( context, finder ->
            {
                assertPaths( finder.findAllPaths( graph.getNode( transaction, "a" ), graph.getNode( transaction, "d" ) ) );
                assertPaths( finder.findAllPaths( graph.getNode( transaction, "a" ), graph.getNode( transaction, "e" ) ) );
            }, allTypesAndDirections(), 2 );
            transaction.commit();
        }
    }

    @Test
    void makeSureDescentStopsWhenPathIsFound()
    {
        /*
         * (a)==>(b)==>(c)==>(d)==>(e)
         *   \
         *    v
         *    (f)-->(g)-->(h)-->(i)
         */
        try ( Transaction transaction = graphDb.beginTx() )
        {
            graph.makeEdgeChain( transaction, "a,b,c,d,e" );
            graph.makeEdgeChain( transaction, "a,b,c,d,e" );
            graph.makeEdgeChain( transaction, "a,f,g,h,i" );
            final Node a = graph.getNode( transaction, "a" );
            final Node b = graph.getNode( transaction, "b" );
            final Node c = graph.getNode( transaction, "c" );
            final Set<Node> allowedNodes = new HashSet<>( asList( a, b, c ) );
            var context = new BasicEvaluationContext( transaction, graphDb );
            final PathFinder<Path> finder = new ShortestPath( context, 100, PathExpanders.forDirection( OUTGOING ) )
            {
                @Override
                protected Node filterNextLevelNodes( Node nextNode )
                {
                    if ( !allowedNodes.contains( nextNode ) )
                    {
                        return null;
                    }
                    return nextNode;
                }
            };
            Iterator<Path> paths = finder.findAllPaths( a, c ).iterator();
            for ( int i = 0; i < 4; i++ )
            {
                Path aToBToC = paths.next();
                assertPath( aToBToC, a, b, c );
            }
            assertFalse( paths.hasNext(), "should only have contained four paths" );
            transaction.commit();
        }
    }

    @Test
    void makeSureRelationshipNotConnectedIssueNotThere()
    {
        /*
         *                                  (g)
         *                                  / ^
         *                                 v   \
         * (a)<--(b)<--(c)<--(d)<--(e)<--(f)   (i)
         *                                 ^   /
         *                                  \ v
         *                                  (h)
         */
        try ( Transaction transaction = graphDb.beginTx() )
        {
            graph.makeEdgeChain( transaction, "i,g,f,e,d,c,b,a" );
            graph.makeEdgeChain( transaction, "i,h,f" );
            var context = new BasicEvaluationContext( transaction, graphDb );
            testShortestPathFinder( context, finder ->
            {
                final Node start = graph.getNode( transaction, "a" );
                final Node end = graph.getNode( transaction, "i" );
                assertPaths( finder.findAllPaths( start, end ), "a,b,c,d,e,f,g,i", "a,b,c,d,e,f,h,i" );
            }, forTypeAndDirection( R1, INCOMING ), 10 );
            transaction.commit();
        }
    }

    @Test
    void makeSureShortestPathCanBeFetchedEvenIfANodeHasLoops()
    {
        // Layout:
        //
        // = means loop :)
        //
        //   (m)
        //   /  \
        // (s)  (o)=
        //   \  /
        //   (n)=
        //    |
        //   (p)
        try ( Transaction transaction = graphDb.beginTx() )
        {
            graph.makeEdgeChain( transaction, "m,s,n,p" );
            graph.makeEdgeChain( transaction, "m,o,n" );
            graph.makeEdge( transaction, "o", "o" );
            graph.makeEdge( transaction, "n", "n" );
            var context = new BasicEvaluationContext( transaction, graphDb );
            testShortestPathFinder( context, finder -> assertPaths( finder.findAllPaths( graph.getNode( transaction, "m" ),
                    graph.getNode( transaction, "p" ) ), "m,s,n,p", "m,o,n,p" ),
                    forTypeAndDirection( R1, BOTH ), 3 );
            transaction.commit();
        }
    }

    @Test
    void makeSureAMaxResultCountIsObeyed()
    {
        // Layout:
        //
        //   (a)--(b)--(c)--(d)--(e)
        //    |                 / | \
        //   (f)--(g)---------(h) |  \
        //    |                   |   |
        //   (i)-----------------(j)  |
        //    |                       |
        //   (k)----------------------
        //
        try ( Transaction transaction = graphDb.beginTx() )
        {
            graph.makeEdgeChain( transaction, "a,b,c,d,e" );
            graph.makeEdgeChain( transaction, "a,f,g,h,e" );
            graph.makeEdgeChain( transaction, "f,i,j,e" );
            graph.makeEdgeChain( transaction, "i,k,e" );
            final Node a = graph.getNode( transaction, "a" );
            final Node e = graph.getNode( transaction, "e" );
            final PathExpander expander = forTypeAndDirection( R1, OUTGOING );
            var context = new BasicEvaluationContext( transaction, graphDb );
            testShortestPathFinder( context, finder -> assertEquals( 4, count( finder.findAllPaths( a, e ) ) ), expander, 10, 10 );
            for ( int i = 4; i >= 1; i-- )
            {
                final int count = i;
                testShortestPathFinder( context, finder -> assertEquals( count, count( finder.findAllPaths( a, e ) ) ), expander, 10, count );
            }
            transaction.commit();
        }
    }

    @Test
    void unfortunateRelationshipOrderingInTriangle()
    {
        /*
         *            (b)
         *           ^   \
         *          /     v
         *        (a)---->(c)
         *
         * Relationships are created in such a way that they are iterated in the worst order,
         * i.e. (S) a-->b, (E) c<--b, (S) a-->c
         */
        try ( Transaction transaction = graphDb.beginTx() )
        {
            graph.makeEdgeChain( transaction, "a,b,c" );
            graph.makeEdgeChain( transaction, "a,c" );
            final Node a = graph.getNode( transaction, "a" );
            final Node c = graph.getNode( transaction, "c" );
            var context = new BasicEvaluationContext( transaction, graphDb );
            testShortestPathFinder( context, finder -> assertPathDef( finder.findSinglePath( a, c ), "a", "c" ),
                    forTypeAndDirection( R1, OUTGOING ), 2 );
            testShortestPathFinder( context, finder -> assertPathDef( finder.findSinglePath( c, a ), "c", "a" ),
                    forTypeAndDirection( R1, INCOMING ), 2 );
            transaction.commit();
        }
    }

    @Test
    void shouldFindShortestPathWhenOneSideFindsLongerPathFirst()
    {
        /*
        The order in which nodes are created matters when reproducing the original problem
         */
        try ( Transaction transaction = graphDb.beginTx() )
        {
            graph.makeEdge( transaction, "start", "c" );
            graph.makeEdge( transaction, "start", "a" );
            graph.makeEdge( transaction, "b", "end" );
            graph.makeEdge( transaction, "d", "end" );
            graph.makeEdge( transaction, "c", "e" );
            graph.makeEdge( transaction, "f", "end" );
            graph.makeEdge( transaction, "c", "b" );
            graph.makeEdge( transaction, "e", "end" );
            graph.makeEdge( transaction, "a", "end" );
            final Node start = graph.getNode( transaction, "start" );
            final Node end = graph.getNode( transaction, "end" );
            var context = new BasicEvaluationContext( transaction, graphDb );
            assertThat( new ShortestPath( context, 2, allTypesAndDirections(), 42 ).findSinglePath( start, end ).length() ).isEqualTo( 2 );
            assertThat( new ShortestPath( context, 3, allTypesAndDirections(), 42 ).findSinglePath( start, end ).length() ).isEqualTo( 2 );
            transaction.commit();
        }
    }

    @Test
    void shouldMakeSureResultLimitIsRespectedForMultiPathHits()
    {
        /*       _____
         *      /     \
         *    (a)-----(b)
         *      \_____/
         */
        try ( Transaction transaction = graphDb.beginTx() )
        {
            for ( int i = 0; i < 3; i++ )
            {
                graph.makeEdge( transaction, "a", "b" );
            }

            Node a = graph.getNode( transaction, "a" );
            Node b = graph.getNode( transaction, "b" );
            var context = new BasicEvaluationContext( transaction, graphDb );
            testShortestPathFinder( context, finder -> assertEquals( 1, count( finder.findAllPaths( a, b ) ) ),
                    allTypesAndDirections(), 2, 1 );
            transaction.commit();
        }
    }

    private void testShortestPathFinder( EvaluationContext context, PathFinderTester tester, PathExpander expander, int maxDepth )
    {
        testShortestPathFinder( context, tester, expander, maxDepth, null );
    }

    private void testShortestPathFinder( EvaluationContext context, PathFinderTester tester, PathExpander expander, int maxDepth,
            Integer maxResultCount )
    {
        final LengthCheckingExpanderWrapper lengthChecker = new LengthCheckingExpanderWrapper( expander );
        final List<PathFinder<Path>> finders = new ArrayList<>();
        finders.add( maxResultCount != null ? shortestPath( context, lengthChecker, maxDepth, maxResultCount )
                                            : shortestPath( context, lengthChecker, maxDepth ) );
        finders.add( maxResultCount != null ? new TraversalShortestPath( context, lengthChecker, maxDepth, maxResultCount )
                                            : new TraversalShortestPath( context, lengthChecker, maxDepth ) );
        for ( final PathFinder<Path> finder : finders )
        {
            tester.test( finder );
        }
    }

    private interface PathFinderTester
    {
        void test( PathFinder<Path> finder );
    }

    private static class LengthCheckingExpanderWrapper implements PathExpander<Object>
    {
        private final PathExpander expander;

        LengthCheckingExpanderWrapper( PathExpander expander )
        {
            this.expander = expander;
        }

        @Override
        @SuppressWarnings( "unchecked" )
        public Iterable<Relationship> expand( Path path, BranchState<Object> state )
        {
            if ( path.startNode().equals( path.endNode() ) )
            {
                assertEquals( 0, path.length(), "Path length must be zero" );
            }
            else
            {
                assertTrue( path.length() > 0, "Path length must be positive" );
            }
            return expander.expand( path, state );
        }

        @Override
        public PathExpander<Object> reverse()
        {
            return new LengthCheckingExpanderWrapper( expander.reverse() );
        }
    }

    // Used to count how many nodes are visited
    private static class CountingPathExpander implements PathExpander
    {
        private MutableInt nodesVisited;
        private final PathExpander delegate;

        CountingPathExpander( PathExpander delegate )
        {
            nodesVisited = new MutableInt( 0 );
            this.delegate = delegate;
        }

        CountingPathExpander( PathExpander delegate, MutableInt nodesVisited )
        {
            this( delegate );
            this.nodesVisited = nodesVisited;
        }

        @Override
        public Iterable expand( Path path, BranchState state )
        {
            nodesVisited.increment();
            return delegate.expand( path, state );
        }

        @Override
        public PathExpander reverse()
        {
            return new CountingPathExpander( delegate.reverse(), nodesVisited );
        }
    }
}
