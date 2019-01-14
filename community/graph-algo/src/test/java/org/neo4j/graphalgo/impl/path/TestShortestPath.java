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
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.PathExpanderBuilder;
import org.neo4j.graphdb.PathExpanders;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.impl.StandardExpander;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.helpers.collection.Iterables;

import static common.Neo4jAlgoTestCase.MyRelTypes.R1;
import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphalgo.GraphAlgoFactory.shortestPath;
import static org.neo4j.graphdb.Direction.BOTH;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.graphdb.PathExpanders.allTypesAndDirections;
import static org.neo4j.helpers.collection.Iterables.count;

public class TestShortestPath extends Neo4jAlgoTestCase
{
    // Attempt at recreating this issue without cypher
    // https://github.com/neo4j/neo4j/issues/4160
    @Test
    public void shouldAbortAsSoonAsPossible()
    {
        final Label A = Label.label( "A" );
        final Label B = Label.label( "B" );
        final Label C = Label.label( "C" );
        final Label D = Label.label( "D" );
        final Label E = Label.label( "E" );
        final Label F = Label.label( "F" );
        final RelationshipType relType = RelationshipType.withName( "TO" );
        recursiveSnowFlake( null, 0, 4, 5, new Label[]{A, B, C, D, E}, relType );
        Node a = getNodeByLabel( A );
        try ( ResourceIterator<Node> allE = graphDb.findNodes( E ) )
        {
            while ( allE.hasNext() )
            {
                final Node e = allE.next();
                final Node f = graphDb.createNode( F );
                f.createRelationshipTo( e, relType );
            }
        }
        final CountingPathExpander countingPathExpander =
                new CountingPathExpander( PathExpanders.forTypeAndDirection( relType, Direction.OUTGOING ) );
        final ShortestPath shortestPath = new ShortestPath( Integer.MAX_VALUE, countingPathExpander, Integer.MAX_VALUE );
        try ( ResourceIterator<Node> allF = graphDb.findNodes( F ) )
        {
            while ( allF.hasNext() )
            {
                final Node f = allF.next();
                shortestPath.findAllPaths( a, f );
            }
        }
        assertEquals(
                "There are 625 different end nodes. The algorithm should start one traversal for each such node. " +
                        "That is 625*2 visited nodes if traversal is interrupted correctly.", 1250,
                countingPathExpander.nodesVisited.intValue() );
    }

    private Node getNodeByLabel( Label label )
    {
        try ( ResourceIterator<Node> iterator = graphDb.findNodes( label ) )
        {
            return iterator.next();
        }
    }

    private void recursiveSnowFlake( Node parent, int level, final int desiredLevel, final int branchingFactor,
            final Label[] labels, final RelationshipType relType )
    {
        if ( level != 0 )
        {
            for ( int n = 0; n < branchingFactor; n++ )
            {
                final Node node = graphDb.createNode( labels[level] );
                if ( parent != null )
                {
                    parent.createRelationshipTo( node, relType );
                }
                if ( level < desiredLevel )
                {
                    recursiveSnowFlake( node, level + 1, desiredLevel, branchingFactor, labels, relType );
                }
            }
        }
        else
        {
            final Node node = graphDb.createNode( labels[level] );
            recursiveSnowFlake( node, level + 1, desiredLevel, branchingFactor, labels, relType );
        }
    }

    @Test
    public void testSimplestGraph()
    {
        // Layout:
        //    __
        //   /  \
        // (s)  (t)
        //   \__/
        graph.makeEdge( "s", "t" );
        graph.makeEdge( "s", "t" );
        testShortestPathFinder( finder ->
        {
            final Iterable<Path> paths = finder.findAllPaths( graph.getNode( "s" ), graph.getNode( "t" ) );
            assertPaths( paths, "s,t", "s,t" );
            assertPaths( asList( finder.findSinglePath( graph.getNode( "s" ), graph.getNode( "t" ) ) ), "s,t" );
        }, PathExpanders.forTypeAndDirection( R1, BOTH ), 1 );
    }

    @Test
    public void testAnotherSimpleGraph()
    {
        // Layout:
        //   (m)
        //   /  \
        // (s)  (o)---(t)
        //   \  /       \
        //   (n)---(p)---(q)
        graph.makeEdge( "s", "m" );
        graph.makeEdge( "m", "o" );
        graph.makeEdge( "s", "n" );
        graph.makeEdge( "n", "p" );
        graph.makeEdge( "p", "q" );
        graph.makeEdge( "q", "t" );
        graph.makeEdge( "n", "o" );
        graph.makeEdge( "o", "t" );
        testShortestPathFinder( finder ->
        {
            final Iterable<Path> paths = finder.findAllPaths( graph.getNode( "s" ), graph.getNode( "t" ) );
            assertPaths( paths, "s,m,o,t", "s,n,o,t" );
        }, PathExpanders.forTypeAndDirection( R1, BOTH ), 6 );
    }

    @Test
    public void testCrossedCircle()
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
        graph.makeEdge( "s", "1" );
        graph.makeEdge( "s", "3" );
        graph.makeEdge( "1", "2" );
        graph.makeEdge( "1", "4" );
        graph.makeEdge( "3", "2" );
        graph.makeEdge( "3", "4" );
        graph.makeEdge( "2", "t" );
        graph.makeEdge( "4", "t" );
        testShortestPathFinder(
                finder -> assertPaths( finder.findAllPaths( graph.getNode( "s" ), graph.getNode( "t" ) ), "s,1,2,t",
                        "s,1,4,t", "s,3,2,t", "s,3,4,t" ), PathExpanders.forTypeAndDirection( R1, BOTH ), 3 );
    }

    @Test
    public void testDirectedFinder()
    {
        // Layout:
        //
        // (a)->(b)->(c)->(d)->(e)->(f)-------\
        //    \                                v
        //     >(g)->(h)->(i)->(j)->(k)->(l)->(m)
        //
        graph.makeEdgeChain( "a,b,c,d,e,f,m" );
        graph.makeEdgeChain( "a,g,h,i,j,k,l,m" );
        testShortestPathFinder(
                finder -> assertPaths( finder.findAllPaths( graph.getNode( "a" ), graph.getNode( "j" ) ), "a,g,h,i,j" ),
                PathExpanders.forTypeAndDirection( R1, OUTGOING ), 4 );
    }

    @Test
    public void makeSureShortestPathsReturnsNoLoops()
    {
        // Layout:
        //
        // (a)-->(b)==>(c)-->(e)
        //        ^    /
        //         \  v
        //         (d)
        //
        graph.makeEdgeChain( "a,b,c,d,b,c,e" );
        testShortestPathFinder( finder ->
        {
            final Node a = graph.getNode( "a" );
            final Node e = graph.getNode( "e" );
            assertPaths( finder.findAllPaths( a, e ), "a,b,c,e", "a,b,c,e" );
        }, PathExpanders.forTypeAndDirection( R1, BOTH ), 6 );
    }

    @Test
    public void withFilters()
    {
        // Layout:
        //
        // (a)-->(b)-->(c)-->(d)
        //   \               ^
        //    -->(g)-->(h)--/
        //
        graph.makeEdgeChain( "a,b,c,d" );
        graph.makeEdgeChain( "a,g,h,d" );
        final Node a = graph.getNode( "a" );
        final Node d = graph.getNode( "d" );
        final Node b = graph.getNode( "b" );
        b.setProperty( "skip", true );
        final Predicate<Node> filter = item ->
        {
            final boolean skip = (Boolean) item.getProperty( "skip", false );
            return !skip;
        };
        testShortestPathFinder( finder -> assertPaths( finder.findAllPaths( a, d ), "a,g,h,d" ),
                ((StandardExpander) PathExpanders.allTypesAndDirections()).addNodeFilter( filter ), 10 );
    }

    @Test
    public void filtersTouchesAllIntermediateNodes()
    {
        // Layout:
        //
        // (a)-->(b)-->(c)-->(d)
        //
        graph.makeEdgeChain( "a,b,c,d" );
        final Node a = graph.getNode( "a" );
        final Node d = graph.getNode( "d" );
        Collection<Node> touchedByFilter = new HashSet<>();
        final Predicate<Node> filter = item ->
        {
            touchedByFilter.add( item );
            return true;
        };
        final PathExpander expander =
                PathExpanderBuilder.empty().add( MyRelTypes.R1, OUTGOING ).addNodeFilter( filter ).build();
        //final PathExpander expander = ((StandardExpander) PathExpanders.forTypeAndDirection(R1, OUTGOING)).addNodeFilter( filter );
        Path path = Iterables.single( GraphAlgoFactory.shortestPath( expander, 10 ).findAllPaths( a, d ) );
        assertEquals( 3, path.length() );

        List<Node> nodes = Iterables.asList( path.nodes() );
        List<Node> intermediateNodes = nodes.subList( 1, nodes.size() - 1 );
        assertTrue( "touchedByFilter: " + touchedByFilter, touchedByFilter.containsAll( intermediateNodes ) );
        assertTrue( "startNode was not filtered", !touchedByFilter.contains( a ) );
        assertTrue( "endNode was not filtered", !touchedByFilter.contains( d ) );
    }

    @Test
    public void testFinderShouldNotFindAnythingBeyondLimit()
    {
        // Layout:
        //
        // (a)-->(b)-->(c)-->(d)-->(e)
        //
        graph.makeEdgeChain( "a,b,c,d,e" );
        testShortestPathFinder(
                finder -> assertPaths( finder.findAllPaths( graph.getNode( "a" ), graph.getNode( "b" ) ) ),
                PathExpanders.allTypesAndDirections(), 0 );
        testShortestPathFinder( finder ->
        {
            assertPaths( finder.findAllPaths( graph.getNode( "a" ), graph.getNode( "c" ) ) );
            assertPaths( finder.findAllPaths( graph.getNode( "a" ), graph.getNode( "d" ) ) );
        }, PathExpanders.allTypesAndDirections(), 1 );
        testShortestPathFinder( finder ->
        {
            assertPaths( finder.findAllPaths( graph.getNode( "a" ), graph.getNode( "d" ) ) );
            assertPaths( finder.findAllPaths( graph.getNode( "a" ), graph.getNode( "e" ) ) );
        }, PathExpanders.allTypesAndDirections(), 2 );
    }

    @Test
    public void makeSureDescentStopsWhenPathIsFound()
    {
        /*
         * (a)==>(b)==>(c)==>(d)==>(e)
         *   \
         *    v
         *    (f)-->(g)-->(h)-->(i)
         */
        graph.makeEdgeChain( "a,b,c,d,e" );
        graph.makeEdgeChain( "a,b,c,d,e" );
        graph.makeEdgeChain( "a,f,g,h,i" );
        final Node a = graph.getNode( "a" );
        final Node b = graph.getNode( "b" );
        final Node c = graph.getNode( "c" );
        final Set<Node> allowedNodes = new HashSet<>( Arrays.asList( a, b, c ) );
        final PathFinder<Path> finder = new ShortestPath( 100, PathExpanders.forDirection( OUTGOING ) )
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
        assertFalse( "should only have contained four paths", paths.hasNext() );
    }

    @Test
    public void makeSureRelationshipNotConnectedIssueNotThere()
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
        graph.makeEdgeChain( "i,g,f,e,d,c,b,a" );
        graph.makeEdgeChain( "i,h,f" );
        testShortestPathFinder( finder ->
        {
            final Node start = graph.getNode( "a" );
            final Node end = graph.getNode( "i" );
            assertPaths( finder.findAllPaths( start, end ), "a,b,c,d,e,f,g,i", "a,b,c,d,e,f,h,i" );
        }, PathExpanders.forTypeAndDirection( R1, INCOMING ), 10 );
    }

    @Test
    public void makeSureShortestPathCanBeFetchedEvenIfANodeHasLoops()
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
        graph.makeEdgeChain( "m,s,n,p" );
        graph.makeEdgeChain( "m,o,n" );
        graph.makeEdge( "o", "o" );
        graph.makeEdge( "n", "n" );
        testShortestPathFinder(
                finder -> assertPaths( finder.findAllPaths( graph.getNode( "m" ), graph.getNode( "p" ) ), "m,s,n,p",
                        "m,o,n,p" ), PathExpanders.forTypeAndDirection( R1, BOTH ), 3 );
    }

    @Test
    public void makeSureAMaxResultCountIsObeyed()
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
        graph.makeEdgeChain( "a,b,c,d,e" );
        graph.makeEdgeChain( "a,f,g,h,e" );
        graph.makeEdgeChain( "f,i,j,e" );
        graph.makeEdgeChain( "i,k,e" );
        final Node a = graph.getNode( "a" );
        final Node e = graph.getNode( "e" );
        final PathExpander expander = PathExpanders.forTypeAndDirection( R1, OUTGOING );
        testShortestPathFinder( finder -> assertEquals( 4, Iterables.count( finder.findAllPaths( a, e ) ) ), expander,
                10, 10 );
        for ( int i = 4; i >= 1; i-- )
        {
            final int count = i;
            testShortestPathFinder( finder -> assertEquals( count, Iterables.count( finder.findAllPaths( a, e ) ) ),
                    expander, 10, count );
        }
    }

    @Test
    public void unfortunateRelationshipOrderingInTriangle()
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
        graph.makeEdgeChain( "a,b,c" );
        graph.makeEdgeChain( "a,c" );
        final Node a = graph.getNode( "a" );
        final Node c = graph.getNode( "c" );
        testShortestPathFinder( finder -> assertPathDef( finder.findSinglePath( a, c ), "a", "c" ),
                PathExpanders.forTypeAndDirection( R1, OUTGOING ), 2 );
        testShortestPathFinder( finder -> assertPathDef( finder.findSinglePath( c, a ), "c", "a" ),
                PathExpanders.forTypeAndDirection( R1, INCOMING ), 2 );
    }

    @Test
    public void shouldFindShortestPathWhenOneSideFindsLongerPathFirst()
    {
        /*
        The order in which nodes are created matters when reproducing the original problem
         */
        graph.makeEdge( "start", "c" );
        graph.makeEdge( "start", "a" );
        graph.makeEdge( "b", "end" );
        graph.makeEdge( "d", "end" );
        graph.makeEdge( "c", "e" );
        graph.makeEdge( "f", "end" );
        graph.makeEdge( "c", "b" );
        graph.makeEdge( "e", "end" );
        graph.makeEdge( "a", "end" );
        final Node start = graph.getNode( "start" );
        final Node end = graph.getNode( "end" );
        assertThat( new ShortestPath( 2, allTypesAndDirections(), 42 ).findSinglePath( start, end ).length(), is( 2 ) );
        assertThat( new ShortestPath( 3, allTypesAndDirections(), 42 ).findSinglePath( start, end ).length(), is( 2 ) );
    }

    @Test
    public void shouldMakeSureResultLimitIsRespectedForMultiPathHits()
    {
        /*       _____
         *      /     \
         *    (a)-----(b)
         *      \_____/
         */
        for ( int i = 0; i < 3; i++ )
        {
            graph.makeEdge( "a", "b" );
        }

        Node a = graph.getNode( "a" );
        Node b = graph.getNode( "b" );
        testShortestPathFinder( finder -> assertEquals( 1, count( finder.findAllPaths( a, b ) ) ), allTypesAndDirections(), 2, 1 );
    }

    private void testShortestPathFinder( PathFinderTester tester, PathExpander expander, int maxDepth )
    {
        testShortestPathFinder( tester, expander, maxDepth, null );
    }

    private void testShortestPathFinder( PathFinderTester tester, PathExpander expander, int maxDepth,
            Integer maxResultCount )
    {
        final LengthCheckingExpanderWrapper lengthChecker = new LengthCheckingExpanderWrapper( expander );
        final List<PathFinder<Path>> finders = new ArrayList<>();
        finders.add( maxResultCount != null ? shortestPath( lengthChecker, maxDepth, maxResultCount )
                                            : shortestPath( lengthChecker, maxDepth ) );
        finders.add( maxResultCount != null ? new TraversalShortestPath( lengthChecker, maxDepth, maxResultCount )
                                            : new TraversalShortestPath( lengthChecker, maxDepth ) );
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
                assertTrue( "Path length must be zero", path.length() == 0 );
            }
            else
            {
                assertTrue( "Path length must be positive", path.length() > 0 );
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
    private class CountingPathExpander implements PathExpander
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
