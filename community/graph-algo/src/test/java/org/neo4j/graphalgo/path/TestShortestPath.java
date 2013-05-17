/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.graphalgo.path;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import common.Neo4jAlgoTestCase;
import org.junit.Ignore;
import org.junit.Test;

import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.impl.path.ShortestPath;
import org.neo4j.graphalgo.impl.path.TraversalShortestPath;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipExpander;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.helpers.Predicate;
import org.neo4j.kernel.StandardExpander;
import org.neo4j.kernel.Traversal;

import static java.util.Arrays.asList;
import static common.Neo4jAlgoTestCase.MyRelTypes.R1;
import static common.SimpleGraphBuilder.KEY_ID;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.graphalgo.GraphAlgoFactory.shortestPath;
import static org.neo4j.graphdb.Direction.BOTH;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.helpers.collection.IteratorUtil.count;
import static org.neo4j.kernel.Traversal.expanderForAllTypes;
import static org.neo4j.kernel.Traversal.expanderForTypes;

public class TestShortestPath extends Neo4jAlgoTestCase
{
//    protected PathFinder<Path> instantiatePathFinder( int maxDepth )
//    {
//        return instantiatePathFinder( Traversal.expanderForTypes( MyRelTypes.R1,
//                Direction.BOTH ), maxDepth );
//    }
//    
//    protected PathFinder<Path> instantiatePathFinder( RelationshipExpander expander, int maxDepth )
//    {
////        return GraphAlgoFactory.shortestPath( expander, maxDepth );
//        return new TraversalShortestPath( expander, maxDepth );
//    }

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

        testShortestPathFinder( new PathFinderTester()
        {
            @Override
            public void test( PathFinder<Path> finder )
            {
                Iterable<Path> paths = finder.findAllPaths( graph.getNode( "s" ), graph.getNode( "t" ) );
                assertPaths( paths, "s,t", "s,t" );
                assertPaths( asList( finder.findSinglePath( graph.getNode( "s" ), graph.getNode( "t" ) ) ), "s,t" );
            }
        }, expanderForTypes( R1, BOTH ), 1 );
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

        testShortestPathFinder( new PathFinderTester()
        {
            @Override
            public void test( PathFinder<Path> finder )
            {
                Iterable<Path> paths = finder.findAllPaths( graph.getNode( "s" ), graph.getNode( "t" ) );
                assertPaths( paths, "s,m,o,t", "s,n,o,t" );
            }
        }, expanderForTypes( R1, BOTH ), 6 );
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

        testShortestPathFinder( new PathFinderTester()
        {
            @Override
            public void test( PathFinder<Path> finder )
            {
                assertPaths( finder.findAllPaths( graph.getNode( "s" ), graph.getNode( "t" ) ),
                        "s,1,2,t", "s,1,4,t", "s,3,2,t", "s,3,4,t" );
            }
        }, expanderForTypes( R1, BOTH ), 3 );
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

        testShortestPathFinder( new PathFinderTester()
        {
            @Override
            public void test( PathFinder<Path> finder )
            {
                assertPaths( finder.findAllPaths( graph.getNode( "a" ), graph.getNode( "j" ) ), "a,g,h,i,j" );
            }
        }, expanderForTypes( R1, OUTGOING ), 4 );
    }

    @Test
    public void testExactDepthFinder()
    {
        // Layout (a to k):
        //
        //     (a)--(c)--(g)--(k)
        //    /                /
        //  (b)-----(d)------(j)
        //   |        \      /
        //  (e)--(f)--(h)--(i)
        // 
        graph.makeEdgeChain( "a,c,g,k" );
        graph.makeEdgeChain( "a,b,d,j,k" );
        graph.makeEdgeChain( "b,e,f,h,i,j" );
        graph.makeEdgeChain( "d,h" );

        RelationshipExpander expander = Traversal.expanderForTypes( MyRelTypes.R1, Direction.OUTGOING );
        Node a = graph.getNode( "a" );
        Node k = graph.getNode( "k" );
        assertPaths( GraphAlgoFactory.pathsWithLength( expander, 3 ).findAllPaths( a, k ), "a,c,g,k" );
        assertPaths( GraphAlgoFactory.pathsWithLength( expander, 4 ).findAllPaths( a, k ), "a,b,d,j,k" );
        assertPaths( GraphAlgoFactory.pathsWithLength( expander, 5 ).findAllPaths( a, k ) );
        assertPaths( GraphAlgoFactory.pathsWithLength( expander, 6 ).findAllPaths( a, k ), "a,b,d,h,i,j,k" );
        assertPaths( GraphAlgoFactory.pathsWithLength( expander, 7 ).findAllPaths( a, k ), "a,b,e,f,h,i,j,k" );
        assertPaths( GraphAlgoFactory.pathsWithLength( expander, 8 ).findAllPaths( a, k ) );
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

        testShortestPathFinder( new PathFinderTester()
        {
            @Override
            public void test( PathFinder<Path> finder )
            {
                Node a = graph.getNode( "a" );
                Node e = graph.getNode( "e" );
                assertPaths( finder.findAllPaths( a, e ), "a,b,c,e", "a,b,c,e" );
            }
        }, expanderForTypes( R1, BOTH ), 6 );
    }

    @Test
    public void testExactDepthPathsReturnsNoLoops()
    {
        // Layout:
        //
        // (a)-->(b)==>(c)-->(e)
        //        ^    /
        //         \  v
        //         (d)
        //
        graph.makeEdgeChain( "a,b,c,d,b,c,e" );

        Node a = graph.getNode( "a" );
        Node e = graph.getNode( "e" );
        assertPaths( GraphAlgoFactory.pathsWithLength(
                Traversal.expanderForTypes( MyRelTypes.R1 ), 3 ).findAllPaths( a, e ), "a,b,c,e", "a,b,c,e" );
        assertPaths( GraphAlgoFactory.pathsWithLength(
                Traversal.expanderForTypes( MyRelTypes.R1 ), 4 ).findAllPaths( a, e ), "a,b,d,c,e" );
        assertPaths( GraphAlgoFactory.pathsWithLength(
                Traversal.expanderForTypes( MyRelTypes.R1 ), 6 ).findAllPaths( a, e ) );
    }

    @Test
    public void withFilters() throws Exception
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
        Predicate<Node> filter = new Predicate<Node>()
        {
            @Override
            public boolean accept( Node item )
            {
                boolean skip = (Boolean) item.getProperty( "skip", false );
                return !skip;
            }
        };

        testShortestPathFinder( new PathFinderTester()
        {
            @Override
            public void test( PathFinder<Path> finder )
            {
                assertPaths( finder.findAllPaths( a, d ), "a,g,h,d" );
            }
        }, Traversal.expanderForAllTypes().addNodeFilter( filter ), 10 );
    }

    @Test
    public void testFinderShouldNotFindAnythingBeyondLimit()
    {
        // Layout:
        //
        // (a)-->(b)-->(c)-->(d)-->(e)
        //
        graph.makeEdgeChain( "a,b,c,d,e" );

        testShortestPathFinder( new PathFinderTester()
        {
            @Override
            public void test( PathFinder<Path> finder )
            {
                assertPaths( finder.findAllPaths( graph.getNode( "a" ), graph.getNode( "b" ) ) );
            }
        }, Traversal.emptyExpander(), 0 );

        testShortestPathFinder( new PathFinderTester()
        {
            @Override
            public void test( PathFinder<Path> finder )
            {
                assertPaths( finder.findAllPaths( graph.getNode( "a" ), graph.getNode( "c" ) ) );
                assertPaths( finder.findAllPaths( graph.getNode( "a" ), graph.getNode( "d" ) ) );
            }
        }, Traversal.emptyExpander(), 1 );

        testShortestPathFinder( new PathFinderTester()
        {
            @Override
            public void test( PathFinder<Path> finder )
            {
                assertPaths( finder.findAllPaths( graph.getNode( "a" ), graph.getNode( "d" ) ) );
                assertPaths( finder.findAllPaths( graph.getNode( "a" ), graph.getNode( "e" ) ) );
            }
        }, Traversal.emptyExpander(), 2 );
    }

    @Test
    public void makeSureDescentStopsWhenPathIsFound() throws Exception
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

        Node a = graph.getNode( "a" );
        Node b = graph.getNode( "b" );
        Node c = graph.getNode( "c" );
        final Set<Node> allowedNodes = new HashSet<Node>( Arrays.asList( a, b, c ) );

        PathFinder<Path> finder = new ShortestPath( 100, Traversal.expanderForAllTypes( Direction.OUTGOING ) )
        {
            @Override
            protected Collection<Node> filterNextLevelNodes( Collection<Node> nextNodes )
            {
                for ( Node node : nextNodes )
                {
                    if ( !allowedNodes.contains( node ) )
                    {
                        fail( "Node " + node.getProperty( KEY_ID ) + " shouldn't be expanded" );
                    }
                }
                return nextNodes;
            }
        };
        finder.findAllPaths( a, c );
    }

    @Test
    public void makeSureRelationshipNotConnectedIssueNotThere() throws Exception
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

        testShortestPathFinder( new PathFinderTester()
        {

            @Override
            public void test( PathFinder<Path> finder )
            {
                Node start = graph.getNode( "a" );
                Node end = graph.getNode( "i" );
                assertPaths( finder.findAllPaths( start, end ), "a,b,c,d,e,f,g,i", "a,b,c,d,e,f,h,i" );
            }
        }, expanderForTypes( R1, INCOMING ), 10 );
    }

    @Test
    public void makeSureShortestPathCanBeFetchedEvenIfANodeHasLoops() throws Exception
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

        testShortestPathFinder( new PathFinderTester()
        {
            @Override
            public void test( PathFinder<Path> finder )
            {
                assertPaths( finder.findAllPaths( graph.getNode( "m" ), graph.getNode( "p" ) ), "m,s,n,p", "m,o,n,p" );
            }
        }, expanderForTypes( R1, BOTH ), 3 );
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
        RelationshipExpander expander = expanderForTypes( R1, OUTGOING );

        testShortestPathFinder( new PathFinderTester()
        {
            @Override
            public void test( PathFinder<Path> finder )
            {
                assertEquals( 4, count( finder.findAllPaths( a, e ) ) );
            }
        }, expander, 10, 10 );

        for ( int i = 4; i >= 1; i-- )
        {
            final int count = i;
            testShortestPathFinder( new PathFinderTester()
            {
                @Override
                public void test( PathFinder<Path> finder )
                {
                    assertEquals( count, count( finder.findAllPaths( a, e ) ) );
                }
            }, expander, 10, count );
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

        testShortestPathFinder( new PathFinderTester()
        {
            @Override
            public void test( PathFinder<Path> finder )
            {
                assertPathDef( finder.findSinglePath( a, c ), "a", "c" );
            }
        }, expanderForTypes( R1, OUTGOING ), 2 );

        testShortestPathFinder( new PathFinderTester()
        {
            @Override
            public void test( PathFinder<Path> finder )
            {
                assertPathDef( finder.findSinglePath( c, a ), "c", "a" );
            }
        }, expanderForTypes( R1, INCOMING ), 2 );
    }

    @Ignore("Exposes a problem where the expected path isn't returned")
    @Test
    public void pathsWithLengthProblem() throws Exception
    {
        /*
         * 
         *    (a)-->(b)-->(c)<--(f)
         *      \   ^      |
         *       v /       v
         *       (d)      (e)
         * 
         */

        graph.makeEdgeChain( "f,c" );
        graph.makeEdgeChain( "c,e" );
        graph.makeEdgeChain( "a,b,c" );
        graph.makeEdgeChain( "a,d,b" );

        Node a = graph.getNode( "a" );
        Node c = graph.getNode( "c" );

        assertPaths( new ShortestPath( 3, expanderForTypes( R1 ), 10, true ).findAllPaths( a, c ), "a,d,b,c" );
    }

    @Test
    public void shouldFindShortestPathWhenOneSideFindsLongerPathFirst() throws Exception
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

        Node start = graph.getNode( "start" );
        Node end = graph.getNode( "end" );

        assertThat( new ShortestPath( 2, expanderForAllTypes(), 42 ).findSinglePath( start, end ).length(), is( 2 ) );
        assertThat( new ShortestPath( 3, expanderForAllTypes(), 42 ).findSinglePath( start, end ).length(), is( 2 ) );
    }
    
    private void testShortestPathFinder( PathFinderTester tester, RelationshipExpander expander, int maxDepth )
    {
        testShortestPathFinder( tester, expander, maxDepth, null );
    }

    private void testShortestPathFinder( PathFinderTester tester, RelationshipExpander expander, int maxDepth,
                                         Integer maxResultCount )
    {
        LengthCheckingExpanderWrapper lengthChecker = new LengthCheckingExpanderWrapper( StandardExpander.toPathExpander( expander ) );
        
        List<PathFinder<Path>> finders = new ArrayList<PathFinder<Path>>();
        finders.add( maxResultCount != null ? shortestPath( lengthChecker, maxDepth, maxResultCount ) : shortestPath(
                lengthChecker, maxDepth ) );
        finders.add( maxResultCount != null ? new TraversalShortestPath( lengthChecker, maxDepth,
                maxResultCount ) : new TraversalShortestPath( lengthChecker, maxDepth ) );
        for ( PathFinder<Path> finder : finders )
        {
            tester.test( finder );
        }
    }
    
    private interface PathFinderTester
    {
        void test( PathFinder<Path> finder );
    }

    private static class LengthCheckingExpanderWrapper implements PathExpander<Object> {

        private PathExpander expander;
        
        LengthCheckingExpanderWrapper( PathExpander expander )
        {
            this.expander = expander;
        }
        
        @Override
        @SuppressWarnings("unchecked")
        public Iterable<Relationship> expand( Path path, BranchState<Object> state ) 
        {           
            if ( path.startNode().equals(path.endNode()) ) 
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
}
