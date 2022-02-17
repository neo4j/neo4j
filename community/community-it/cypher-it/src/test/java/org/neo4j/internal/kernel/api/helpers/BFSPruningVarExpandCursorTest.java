/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.internal.kernel.api.helpers;

import org.eclipse.collections.impl.block.factory.primitive.LongPredicates;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.function.Consumer;
import java.util.function.Function;

import org.neo4j.exceptions.KernelException;
import org.neo4j.function.Predicates;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.internal.kernel.api.helpers.BFSPruningVarExpandCursor.incomingExpander;
import static org.neo4j.internal.kernel.api.helpers.BFSPruningVarExpandCursor.outgoingExpander;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.api.KernelTransaction.Type.EXPLICIT;

@ImpermanentDbmsExtension
class BFSPruningVarExpandCursorTest
{
    @Inject
    private Kernel kernel;

    @Test
    void shouldDoBreadthFirstSearch() throws KernelException
    {
        //given
        try ( var tx = kernel.beginTransaction( EXPLICIT, AUTH_DISABLED );
              var nodeCursor = tx.cursors().allocateNodeCursor( NULL_CONTEXT );
              var relCursor = tx.cursors().allocateRelationshipTraversalCursor( NULL_CONTEXT )
        )
        {
            Write write = tx.dataWrite();
            int rel = tx.tokenWrite().relationshipTypeGetOrCreateForName( "R" );
            //          (a1) → (b1)
            //        ↗ (a2) → (b2)
            //(start) → (a3) → (b3)
            //        ↘ (a4) → (b4)
            //          (a5) → (b5)
            long start = write.nodeCreate();
            //layer 1
            long a1 = write.nodeCreate();
            long a2 = write.nodeCreate();
            long a3 = write.nodeCreate();
            long a4 = write.nodeCreate();
            long a5 = write.nodeCreate();
            //layer 2
            long b1 = write.nodeCreate();
            long b2 = write.nodeCreate();
            long b3 = write.nodeCreate();
            long b4 = write.nodeCreate();
            long b5 = write.nodeCreate();

            //layer 1
            write.relationshipCreate( start, rel, a1 );
            write.relationshipCreate( start, rel, a2 );
            write.relationshipCreate( start, rel, a3 );
            var filterThis = write.relationshipCreate( start, rel, a4 );
            write.relationshipCreate( start, rel, a5 );
            //layer 2
            write.relationshipCreate( a1, rel, b1 );
            var andFilterThat = write.relationshipCreate( a2, rel, b2 );
            write.relationshipCreate( a3, rel, b3 );
            write.relationshipCreate( a4, rel, b4 );
            write.relationshipCreate( a5, rel, b5 );

            //when
            var expander =
                    outgoingExpander( start, 26, tx.dataRead(), nodeCursor, relCursor, EmptyMemoryTracker.INSTANCE );

            //then
            assertThat( asList( expander, BFSPruningVarExpandCursor::otherNodeReference ) ).isEqualTo( List.of( a1, a2, a3, a4, a5,  b1, b2, b3, b4, b5 ) );
        }
    }

    @Test
    void shouldDoBreadthFirstSearchWithNodePredicate() throws KernelException
    {
        //given
        try ( var tx = kernel.beginTransaction( EXPLICIT, AUTH_DISABLED );
              var nodeCursor = tx.cursors().allocateNodeCursor( NULL_CONTEXT );
              var relCursor = tx.cursors().allocateRelationshipTraversalCursor( NULL_CONTEXT )
        )
        {
            Write write = tx.dataWrite();
            int rel = tx.tokenWrite().relationshipTypeGetOrCreateForName( "R" );
            //          (a1) → (b1)
            //        ↗ (a2) → (b2)
            //(start) → (X) → (b3)
            //        ↘ (a4) → (b4)
            //          (a5) → (b5)
            long start = write.nodeCreate();
            //layer 1
            long a1 = write.nodeCreate();
            long a2 = write.nodeCreate();
            long a3 = write.nodeCreate();
            long a4 = write.nodeCreate();
            long a5 = write.nodeCreate();
            //layer 2
            long b1 = write.nodeCreate();
            long b2 = write.nodeCreate();
            long b3 = write.nodeCreate();
            long b4 = write.nodeCreate();
            long b5 = write.nodeCreate();

            //layer 1
            write.relationshipCreate( start, rel, a1 );
            write.relationshipCreate( start, rel, a2 );
            write.relationshipCreate( start, rel, a3 );
            var filterThis = write.relationshipCreate( start, rel, a4 );
            write.relationshipCreate( start, rel, a5 );
            //layer 2
            write.relationshipCreate( a1, rel, b1 );
            var andFilterThat = write.relationshipCreate( a2, rel, b2 );
            write.relationshipCreate( a3, rel, b3 );
            write.relationshipCreate( a4, rel, b4 );
            write.relationshipCreate( a5, rel, b5 );

            //when
            var expander =
                    outgoingExpander( start, 26, tx.dataRead(), nodeCursor, relCursor,
                                      n -> n != a3,
                                      Predicates.alwaysTrue(),
                                      EmptyMemoryTracker.INSTANCE );

            //then
            assertThat( asList( expander, BFSPruningVarExpandCursor::otherNodeReference ) ).isEqualTo( List.of( a1, a2, a4, a5, b1, b2, b4, b5 ) );
        }
    }

    @Test
    void shouldDoBreadthFirstSearchWithRelationshipPredicate() throws KernelException
    {
        //given
        try ( var tx = kernel.beginTransaction( EXPLICIT, AUTH_DISABLED );
              var nodeCursor = tx.cursors().allocateNodeCursor( NULL_CONTEXT );
              var relCursor = tx.cursors().allocateRelationshipTraversalCursor( NULL_CONTEXT )
        )
        {
            Write write = tx.dataWrite();
            int rel = tx.tokenWrite().relationshipTypeGetOrCreateForName( "R" );
            //          (a1) → (b1)
            //        ↗ (a2) → (b2)
            //(start) → (a3) → (b3)
            //        ↘ (a4) → (b4)
            //          (a5) → (b5)
            long start = write.nodeCreate();
            //layer 1
            long a1 = write.nodeCreate();
            long a2 = write.nodeCreate();
            long a3 = write.nodeCreate();
            long a4 = write.nodeCreate();
            long a5 = write.nodeCreate();
            //layer 2
            long b1 = write.nodeCreate();
            long b2 = write.nodeCreate();
            long b3 = write.nodeCreate();
            long b4 = write.nodeCreate();
            long b5 = write.nodeCreate();

            //layer 1
            write.relationshipCreate( start, rel, a1 );
            write.relationshipCreate( start, rel, a2 );
            write.relationshipCreate( start, rel, a3 );
            var filterThis = write.relationshipCreate( start, rel, a4 );
            write.relationshipCreate( start, rel, a5 );
            //layer 2
            write.relationshipCreate( a1, rel, b1 );
            var andFilterThat = write.relationshipCreate( a2, rel, b2 );
            write.relationshipCreate( a3, rel, b3 );
            write.relationshipCreate( a4, rel, b4 );
            write.relationshipCreate( a5, rel, b5 );

            //when
            var expander =
                    outgoingExpander( start, 26, tx.dataRead(), nodeCursor, relCursor,
                                      LongPredicates.alwaysTrue(),
                                      cursor ->
                                              cursor.relationshipReference() != filterThis &&
                                              cursor.relationshipReference() != andFilterThat,
                                      EmptyMemoryTracker.INSTANCE );

            //then
            assertThat( asList( expander, BFSPruningVarExpandCursor::otherNodeReference ) ).isEqualTo( List.of( a1, a2, a3, a5, b1, b3, b5 ) );
        }
    }

    @Test
    void shouldOnlyTakeShorestPathBetweenNodes() throws KernelException
    {
        //given
        try ( var tx = kernel.beginTransaction( EXPLICIT, AUTH_DISABLED );
              var nodeCursor = tx.cursors().allocateNodeCursor( NULL_CONTEXT );
              var relCursor = tx.cursors().allocateRelationshipTraversalCursor( NULL_CONTEXT )
        )
        {
            Write write = tx.dataWrite();
            int rel = tx.tokenWrite().relationshipTypeGetOrCreateForName( "R" );
            //        ↗ (b1) → (b2) → (b3) ↘
            //(start) → (a1)  →  (a2)  →  (end)
            long start = write.nodeCreate();
            long end = write.nodeCreate();
            long a1 = write.nodeCreate();
            long a2 = write.nodeCreate();
            long b1 = write.nodeCreate();
            long b2 = write.nodeCreate();
            long b3 = write.nodeCreate();

            write.relationshipCreate( start, rel, a1 );
            write.relationshipCreate( a1, rel, a2 );
            write.relationshipCreate( a2, rel, end );
            write.relationshipCreate( start, rel, b1 );
            write.relationshipCreate( b1, rel, b2 );
            write.relationshipCreate( b2, rel, b3 );
            long shouldNotCross = write.relationshipCreate( b3, rel, end );

            //when
            var expander =
                    outgoingExpander( start, 26, tx.dataRead(), nodeCursor, relCursor, EmptyMemoryTracker.INSTANCE );

            //then
            var builder = new GraphBuilder();
            foreach( expander, cursor ->
            {
                builder.addNode( cursor.otherNodeReference() );
                builder.addRelationship( cursor.relationshipReference() );
            } );
            var traversed = builder.build();
            assertThat( traversed.nodes ).isEqualTo( List.of( a1, b1, a2, b2, end,  b3 ) );
            assertThat( traversed.relationships ).doesNotContain( shouldNotCross );
        }
    }

    @Test
    void shouldExpandOutgoing() throws KernelException
    {
        //given
        var graph = circleGraph( 10 );
        try ( var tx = kernel.beginTransaction( EXPLICIT, AUTH_DISABLED );
              var nodeCursor = tx.cursors().allocateNodeCursor( NULL_CONTEXT );
              var relCursor = tx.cursors().allocateRelationshipTraversalCursor( NULL_CONTEXT )
        )
        {
            //when
            var expander =
                    outgoingExpander( graph.startNode(), 3, tx.dataRead(), nodeCursor, relCursor, EmptyMemoryTracker.INSTANCE );

            //then
            assertThat( asList( expander, BFSPruningVarExpandCursor::relationshipReference ) ).hasSameElementsAs( graph.relationships.subList( 0, 3 ) );
        }
    }

    @Test
    void shouldExpandIncoming() throws KernelException
    {
        //given
        var graph = circleGraph( 10 );
        try ( var tx = kernel.beginTransaction( EXPLICIT, AUTH_DISABLED );
              var nodeCursor = tx.cursors().allocateNodeCursor( NULL_CONTEXT );
              var relCursor = tx.cursors().allocateRelationshipTraversalCursor( NULL_CONTEXT )
        )
        {
            //when
            var expander = incomingExpander( graph.startNode(), 3, tx.dataRead(), nodeCursor, relCursor, EmptyMemoryTracker.INSTANCE );

            //then
            assertThat( asList( expander, BFSPruningVarExpandCursor::relationshipReference ) ).hasSameElementsAs( graph.relationships.subList( 7, 10 ) );
        }
    }

    @Test
    void shouldRespectTypesOutgoing() throws KernelException
    {
        //given
        try ( var tx = kernel.beginTransaction( EXPLICIT, AUTH_DISABLED );
              var nodeCursor = tx.cursors().allocateNodeCursor( NULL_CONTEXT );
              var relCursor = tx.cursors().allocateRelationshipTraversalCursor( NULL_CONTEXT )
        )
        {
            //given
            Write write = tx.dataWrite();
            TokenWrite tokenWrite = tx.tokenWrite();
            int type1 = tokenWrite.relationshipTypeGetOrCreateForName( "R1" );
            int type2 = tokenWrite.relationshipTypeGetOrCreateForName( "R2" );
            long start = write.nodeCreate();
            long end = write.nodeCreate();
            write.relationshipCreate( start, type1, end );
            write.relationshipCreate( start, type2, write.nodeCreate() );

            //when
            var expander =
                    outgoingExpander( start, new int[]{type1}, 3, tx.dataRead(), nodeCursor, relCursor, EmptyMemoryTracker.INSTANCE );

            //then
            assertThat( asList( expander, BFSPruningVarExpandCursor::otherNodeReference ) ).isEqualTo( List.of( end ) );
        }
    }

    @Test
    void shouldRespectTypesIncoming() throws KernelException
    {
        //given
        try ( var tx = kernel.beginTransaction( EXPLICIT, AUTH_DISABLED );
              var nodeCursor = tx.cursors().allocateNodeCursor( NULL_CONTEXT );
              var relCursor = tx.cursors().allocateRelationshipTraversalCursor( NULL_CONTEXT )
        )
        {
            //given
            Write write = tx.dataWrite();
            TokenWrite tokenWrite = tx.tokenWrite();
            int type1 = tokenWrite.relationshipTypeGetOrCreateForName( "R1" );
            int type2 = tokenWrite.relationshipTypeGetOrCreateForName( "R2" );
            long start = write.nodeCreate();
            long end = write.nodeCreate();
            write.relationshipCreate( end, type1, start );
            write.relationshipCreate( write.nodeCreate(), type2, start );

            //when
            var expander =
                    incomingExpander( start, new int[]{type1}, 3, tx.dataRead(), nodeCursor, relCursor, EmptyMemoryTracker.INSTANCE );

            //then
            assertThat( asList( expander, BFSPruningVarExpandCursor::otherNodeReference ) ).isEqualTo( List.of( end ) );
        }
    }

    @Test
    void shouldExpandWithLength0() throws KernelException
    {
        //given
        var graph = circleGraph( 10 );
        try ( var tx = kernel.beginTransaction( EXPLICIT, AUTH_DISABLED );
              var nodeCursor = tx.cursors().allocateNodeCursor( NULL_CONTEXT );
              var relCursor = tx.cursors().allocateRelationshipTraversalCursor( NULL_CONTEXT )
        )
        {
            //when
            var expander =
                    outgoingExpander( graph.startNode(), 0, tx.dataRead(), nodeCursor, relCursor, EmptyMemoryTracker.INSTANCE );

            //then
            assertThat( asList( expander, BFSPruningVarExpandCursor::relationshipReference ) ).isEmpty();
        }
    }

    @Test
    void endNodesAreUnique() throws KernelException
    {
        //given
        try ( var tx = kernel.beginTransaction( EXPLICIT, AUTH_DISABLED );
              var nodeCursor = tx.cursors().allocateNodeCursor( NULL_CONTEXT );
              var relCursor = tx.cursors().allocateRelationshipTraversalCursor( NULL_CONTEXT )
        )
        {
            //given
            Write write = tx.dataWrite();
            TokenWrite tokenWrite = tx.tokenWrite();
            int type = tokenWrite.relationshipTypeGetOrCreateForName( "R1" );
            long start = write.nodeCreate();
            long middleNode = write.nodeCreate();
            long end = write.nodeCreate();
            write.relationshipCreate( start, type, end );
            write.relationshipCreate( start, type, middleNode );
            write.relationshipCreate( middleNode, type, end );

            //when
            var expander =
                    outgoingExpander( start, new int[]{type}, 3, tx.dataRead(), nodeCursor, relCursor, EmptyMemoryTracker.INSTANCE );

            //then
            assertThat( asList( expander, BFSPruningVarExpandCursor::otherNodeReference ) ).hasSameElementsAs( List.of( middleNode, end ) );
        }
    }

    @Test
    void shouldTraverseFullGraph() throws KernelException
    {
        //given
        var graph = fanOutGraph( 3, 3 );
        try ( var tx = kernel.beginTransaction( EXPLICIT, AUTH_DISABLED );
              var nodeCursor = tx.cursors().allocateNodeCursor( NULL_CONTEXT );
              var relCursor = tx.cursors().allocateRelationshipTraversalCursor( NULL_CONTEXT )
        )
        {
            //when
            var expander =
                    outgoingExpander( graph.startNode(), 3, tx.dataRead(), nodeCursor, relCursor, EmptyMemoryTracker.INSTANCE );

            //then
            assertThat( asList( expander, BFSPruningVarExpandCursor::relationshipReference ) ).hasSameElementsAs( graph.relationships );
        }
    }

    @Test
    void shouldStopAtSpecifiedDepth() throws KernelException
    {
        //given
        var graph = fanOutGraph( 2, 5 );
        try ( var tx = kernel.beginTransaction( EXPLICIT, AUTH_DISABLED );
              var nodeCursor = tx.cursors().allocateNodeCursor( NULL_CONTEXT );
              var relCursor = tx.cursors().allocateRelationshipTraversalCursor( NULL_CONTEXT )
        )
        {
            //when
            var expander =
                    outgoingExpander( graph.startNode(), 3, tx.dataRead(), nodeCursor, relCursor, EmptyMemoryTracker.INSTANCE );

            //then
            assertThat( asList( expander, BFSPruningVarExpandCursor::relationshipReference ) )
                    .hasSameElementsAs( graph.relationships.subList( 0, 14 ) );
        }
    }

    @Test
    void shouldSatisfyPredicateOnNodes() throws KernelException
    {
        //given
        var graph = circleGraph( 100 );
        try ( var tx = kernel.beginTransaction( EXPLICIT, AUTH_DISABLED );
              var nodeCursor = tx.cursors().allocateNodeCursor( NULL_CONTEXT );
              var relCursor = tx.cursors().allocateRelationshipTraversalCursor( NULL_CONTEXT )
        )
        {
            //when
            var expander =
                    outgoingExpander( graph.startNode(), 11, tx.dataRead(), nodeCursor, relCursor,
                                      value -> value <= graph.nodes.get( 5 ),
                                      Predicates.alwaysTrue(),
                                      EmptyMemoryTracker.INSTANCE );

            //then
            assertThat( asList( expander, BFSPruningVarExpandCursor::relationshipReference ) )
                    .hasSameElementsAs( graph.relationships.subList( 0, 5 ) );
        }
    }

    @Test
    void shouldSatisfyPredicateOnRelationships() throws KernelException
    {
        //given
        var graph = circleGraph( 100 );
        try ( var tx = kernel.beginTransaction( EXPLICIT, AUTH_DISABLED );
              var nodeCursor = tx.cursors().allocateNodeCursor( NULL_CONTEXT );
              var relCursor = tx.cursors().allocateRelationshipTraversalCursor( NULL_CONTEXT )
        )
        {
            //when
            var expander =
                    outgoingExpander( graph.startNode(), 11, tx.dataRead(), nodeCursor, relCursor,
                                      LongPredicates.alwaysTrue(),
                                      cursor -> cursor.relationshipReference() < graph.relationships.get( 9 ),
                                      EmptyMemoryTracker.INSTANCE );

            //then
            assertThat( asList( expander, BFSPruningVarExpandCursor::relationshipReference ) )
                    .hasSameElementsAs( graph.relationships.subList( 0, 9 ) );
        }
    }

    @Test
    void shouldHandleSimpleLoopOutgoing() throws KernelException
    {
        //given
        try ( var tx = kernel.beginTransaction( EXPLICIT, AUTH_DISABLED );
              var nodeCursor = tx.cursors().allocateNodeCursor( NULL_CONTEXT );
              var relCursor = tx.cursors().allocateRelationshipTraversalCursor( NULL_CONTEXT )
        )
        {
            Write write = tx.dataWrite();
            int rel = tx.tokenWrite().relationshipTypeGetOrCreateForName( "R" );
            long node1 = write.nodeCreate();
            long node2 = write.nodeCreate();
            write.relationshipCreate( node1, rel, node2 );
            write.relationshipCreate( node2, rel, node1 );

            //when
            var expander =
                    outgoingExpander( node1, 2, tx.dataRead(), nodeCursor, relCursor, EmptyMemoryTracker.INSTANCE );

            //then
            assertThat( asList( expander, BFSPruningVarExpandCursor::otherNodeReference ) ).hasSameElementsAs( List.of( node2, node1 ) );
        }
    }

    private Graph fanOutGraph( int relPerNode, int depth ) throws KernelException
    {
        long start = -1;
        ArrayList<Long> nodes = new ArrayList<>();
        ArrayList<Long> relationships = new ArrayList<>();
        try ( KernelTransaction tx = kernel.beginTransaction( EXPLICIT, AUTH_DISABLED ) )
        {
            var write = tx.dataWrite();
            var tokenWrite = tx.tokenWrite();
            var type = tokenWrite.relationshipTypeGetOrCreateForName( "R" );
            int totalNumberOfNodes = (int) ((1L - Math.round( Math.pow( relPerNode, depth + 1 ) )) / (1L - relPerNode));
            start = write.nodeCreate();
            nodes.add( start );
            int nodeCount = 1;
            Queue<Long> queue = new LinkedList<>();
            queue.offer( start );
            while ( !queue.isEmpty() )
            {
                var startNode = queue.poll();
                for ( int i = 0; i < relPerNode; i++ )
                {
                    var next = write.nodeCreate();
                    nodes.add( next );
                    queue.offer( next );
                    relationships.add( write.relationshipCreate( startNode, type, next ) );
                }
                nodeCount += relPerNode;

                if ( nodeCount >= totalNumberOfNodes )
                {
                    break;
                }
            }
            tx.commit();
        }

        return new Graph( nodes, relationships );
    }

    private Graph circleGraph( int numberOfNodes ) throws KernelException
    {
        assert numberOfNodes >= 2;
        long start = -1;
        ArrayList<Long> nodes = new ArrayList<>();
        ArrayList<Long> relationships = new ArrayList<>();
        try ( KernelTransaction tx = kernel.beginTransaction( EXPLICIT, AUTH_DISABLED ) )
        {
            var write = tx.dataWrite();
            var tokenWrite = tx.tokenWrite();
            var type = tokenWrite.relationshipTypeGetOrCreateForName( "R" );
            start = write.nodeCreate();
            nodes.add( start );
            long begin = start;
            long end = -1;
            for ( int i = 1; i < numberOfNodes; i++ )
            {
                end = write.nodeCreate();
                nodes.add( end );
                relationships.add( write.relationshipCreate( begin, type, end ) );
                begin = end;
            }
            relationships.add( write.relationshipCreate( end, type, start ) );
            tx.commit();
        }

        return new Graph( nodes, relationships );
    }

    private record Graph(ArrayList<Long> nodes, ArrayList<Long> relationships)
    {
        public long startNode()
        {
            return nodes.get( 0 );
        }
    }

    private static class GraphBuilder
    {
        private final ArrayList<Long> nodes = new ArrayList<>();
        private final ArrayList<Long> relationships = new ArrayList<>();

        void addNode( long node )
        {
            nodes.add( node );
        }

        void addRelationship( long relationship )
        {
            relationships.add( relationship );
        }

        Graph build()
        {
            return new Graph( nodes, relationships );
        }
    }

    <T> List<T> asList( BFSPruningVarExpandCursor expander, Function<BFSPruningVarExpandCursor,T> map )
    {
        ArrayList<T> found = new ArrayList<>();
        while ( expander.next() )
        {
            found.add( map.apply( expander ) );
        }
        return found;
    }

    void foreach( BFSPruningVarExpandCursor expander, Consumer<BFSPruningVarExpandCursor> consumer )
    {
        while ( expander.next() )
        {
           consumer.accept( expander );
        }
    }
}
