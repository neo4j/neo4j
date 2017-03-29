/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.store;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;
import java.util.function.LongFunction;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.cursor.Cursor;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.kernel.impl.locking.Lock;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.util.IoPrimitiveUtils;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.txstate.NodeState;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.test.rule.RepeatRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.collection.primitive.PrimitiveIntCollections.asArray;
import static org.neo4j.collection.primitive.PrimitiveIntCollections.asSet;
import static org.neo4j.kernel.impl.api.store.NodeCursorTest.Mode.APPEND;
import static org.neo4j.kernel.impl.api.store.NodeCursorTest.Mode.FETCH;
import static org.neo4j.kernel.impl.locking.LockService.NO_LOCK_SERVICE;
import static org.neo4j.kernel.impl.store.record.RecordLoad.CHECK;
import static org.neo4j.kernel.impl.transaction.state.NodeLabelsFieldTest.inlinedLabelsLongRepresentation;

@RunWith( Theories.class )
public class NodeCursorTest
{
    enum Mode
    {
        APPEND,
        FETCH
    }

    /*
     * each test is gonna run twice to make sure we reuse cursor correctly
     */
    @Rule
    public RepeatRule repeatRule = new RepeatRule();

    private static final int NON_EXISTING_LABEL = Integer.MAX_VALUE;

    private final NodeRecord nodeRecord = new NodeRecord( -1 );
    private final NodeStore nodeStore = mock( NodeStore.class );
    private final PageCursor pageCursor = mock( PageCursor.class );
    {
        when( nodeStore.newPageCursor() ).thenReturn( pageCursor );
        when( nodeStore.newRecord() ).thenReturn( nodeRecord );
    }

    // cursor is shared since it is designed to be reusable
    private final NodeCursor reusableCursor = new NodeCursor( nodeStore, i -> {}, NO_LOCK_SERVICE );

    @SuppressWarnings( "unchecked" )
    @DataPoints
    public static LongFunction<Operation>[] data()
    {
        return new LongFunction[]{NodeOnDisk::new, AddedNode::new, DeletedNode::new, ModifiedNode::new};
    }

    @Theory
    @RepeatRule.Repeat( times = 2 )
    public void nothingInAppendMode() throws Throwable
    {
        // given
        TestRun test = new TestRun( APPEND, new Operation[0] );
        Cursor<NodeItem> cursor = test.initialize( reusableCursor, nodeStore, pageCursor, nodeRecord );

        // when/then
        test.runAndVerify( cursor );
    }

    @Theory
    @RepeatRule.Repeat( times = 2 )
    public void nothingInFetchMode() throws Throwable
    {
        // given
        TestRun test = new TestRun( FETCH, new Operation[0] );
        Cursor<NodeItem> cursor = test.initialize( reusableCursor, nodeStore, pageCursor, nodeRecord );

        // when/then
        test.runAndVerify( cursor );
    }

    @Theory
    @RepeatRule.Repeat( times = 2 )
    public void anElementInAppendMode( LongFunction<Operation> op0 ) throws Throwable
    {
        // given
        TestRun test = new TestRun( APPEND, new Operation[]{op0.apply( 0 )} );
        Cursor<NodeItem> cursor = test.initialize( reusableCursor, nodeStore, pageCursor, nodeRecord );

        // when/then
        test.runAndVerify( cursor );
    }

    @Theory
    @RepeatRule.Repeat( times = 2 )
    public void anElementInFetchMode( LongFunction<Operation> op0 ) throws Throwable
    {
        // given
        TestRun test = new TestRun( FETCH, new Operation[]{op0.apply( 0 )} );
        Cursor<NodeItem> cursor = test.initialize( reusableCursor, nodeStore, pageCursor, nodeRecord );

        // when/then
        test.runAndVerify( cursor );
    }

    @Theory
    @RepeatRule.Repeat( times = 2 )
    public void twoElementsInAppendMode( LongFunction<Operation> op0, LongFunction<Operation> op1 ) throws Throwable
    {
        // given
        TestRun test = new TestRun( APPEND, new Operation[]{op0.apply( 0 ), op1.apply( 1 )} );
        Cursor<NodeItem> cursor = test.initialize( reusableCursor, nodeStore, pageCursor, nodeRecord );

        // when/then
        test.runAndVerify( cursor );
    }

    @Theory
    @RepeatRule.Repeat( times = 2 )
    public void twoElementsInFetchMode( LongFunction<Operation> op0, LongFunction<Operation> op1 ) throws Throwable
    {
        // given
        TestRun test = new TestRun( FETCH, new Operation[]{op0.apply( 0 ), op1.apply( 1 )} );
        Cursor<NodeItem> cursor = test.initialize( reusableCursor, nodeStore, pageCursor, nodeRecord );

        // when/then
        test.runAndVerify( cursor );
    }

    @Theory
    @RepeatRule.Repeat( times = 2 )
    public void threeElementsInAppendMode( LongFunction<Operation> op0, LongFunction<Operation> op1,
            LongFunction<Operation> op2 ) throws Throwable
    {
        // given
        TestRun test = new TestRun( APPEND, new Operation[]{op0.apply( 0 ), op1.apply( 1 ), op2.apply( 2 )} );
        Cursor<NodeItem> cursor = test.initialize( reusableCursor, nodeStore, pageCursor, nodeRecord );

        // when/then
        test.runAndVerify( cursor );
    }

    @Theory
    @RepeatRule.Repeat( times = 2 )
    public void threeElementsInFetchMode( LongFunction<Operation> op0, LongFunction<Operation> op1,
            LongFunction<Operation> op2 ) throws Throwable
    {
        // given
        TestRun test = new TestRun( FETCH, new Operation[]{op0.apply( 0 ), op1.apply( 1 ), op2.apply( 2 )} );
        Cursor<NodeItem> cursor = test.initialize( reusableCursor, nodeStore, pageCursor, nodeRecord );

        // when/then
        test.runAndVerify( cursor );
    }

    @Theory
    @RepeatRule.Repeat( times = 2 )
    public void fourElementsInAppendMode( LongFunction<Operation> op0, LongFunction<Operation> op1,
            LongFunction<Operation> op2, LongFunction<Operation> op3 ) throws Throwable
    {
        // given
        TestRun test =
                new TestRun( APPEND, new Operation[]{op0.apply( 0 ), op1.apply( 1 ), op2.apply( 2 ), op3.apply( 3 )} );
        Cursor<NodeItem> cursor = test.initialize( reusableCursor, nodeStore, pageCursor, nodeRecord );

        // when/then
        test.runAndVerify( cursor );
    }

    @Theory
    @RepeatRule.Repeat( times = 2 )
    public void fourElementsInFetchMode( LongFunction<Operation> op0, LongFunction<Operation> op1,
            LongFunction<Operation> op2, LongFunction<Operation> op3 ) throws Throwable
    {
        // given
        TestRun test =
                new TestRun( FETCH, new Operation[]{op0.apply( 0 ), op1.apply( 1 ), op2.apply( 2 ), op3.apply( 3 )} );
        Cursor<NodeItem> cursor = test.initialize( reusableCursor, nodeStore, pageCursor, nodeRecord );

        // when/then
        test.runAndVerify( cursor );
    }

    @Test
    public void shouldCallTheConsumerOnClose()
    {
        MutableBoolean called = new MutableBoolean();
        NodeCursor cursor = new NodeCursor( nodeStore, c -> called.setTrue(), NO_LOCK_SERVICE );
        cursor.init( mock( NodeProgression.class ) );
        assertFalse( called.booleanValue() );

        cursor.close();
        assertTrue( called.booleanValue() );
    }

    @Test
    public void shouldCloseThePageCursorWhenDisposed()
    {
        NodeCursor cursor = new NodeCursor( nodeStore, c -> {}, NO_LOCK_SERVICE );
        cursor.init( mock( NodeProgression.class ) );

        cursor.close();
        cursor.dispose();
        verify( pageCursor ).close();
    }

    private static class ModifiedNode extends NodeOnDisk
    {
        private ModifiedNode( long id )
        {
            super( id );
        }

        @Override
        public TxState prepare( NodeStore nodeStore, PageCursor pageCursor, NodeRecord nodeRecord, TxState state )
        {
            state = state == null ? new TxState() : state;
            state.nodeDoAddLabel( 6 + (int) id, id );
            state.nodeDoRemoveLabel( 5 + (int) id, id );
            return super.prepare( nodeStore, pageCursor, nodeRecord, state );
        }

        @Override
        public String toString()
        {
            return String.format( "ModifiedNode[%d]", id );
        }
    }

    private static class AddedNode implements Operation
    {
        private final long id;

        private TxState state;

        private AddedNode( long id )
        {
            this.id = id;
        }

        @Override
        public long id()
        {
            return id;
        }

        @Override
        public TxState prepare( NodeStore nodeStore, PageCursor pageCursor, NodeRecord nodeRecord, TxState state )
        {
            this.state = state = state == null ? new TxState() : state;
            state.nodeDoCreate( id );
            state.nodeDoAddLabel( 20 + (int) id, id );
            return state;
        }

        @Override
        public boolean fromDisk()
        {
            return false;
        }

        @Override
        public void check( NodeItem node )
        {
            Set<Long> added = state.addedAndRemovedNodes().getAdded();
            assertTrue( added.contains( node.id() ) );
            state.getNodeState( node.id() ).labelDiffSets().getAdded();
            PrimitiveIntSet expectedLabels =
                    asSet( asArray( state.getNodeState( node.id() ).labelDiffSets().getAdded() ) );
            assertEquals( expectedLabels, node.labels() );
            assertEquals( StatementConstants.NO_SUCH_PROPERTY, node.nextPropertyId() );
            assertFalse( node.isDense() );
            assertEquals( StatementConstants.NO_SUCH_RELATIONSHIP, node.nextRelationshipId() );
            PrimitiveIntIterator iterator = expectedLabels.iterator();
            while ( iterator.hasNext() )
            {
                assertTrue( node.hasLabel( iterator.next() ) );
                assertFalse( node.hasLabel( NON_EXISTING_LABEL ) );
            }
        }

        @Override
        public String toString()
        {
            return String.format( "AddedNode[%d]", id );
        }
    }

    private static class DeletedNode implements Operation
    {
        private final long id;

        private DeletedNode( long id )
        {
            this.id = id;
        }

        @Override
        public long id()
        {
            return id;
        }

        @Override
        public TxState prepare( NodeStore nodeStore, PageCursor pageCursor, NodeRecord nodeRecord, TxState state )
        {
            state = state == null ? new TxState() : state;
            state.nodeDoDelete( id );
            record( id, nodeStore, pageCursor, nodeRecord, state );
            return state;
        }

        @Override
        public boolean fromDisk()
        {
            return true;
        }

        @Override
        public void check( NodeItem nodeItem )
        {
            throw new UnsupportedOperationException( "no check" );
        }

        @Override
        public String toString()
        {
            return String.format( "DeletedNode[%d]", id );
        }
    }

    private static class NodeOnDisk implements Operation
    {
        protected final long id;
        private NodeItem expected;

        private NodeOnDisk( long id )
        {
            this.id = id;
        }

        @Override
        public long id()
        {
            return id;
        }

        @Override
        public TxState prepare( NodeStore nodeStore, PageCursor pageCursor, NodeRecord nodeRecord, TxState state )
        {
            expected = record( id, nodeStore, pageCursor, nodeRecord, state );
            return state;
        }

        @Override
        public boolean fromDisk()
        {
            return true;
        }

        @Override
        public void check( NodeItem nodeItem )
        {
            assertEquals( expected.id(), nodeItem.id() );
            assertEquals( expected.nextPropertyId(), nodeItem.nextPropertyId() );
            assertEquals( expected.isDense(), nodeItem.isDense() );
            assertEquals( expected.labels(), nodeItem.labels() );
            if ( nodeItem.isDense() )
            {
                assertEquals( expected.nextGroupId(), nodeItem.nextGroupId() );
            }
            else
            {
                assertEquals( expected.nextRelationshipId(), nodeItem.nextRelationshipId() );
            }

            PrimitiveIntIterator iterator = expected.labels().iterator();
            while ( iterator.hasNext() )
            {
                assertTrue( nodeItem.hasLabel( iterator.next() ) );
                assertFalse( nodeItem.hasLabel( NON_EXISTING_LABEL ) );
            }
        }

        @Override
        public String toString()
        {
            return String.format( "NodeOnDisk[%d]", id );
        }
    }

    private static NodeItem record( long id, NodeStore nodeStore, PageCursor pageCursor, NodeRecord nodeRecord,
            ReadableTransactionState state )
    {
        boolean dense = id % 2 == 0;
        int nextProp = 42 + (int) id;
        long nextRel = 43 + id;
        long[] labelIds = new long[]{4 + id, 5 + id};
        when( nodeStore.readRecord( id, nodeRecord, CHECK, pageCursor ) ).thenAnswer( invocationOnMock ->
        {
            nodeRecord.setId( id );
            nodeRecord.initialize( true, nextProp, dense, nextRel, inlinedLabelsLongRepresentation( labelIds ) );
            return nodeRecord;
        } );

        return new NodeItem()
        {
            @Override
            public long id()
            {
                return id;
            }

            @Override
            public PrimitiveIntSet labels()
            {
                PrimitiveIntSet labels = asSet( labelIds, IoPrimitiveUtils::safeCastLongToInt );
                return state == null ? labels : state.getNodeState( id ).augmentLabels( labels );
            }

            @Override
            public boolean isDense()
            {
                return dense;
            }

            @Override
            public boolean hasLabel( int labelId )
            {
                throw new UnsupportedOperationException( "don't call this" );
            }

            @Override
            public long nextGroupId()
            {
                assert isDense();
                return nextRelationshipId();
            }

            @Override
            public long nextRelationshipId()
            {
                return nextRel;
            }

            @Override
            public long nextPropertyId()
            {
                return nextProp;
            }

            @Override
            public Lock lock()
            {
                throw new UnsupportedOperationException( "don't call this" );
            }
        };
    }

    private interface Operation
    {
        long id();

        TxState prepare( NodeStore nodeStore, PageCursor pageCursor, NodeRecord nodeRecord, TxState state );

        boolean fromDisk();

        void check( NodeItem nodeItem );
    }

    private static class TestRun
    {
        private final Mode mode;
        private final Operation[] ops;

        private TxState state = null;

        private TestRun( Mode mode, Operation[] ops )
        {
            this.mode = mode;
            this.ops = ops;
        }

        Cursor<NodeItem> initialize( NodeCursor cursor, NodeStore nodeStore, PageCursor pageCursor,
                NodeRecord nodeRecord )
        {
            for ( Operation op : ops )
            {
                state = op.prepare( nodeStore, pageCursor, nodeRecord, state );
            }
            return cursor.init( createProgression( ops, mode, state ) );
        }

        private NodeProgression createProgression( Operation[] ops, Mode mode, TxState state )
        {
            return new NodeProgression()
            {
                private int i = 0;

                @Override
                public boolean nextBatch( Batch batch )
                {
                    while ( i < ops.length )
                    {
                        Operation op = ops[i++];
                        if ( op.fromDisk() || mode == FETCH )
                        {
                            batch.init( op.id(), op.id() );
                            return true;
                        }
                    }
                    batch.nothing();
                    return false;
                }

                @Override
                public Iterator<Long> addedNodes()
                {
                    return mode == APPEND && state != null ? state.addedAndRemovedNodes().getAdded().iterator() : null;
                }

                @Override
                public boolean fetchFromTxState( long id )
                {
                    return mode == FETCH && state != null && state.nodeIsAddedInThisTx( id );
                }

                @Override
                public boolean fetchFromDisk( long id )
                {
                    return state == null || !state.nodeIsDeletedInThisTx( id );
                }

                @Override
                public NodeState nodeState( long id )
                {
                    return state == null ? NodeState.EMPTY : state.getNodeState( id );
                }
            };
        }

        void runAndVerify( Cursor<NodeItem> cursor )
        {
            Operation[] operations = Arrays.copyOf( ops, ops.length );
            if ( mode == APPEND )
            {
                // push the AddedNodes at the end of the array during the check phase since if added in the cursor they
                // are at the end of the stream
                Operation tmp;
                for ( int i = operations.length - 1; i >= 0; i-- )
                {
                    if ( operations[i] instanceof AddedNode )
                    {
                        for ( int j = i; j < operations.length - 1 && !(operations[j + 1] instanceof AddedNode); j++ )
                        {
                            tmp = operations[j];
                            operations[j] = operations[j + 1];
                            operations[j + 1] = tmp;
                        }
                    }
                }
            }

            state = state == null ? new TxState() : state;

            int injectedId = 100_000;
            for ( Operation op : operations )
            {
                // simulates tx changes after the cursor has been initialized, the cursor should no return this ids!
                state.nodeDoCreate( injectedId++ );
                if ( !(op instanceof DeletedNode) )
                {
                    assertTrue( cursor.next() );
                    op.check( cursor.get() );
                }
            }

            assertNoMoreElements( cursor );
            cursor.close();
            assertNoMoreElements( cursor );
        }

        private static void assertNoMoreElements( Cursor<NodeItem> cursor )
        {
            assertFalse( cursor.next() );
            try
            {
                cursor.get();
                fail( "should have thrown" );
            }
            catch ( IllegalStateException ex )
            {
                // good
            }
        }

        @Override
        public String toString()
        {
            return String.format( "{mode=%s, ops=%s}", mode, Arrays.toString( ops ) );
        }
    }
}
