/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.index.gbptree;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.io.pagecache.PageCursor;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.index.gbptree.GenSafePointerPair.GEN_COMPARISON_MASK;
import static org.neo4j.index.gbptree.GenSafePointerPair.NO_LOGICAL_POS;
import static org.neo4j.index.gbptree.GenSafePointerPair.FLAG_READ;
import static org.neo4j.index.gbptree.GenSafePointerPair.READ_OR_WRITE_MASK;
import static org.neo4j.index.gbptree.GenSafePointerPair.SHIFT_STATE_A;
import static org.neo4j.index.gbptree.GenSafePointerPair.SHIFT_STATE_B;
import static org.neo4j.index.gbptree.GenSafePointerPair.FLAG_WRITE;
import static org.neo4j.index.gbptree.GenSafePointerPair.failureDescription;
import static org.neo4j.index.gbptree.GenSafePointerPair.isRead;
import static org.neo4j.index.gbptree.GenSafePointerPair.pointerStateFromResult;
import static org.neo4j.index.gbptree.GenSafePointerPair.pointerStateName;

@RunWith( Parameterized.class )
public class GenSafePointerPairTest
{
    private static final int PAGE_SIZE = 128;
    private static final int OLD_STABLE_GENERATION = 1;
    private static final int STABLE_GENERATION = 2;
    private static final int OLD_CRASH_GENERATION = 3;
    private static final int CRASH_GENERATION = 4;
    private static final int UNSTABLE_GENERATION = 5;
    private static final long EMPTY_POINTER = 0L;

    private static final long POINTER_A = 5;
    private static final long POINTER_B = 6;
    private static final long WRITTEN_POINTER = 10;

    private static final int EXPECTED_GEN_DISREGARD = -2;
    private static final int EXPECTED_GEN_B_BIG = -1;
    private static final int EXPECTED_GEN_EQUAL = 0;
    private static final int EXPECTED_GEN_A_BIG = 1;

    private static final boolean SLOT_A = true;
    private static final boolean SLOT_B = false;
    private static final int GSPP_OFFSET = 5;
    private static final int SLOT_A_OFFSET = GSPP_OFFSET;
    private static final int SLOT_B_OFFSET = SLOT_A_OFFSET + GenSafePointer.SIZE;

    @Parameters( name = "{0},{1},read {2},write {3}" )
    public static Collection<Object[]> data()
    {
        Collection<Object[]> data = new ArrayList<>();

        //             ┌─────────────────┬─────────────────┬───────────────────┬───────────────────────┐
        //             │ State A         │ State B         │ Read outcome      │ Write outcome         │
        //             └─────────────────┴─────────────────┴───────────────────┴───────────────────────┘
        data.add( array( State.EMPTY,      State.EMPTY,      Fail.GEN_DISREGARD, Success.A ) );
        data.add( array( State.EMPTY,      State.UNSTABLE,   Success.B,          Success.B ) );
        data.add( array( State.EMPTY,      State.STABLE,     Success.B,          Success.A ) );
        data.add( array( State.EMPTY,      State.CRASH,      Fail.GEN_DISREGARD, Fail.GEN_DISREGARD ) );
        data.add( array( State.EMPTY,      State.BROKEN,     Fail.GEN_DISREGARD, Fail.GEN_DISREGARD ) );
        data.add( array( State.UNSTABLE,   State.EMPTY,      Success.A,          Success.A ) );
        data.add( array( State.UNSTABLE,   State.UNSTABLE,   Fail.GEN_EQUAL,     Fail.GEN_EQUAL ) );
        data.add( array( State.UNSTABLE,   State.STABLE,     Success.A,          Success.A ) );
        data.add( array( State.UNSTABLE,   State.CRASH,      Fail.GEN_A_BIG,     Fail.GEN_A_BIG ) );
        data.add( array( State.UNSTABLE,   State.BROKEN,     Fail.GEN_DISREGARD, Fail.GEN_DISREGARD ) );
        data.add( array( State.STABLE,     State.EMPTY,      Success.A,          Success.B ) );
        data.add( array( State.STABLE,     State.UNSTABLE,   Success.B,          Success.B ) );
        data.add( array( State.STABLE,     State.OLD_STABLE, Success.A,          Success.B ) );
        data.add( array( State.OLD_STABLE, State.STABLE,     Success.B,          Success.A ) );
        data.add( array( State.STABLE,     State.STABLE,     Fail.GEN_EQUAL,     Fail.GEN_EQUAL ) );
        data.add( array( State.STABLE,     State.CRASH,      Success.A,          Success.B ) );
        data.add( array( State.STABLE,     State.BROKEN,     Success.A,          Success.B ) );
        data.add( array( State.CRASH,      State.EMPTY,      Fail.GEN_DISREGARD, Fail.GEN_DISREGARD ) );
        data.add( array( State.CRASH,      State.UNSTABLE,   Fail.GEN_B_BIG,     Fail.GEN_B_BIG ) );
        data.add( array( State.CRASH,      State.STABLE,     Success.B,          Success.A ) );
        data.add( array( State.CRASH,      State.OLD_CRASH,  Fail.GEN_A_BIG,     Fail.GEN_A_BIG ) );
        data.add( array( State.OLD_CRASH,  State.CRASH,      Fail.GEN_B_BIG,     Fail.GEN_B_BIG ) );
        data.add( array( State.CRASH,      State.CRASH,      Fail.GEN_EQUAL,     Fail.GEN_EQUAL ) );
        data.add( array( State.CRASH,      State.BROKEN,     Fail.GEN_DISREGARD, Fail.GEN_DISREGARD ) );
        data.add( array( State.BROKEN,     State.EMPTY,      Fail.GEN_DISREGARD, Fail.GEN_DISREGARD ) );
        data.add( array( State.BROKEN,     State.UNSTABLE,   Fail.GEN_DISREGARD, Fail.GEN_DISREGARD ) );
        data.add( array( State.BROKEN,     State.STABLE,     Success.B,          Success.A ) );
        data.add( array( State.BROKEN,     State.CRASH,      Fail.GEN_DISREGARD, Fail.GEN_DISREGARD ) );
        data.add( array( State.BROKEN,     State.BROKEN,     Fail.GEN_DISREGARD, Fail.GEN_DISREGARD ) );

        return data;
    }

    @Parameter( 0 )
    public State stateA;
    @Parameter( 1 )
    public State stateB;
    @Parameter( 2 )
    public Slot expectedReadOutcome;
    @Parameter( 3 )
    public Slot expectedWriteOutcome;

    private final PageCursor cursor = ByteArrayPageCursor.wrap( new byte[PAGE_SIZE] );

    @Test
    public void shouldReadWithLogicalPosition() throws Exception
    {
        // GIVEN
        cursor.setOffset( SLOT_A_OFFSET );
        long preStatePointerA = stateA.materialize( cursor, POINTER_A );
        cursor.setOffset( SLOT_B_OFFSET );
        long preStatePointerB = stateB.materialize( cursor, POINTER_B );
        int pos = 1234;

        // WHEN
        cursor.setOffset( GSPP_OFFSET );
        long result = GenSafePointerPair.read( cursor, STABLE_GENERATION, UNSTABLE_GENERATION, pos );

        // THEN
        expectedReadOutcome.verifyRead( cursor, result, stateA, stateB, preStatePointerA, preStatePointerB, pos );
    }

    @Test
    public void shouldReadWithNoLogicalPosition() throws Exception
    {
        // GIVEN
        cursor.setOffset( SLOT_A_OFFSET );
        long preStatePointerA = stateA.materialize( cursor, POINTER_A );
        cursor.setOffset( SLOT_B_OFFSET );
        long preStatePointerB = stateB.materialize( cursor, POINTER_B );

        // WHEN
        cursor.setOffset( GSPP_OFFSET );
        long result = GenSafePointerPair.read( cursor, STABLE_GENERATION, UNSTABLE_GENERATION, NO_LOGICAL_POS );

        // THEN
        expectedReadOutcome.verifyRead( cursor, result, stateA, stateB, preStatePointerA, preStatePointerB,
                NO_LOGICAL_POS );
    }

    @Test
    public void shouldWrite() throws Exception
    {
        // GIVEN
        cursor.setOffset( SLOT_A_OFFSET );
        long preStatePointerA = stateA.materialize( cursor, POINTER_A );
        cursor.setOffset( SLOT_B_OFFSET );
        long preStatePointerB = stateB.materialize( cursor, POINTER_B );

        // WHEN
        cursor.setOffset( GSPP_OFFSET );
        long written = GenSafePointerPair.write( cursor, WRITTEN_POINTER, STABLE_GENERATION, UNSTABLE_GENERATION );

        // THEN
        expectedWriteOutcome.verifyWrite( cursor, written, stateA, stateB, preStatePointerA, preStatePointerB );
    }

    private static void assertFailure( long result, long readOrWrite, int genComparison,
            byte pointerStateA, byte pointerStateB )
    {
        assertFalse( GenSafePointerPair.isSuccess( result ) );

        // Raw failure bits
        assertEquals( readOrWrite, result & READ_OR_WRITE_MASK );
        if ( genComparison != EXPECTED_GEN_DISREGARD )
        {
            assertEquals( genComparisonBits( genComparison ), result & GEN_COMPARISON_MASK );
        }
        assertEquals( pointerStateA, pointerStateFromResult( result, SHIFT_STATE_A ) );
        assertEquals( pointerStateB, pointerStateFromResult( result, SHIFT_STATE_B ) );

        // Failure description
        String failureDescription = failureDescription( result );
        assertThat( failureDescription, containsString( isRead( result ) ? "READ" : "WRITE" ) );
        if ( genComparison != EXPECTED_GEN_DISREGARD )
        {
            assertThat( failureDescription, containsString( genComparisonName( genComparison ) ) );
        }
        assertThat( failureDescription, containsString( pointerStateName( pointerStateA ) ) );
        assertThat( failureDescription, containsString( pointerStateName( pointerStateB ) ) );
    }

    private static String genComparisonName( int genComparison )
    {
        switch ( genComparison )
        {
        case EXPECTED_GEN_B_BIG:
            return GenSafePointerPair.GEN_COMPARISON_NAME_B_BIG;
        case EXPECTED_GEN_EQUAL:
            return GenSafePointerPair.GEN_COMPARISON_NAME_EQUAL;
        case EXPECTED_GEN_A_BIG:
            return GenSafePointerPair.GEN_COMPARISON_NAME_A_BIG;
        default:
            throw new UnsupportedOperationException( String.valueOf( genComparison ) );
        }
    }

    private static long genComparisonBits( int genComparison )
    {
        switch ( genComparison )
        {
        case EXPECTED_GEN_B_BIG:
            return GenSafePointerPair.FLAG_GEN_B_BIG;
        case EXPECTED_GEN_EQUAL:
            return GenSafePointerPair.FLAG_GEN_EQUAL;
        case EXPECTED_GEN_A_BIG:
            return GenSafePointerPair.FLAG_GEN_A_BIG;
        default:
            throw new UnsupportedOperationException( String.valueOf( genComparison ) );
        }
    }

    private static long readSlotA( PageCursor cursor )
    {
        cursor.setOffset( SLOT_A_OFFSET );
        return readSlot( cursor );
    }

    private static long readSlotB( PageCursor cursor )
    {
        cursor.setOffset( SLOT_B_OFFSET );
        return readSlot( cursor );
    }

    private static long readSlot( PageCursor cursor )
    {
        long generation = GenSafePointer.readGeneration( cursor );
        long pointer = GenSafePointer.readPointer( cursor );
        short checksum = GenSafePointer.readChecksum( cursor );
        assertEquals( GenSafePointer.checksumOf( generation, pointer ), checksum );
        return pointer;
    }

    private static Object[] array( Object... array )
    {
        return array;
    }

    enum State
    {
        EMPTY( GenSafePointerPair.EMPTY )
        {
            @Override
            long materialize( PageCursor cursor, long pointer )
            {   // Nothing to write
                return EMPTY_POINTER;
            }
        },
        BROKEN( GenSafePointerPair.BROKEN )
        {
            @Override
            long materialize( PageCursor cursor, long pointer )
            {
                // write an arbitrary GSP
                int offset = cursor.getOffset();
                GenSafePointer.write( cursor, 10, 20 );

                // then break its checksum
                cursor.setOffset( offset + GenSafePointer.SIZE - GenSafePointer.CHECKSUM_SIZE );
                short checksum = GenSafePointer.readChecksum( cursor );
                cursor.setOffset( offset + GenSafePointer.SIZE - GenSafePointer.CHECKSUM_SIZE );
                cursor.putShort( (short) ~checksum );
                return pointer;
            }

            @Override
            void verify( PageCursor cursor, long expectedPointer, boolean slotA, int logicalPos )
            {
                cursor.setOffset( slotA ? SLOT_A_OFFSET : SLOT_B_OFFSET );

                long generation = GenSafePointer.readGeneration( cursor );
                long pointer = GenSafePointer.readPointer( cursor );
                short checksum = GenSafePointer.readChecksum( cursor );
                assertNotEquals( GenSafePointer.checksumOf( generation, pointer ), checksum );
            }
        },
        OLD_CRASH( GenSafePointerPair.CRASH )
        {
            @Override
            long materialize( PageCursor cursor, long pointer )
            {
                GenSafePointer.write( cursor, OLD_CRASH_GENERATION, pointer );
                return pointer;
            }
        },
        CRASH( GenSafePointerPair.CRASH )
        {
            @Override
            long materialize( PageCursor cursor, long pointer )
            {
                GenSafePointer.write( cursor, CRASH_GENERATION, pointer );
                return pointer;
            }
        },
        OLD_STABLE( GenSafePointerPair.STABLE )
        {
            @Override
            long materialize( PageCursor cursor, long pointer )
            {
                GenSafePointer.write( cursor, OLD_STABLE_GENERATION, pointer );
                return pointer;
            }
        },
        STABLE( GenSafePointerPair.STABLE )
        {
            @Override
            long materialize( PageCursor cursor, long pointer )
            {
                GenSafePointer.write( cursor, STABLE_GENERATION, pointer );
                return pointer;
            }
        },
        UNSTABLE( GenSafePointerPair.UNSTABLE )
        {
            @Override
            long materialize( PageCursor cursor, long pointer )
            {
                GenSafePointer.write( cursor, UNSTABLE_GENERATION, pointer );
                return pointer;
            }
        };

        /**
         * Actual {@link GenSafePointerPair} pointer state value.
         */
        private final byte byteValue;

        private State( byte byteValue )
        {
            this.byteValue = byteValue;
        }

        /**
         * Writes this state onto cursor.
         *
         * @param cursor {@link PageCursor} to write pre-state to.
         * @param pointer pointer to write in GSP. Generation is decided by the pre-state.
         * @return written pointer.
         */
        abstract long materialize( PageCursor cursor, long pointer );

        /**
         * Verifies result after WHEN section in test.
         *
         * @param cursor {@link PageCursor} to read actual pointer from.
         * @param expectedPointer expected pointer, as received from {@link #materialize(PageCursor, long)}.
         * @param slotA whether or not this is for slot A, otherwise B.
         */
        void verify( PageCursor cursor, long expectedPointer, boolean slotA, int logicalPos )
        {
            assertEquals( expectedPointer, slotA ? readSlotA( cursor ) : readSlotB( cursor ) );
        }
    }

    interface Slot
    {
        /**
         * @param cursor {@link PageCursor} to read actual result from.
         * @param result read-result from {@link GenSafePointerPair#read(PageCursor, long, long, int)}.
         * @param stateA state of pointer A when read.
         * @param stateB state of pointer B when read.
         * @param preStatePointerA pointer A as it looked like in pre-state.
         * @param preStatePointerB pointer B as it looked like in pre-state.
         * @param logicalPos expected logical pos.
         */
        void verifyRead( PageCursor cursor, long result, State stateA, State stateB,
                long preStatePointerA, long preStatePointerB, int logicalPos );

        /**
         * @param cursor {@link PageCursor} to read actual result from.
         * @param result write-result from {@link GenSafePointerPair#write(PageCursor, long, long, long)}.
         * @param stateA state of pointer A when written.
         * @param stateB state of pointer B when written.
         * @param preStatePointerA pointer A as it looked like in pre-state.
         * @param preStatePointerB pointer B as it looked like in pre-state.
         */
        void verifyWrite( PageCursor cursor, long result, State stateA, State stateB,
                long preStatePointerA, long preStatePointerB );
    }

    enum Success implements Slot
    {
        A( POINTER_A, SLOT_A ),
        B( POINTER_B, SLOT_B );

        private final long expectedPointer;
        private final boolean expectedSlot;

        private Success( long expectedPointer, boolean expectedSlot )
        {
            this.expectedPointer = expectedPointer;
            this.expectedSlot = expectedSlot;
        }

        @Override
        public void verifyRead( PageCursor cursor, long result, State stateA, State stateB,
                long preStatePointerA, long preStatePointerB, int logicalPos )
        {
            assertSuccess( result );
            long pointer = GenSafePointerPair.pointer( result );
            assertEquals( expectedPointer, pointer );
            assertEquals( expectedSlot == SLOT_A, GenSafePointerPair.resultIsFromSlotA( result ) );
            if ( logicalPos == NO_LOGICAL_POS )
            {
                assertFalse( GenSafePointerPair.isLogicalPos( result ) );
                assertEquals( GSPP_OFFSET, GenSafePointerPair.genOffset( result ) );
            }
            else
            {
                assertTrue( GenSafePointerPair.isLogicalPos( result ) );
                assertEquals( logicalPos, GenSafePointerPair.genOffset( result ) );
            }

            stateA.verify( cursor, preStatePointerA, SLOT_A, logicalPos );
            stateB.verify( cursor, preStatePointerB, SLOT_B, logicalPos );
        }

        @Override
        public void verifyWrite( PageCursor cursor, long result, State stateA, State stateB,
                long preStatePointerA, long preStatePointerB )
        {
            assertSuccess( result );
            boolean actuallyWrittenSlot =
                    (result & GenSafePointerPair.SLOT_MASK) == GenSafePointerPair.FLAG_SLOT_A ? SLOT_A : SLOT_B;
            assertEquals( expectedSlot, actuallyWrittenSlot );

            if ( expectedSlot == SLOT_A )
            {
                // Expect slot A to have been written, B staying the same
                assertEquals( WRITTEN_POINTER, readSlotA( cursor ) );
                assertEquals( preStatePointerB, readSlotB( cursor ) );
            }
            else
            {
                // Expect slot B to have been written, A staying the same
                assertEquals( preStatePointerA, readSlotA( cursor ) );
                assertEquals( WRITTEN_POINTER, readSlotB( cursor ) );
            }
        }

        private static void assertSuccess( long result )
        {
            assertTrue( GenSafePointerPair.isSuccess( result ) );
        }
    }

    enum Fail implements Slot
    {
        GEN_DISREGARD( EXPECTED_GEN_DISREGARD ),
        GEN_B_BIG( EXPECTED_GEN_B_BIG ),
        GEN_EQUAL( EXPECTED_GEN_EQUAL ),
        GEN_A_BIG( EXPECTED_GEN_A_BIG );

        private final int genComparison;

        private Fail( int genComparison )
        {
            this.genComparison = genComparison;
        }

        @Override
        public void verifyRead( PageCursor cursor, long result, State stateA, State stateB,
                long preStatePointerA, long preStatePointerB, int logicalPos )
        {
            assertFailure( result, FLAG_READ, genComparison, stateA.byteValue, stateB.byteValue );
            stateA.verify( cursor, preStatePointerA, SLOT_A, logicalPos );
            stateB.verify( cursor, preStatePointerB, SLOT_B, logicalPos );
        }

        @Override
        public void verifyWrite( PageCursor cursor, long result, State stateA, State stateB,
                long preStatePointerA, long preStatePointerB )
        {
            assertFailure( result, FLAG_WRITE, genComparison, stateA.byteValue, stateB.byteValue );
            stateA.verify( cursor, preStatePointerA, SLOT_A, NO_LOGICAL_POS /*Don't care*/ );
            stateB.verify( cursor, preStatePointerB, SLOT_B, NO_LOGICAL_POS /*Don't care*/ );
        }
    }
}
