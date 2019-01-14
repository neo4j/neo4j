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
package org.neo4j.index.internal.gbptree;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.io.pagecache.ByteArrayPageCursor;
import org.neo4j.io.pagecache.PageCursor;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.index.internal.gbptree.GenerationSafePointerPair.FLAG_READ;
import static org.neo4j.index.internal.gbptree.GenerationSafePointerPair.FLAG_WRITE;
import static org.neo4j.index.internal.gbptree.GenerationSafePointerPair.GENERATION_COMPARISON_MASK;
import static org.neo4j.index.internal.gbptree.GenerationSafePointerPair.NO_LOGICAL_POS;
import static org.neo4j.index.internal.gbptree.GenerationSafePointerPair.READ_OR_WRITE_MASK;
import static org.neo4j.index.internal.gbptree.GenerationSafePointerPair.SHIFT_STATE_A;
import static org.neo4j.index.internal.gbptree.GenerationSafePointerPair.SHIFT_STATE_B;
import static org.neo4j.index.internal.gbptree.GenerationSafePointerPair.failureDescription;
import static org.neo4j.index.internal.gbptree.GenerationSafePointerPair.isRead;
import static org.neo4j.index.internal.gbptree.GenerationSafePointerPair.pointerStateFromResult;
import static org.neo4j.index.internal.gbptree.GenerationSafePointerPair.pointerStateName;

@RunWith( Parameterized.class )
public class GenerationSafePointerPairTest
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

    private static final int EXPECTED_GENERATION_DISREGARD = -2;
    private static final int EXPECTED_GENERATION_B_BIG = -1;
    private static final int EXPECTED_GENERATION_EQUAL = 0;
    private static final int EXPECTED_GENERATION_A_BIG = 1;

    private static final boolean SLOT_A = true;
    private static final boolean SLOT_B = false;
    private static final int GSPP_OFFSET = 5;
    private static final int SLOT_A_OFFSET = GSPP_OFFSET;
    private static final int SLOT_B_OFFSET = SLOT_A_OFFSET + GenerationSafePointer.SIZE;

    @Parameters( name = "{0},{1},read {2},write {3}" )
    public static Collection<Object[]> data()
    {
        Collection<Object[]> data = new ArrayList<>();

        //             ┌─────────────────┬─────────────────┬─────────────────-------──┬───────------────────────────┐
        //             │ State A         │ State B         │ Read outcome             │ Write outcome               │
        //             └─────────────────┴─────────────────┴──────────────────-------─┴────────────────────------───┘
        data.add( array( State.EMPTY,      State.EMPTY,      Fail.GENERATION_DISREGARD, Success.A ) );
        data.add( array( State.EMPTY,      State.UNSTABLE,   Success.B,                 Success.B ) );
        data.add( array( State.EMPTY,      State.STABLE,     Success.B,                 Success.A ) );
        data.add( array( State.EMPTY,      State.CRASH,      Fail.GENERATION_DISREGARD, Fail.GENERATION_DISREGARD ) );
        data.add( array( State.EMPTY,      State.BROKEN,     Fail.GENERATION_DISREGARD, Fail.GENERATION_DISREGARD ) );
        data.add( array( State.UNSTABLE,   State.EMPTY,      Success.A,                 Success.A ) );
        data.add( array( State.UNSTABLE,   State.UNSTABLE,   Fail.GENERATION_EQUAL,     Fail.GENERATION_EQUAL ) );
        data.add( array( State.UNSTABLE,   State.STABLE,     Success.A,                  Success.A ) );
        data.add( array( State.UNSTABLE,   State.CRASH,      Fail.GENERATION_A_BIG,     Fail.GENERATION_A_BIG ) );
        data.add( array( State.UNSTABLE,   State.BROKEN,     Fail.GENERATION_DISREGARD, Fail.GENERATION_DISREGARD ) );
        data.add( array( State.STABLE,     State.EMPTY,      Success.A,                 Success.B ) );
        data.add( array( State.STABLE,     State.UNSTABLE,   Success.B,                 Success.B ) );
        data.add( array( State.STABLE,     State.OLD_STABLE, Success.A,                 Success.B ) );
        data.add( array( State.OLD_STABLE, State.STABLE,     Success.B,                 Success.A ) );
        data.add( array( State.STABLE,     State.STABLE,     Fail.GENERATION_EQUAL,     Fail.GENERATION_EQUAL ) );
        data.add( array( State.STABLE,     State.CRASH,      Success.A,                 Success.B ) );
        data.add( array( State.STABLE,     State.BROKEN,     Success.A,                 Success.B ) );
        data.add( array( State.CRASH,      State.EMPTY,      Fail.GENERATION_DISREGARD, Fail.GENERATION_DISREGARD ) );
        data.add( array( State.CRASH,      State.UNSTABLE,   Fail.GENERATION_B_BIG,     Fail.GENERATION_B_BIG ) );
        data.add( array( State.CRASH,      State.STABLE,     Success.B,                 Success.A ) );
        data.add( array( State.CRASH,      State.OLD_CRASH,  Fail.GENERATION_A_BIG,     Fail.GENERATION_A_BIG ) );
        data.add( array( State.OLD_CRASH,  State.CRASH,      Fail.GENERATION_B_BIG,     Fail.GENERATION_B_BIG ) );
        data.add( array( State.CRASH,      State.CRASH,      Fail.GENERATION_EQUAL,     Fail.GENERATION_EQUAL ) );
        data.add( array( State.CRASH,      State.BROKEN,     Fail.GENERATION_DISREGARD, Fail.GENERATION_DISREGARD ) );
        data.add( array( State.BROKEN,     State.EMPTY,      Fail.GENERATION_DISREGARD, Fail.GENERATION_DISREGARD ) );
        data.add( array( State.BROKEN,     State.UNSTABLE,   Fail.GENERATION_DISREGARD, Fail.GENERATION_DISREGARD ) );
        data.add( array( State.BROKEN,     State.STABLE,     Success.B,          Success.A ) );
        data.add( array( State.BROKEN,     State.CRASH,      Fail.GENERATION_DISREGARD, Fail.GENERATION_DISREGARD ) );
        data.add( array( State.BROKEN,     State.BROKEN,     Fail.GENERATION_DISREGARD, Fail.GENERATION_DISREGARD ) );

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
    public void shouldReadWithLogicalPosition()
    {
        // GIVEN
        cursor.setOffset( SLOT_A_OFFSET );
        long preStatePointerA = stateA.materialize( cursor, POINTER_A );
        cursor.setOffset( SLOT_B_OFFSET );
        long preStatePointerB = stateB.materialize( cursor, POINTER_B );
        int pos = 1234;

        // WHEN
        cursor.setOffset( GSPP_OFFSET );
        long result = GenerationSafePointerPair.read( cursor, STABLE_GENERATION, UNSTABLE_GENERATION, pos );

        // THEN
        expectedReadOutcome.verifyRead( cursor, result, stateA, stateB, preStatePointerA, preStatePointerB, pos );
    }

    @Test
    public void shouldReadWithNoLogicalPosition()
    {
        // GIVEN
        cursor.setOffset( SLOT_A_OFFSET );
        long preStatePointerA = stateA.materialize( cursor, POINTER_A );
        cursor.setOffset( SLOT_B_OFFSET );
        long preStatePointerB = stateB.materialize( cursor, POINTER_B );

        // WHEN
        cursor.setOffset( GSPP_OFFSET );
        long result = GenerationSafePointerPair.read( cursor, STABLE_GENERATION, UNSTABLE_GENERATION, NO_LOGICAL_POS );

        // THEN
        expectedReadOutcome.verifyRead( cursor, result, stateA, stateB, preStatePointerA, preStatePointerB,
                NO_LOGICAL_POS );
    }

    @Test
    public void shouldWrite()
    {
        // GIVEN
        cursor.setOffset( SLOT_A_OFFSET );
        long preStatePointerA = stateA.materialize( cursor, POINTER_A );
        cursor.setOffset( SLOT_B_OFFSET );
        long preStatePointerB = stateB.materialize( cursor, POINTER_B );

        // WHEN
        cursor.setOffset( GSPP_OFFSET );
        long written = GenerationSafePointerPair.write( cursor, WRITTEN_POINTER, STABLE_GENERATION, UNSTABLE_GENERATION );

        // THEN
        expectedWriteOutcome.verifyWrite( cursor, written, stateA, stateB, preStatePointerA, preStatePointerB );
    }

    private static void assertFailure( long result, long readOrWrite, int generationComparison,
            byte pointerStateA, byte pointerStateB )
    {
        assertFalse( GenerationSafePointerPair.isSuccess( result ) );

        // Raw failure bits
        assertEquals( readOrWrite, result & READ_OR_WRITE_MASK );
        if ( generationComparison != EXPECTED_GENERATION_DISREGARD )
        {
            assertEquals( generationComparisonBits( generationComparison ), result & GENERATION_COMPARISON_MASK );
        }
        assertEquals( pointerStateA, pointerStateFromResult( result, SHIFT_STATE_A ) );
        assertEquals( pointerStateB, pointerStateFromResult( result, SHIFT_STATE_B ) );

        // Failure description
        String failureDescription = failureDescription( result );
        assertThat( failureDescription, containsString( isRead( result ) ? "READ" : "WRITE" ) );
        if ( generationComparison != EXPECTED_GENERATION_DISREGARD )
        {
            assertThat( failureDescription, containsString( generationComparisonName( generationComparison ) ) );
        }
        assertThat( failureDescription, containsString( pointerStateName( pointerStateA ) ) );
        assertThat( failureDescription, containsString( pointerStateName( pointerStateB ) ) );
    }

    private static String generationComparisonName( int generationComparison )
    {
        switch ( generationComparison )
        {
        case EXPECTED_GENERATION_B_BIG:
            return GenerationSafePointerPair.GENERATION_COMPARISON_NAME_B_BIG;
        case EXPECTED_GENERATION_EQUAL:
            return GenerationSafePointerPair.GENERATION_COMPARISON_NAME_EQUAL;
        case EXPECTED_GENERATION_A_BIG:
            return GenerationSafePointerPair.GENERATION_COMPARISON_NAME_A_BIG;
        default:
            throw new UnsupportedOperationException( String.valueOf( generationComparison ) );
        }
    }

    private static long generationComparisonBits( int generationComparison )
    {
        switch ( generationComparison )
        {
        case EXPECTED_GENERATION_B_BIG:
            return GenerationSafePointerPair.FLAG_GENERATION_B_BIG;
        case EXPECTED_GENERATION_EQUAL:
            return GenerationSafePointerPair.FLAG_GENERATION_EQUAL;
        case EXPECTED_GENERATION_A_BIG:
            return GenerationSafePointerPair.FLAG_GENERATION_A_BIG;
        default:
            throw new UnsupportedOperationException( String.valueOf( generationComparison ) );
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
        long generation = GenerationSafePointer.readGeneration( cursor );
        long pointer = GenerationSafePointer.readPointer( cursor );
        short checksum = GenerationSafePointer.readChecksum( cursor );
        assertEquals( GenerationSafePointer.checksumOf( generation, pointer ), checksum );
        return pointer;
    }

    private static Object[] array( Object... array )
    {
        return array;
    }

    enum State
    {
        EMPTY( GenerationSafePointerPair.EMPTY )
        {
            @Override
            long materialize( PageCursor cursor, long pointer )
            {   // Nothing to write
                return EMPTY_POINTER;
            }
        },
        BROKEN( GenerationSafePointerPair.BROKEN )
        {
            @Override
            long materialize( PageCursor cursor, long pointer )
            {
                // write an arbitrary GSP
                int offset = cursor.getOffset();
                GenerationSafePointer.write( cursor, 10, 20 );

                // then break its checksum
                cursor.setOffset( offset + GenerationSafePointer.SIZE - GenerationSafePointer.CHECKSUM_SIZE );
                short checksum = GenerationSafePointer.readChecksum( cursor );
                cursor.setOffset( offset + GenerationSafePointer.SIZE - GenerationSafePointer.CHECKSUM_SIZE );
                cursor.putShort( (short) ~checksum );
                return pointer;
            }

            @Override
            void verify( PageCursor cursor, long expectedPointer, boolean slotA, int logicalPos )
            {
                cursor.setOffset( slotA ? SLOT_A_OFFSET : SLOT_B_OFFSET );

                long generation = GenerationSafePointer.readGeneration( cursor );
                long pointer = GenerationSafePointer.readPointer( cursor );
                short checksum = GenerationSafePointer.readChecksum( cursor );
                assertNotEquals( GenerationSafePointer.checksumOf( generation, pointer ), checksum );
            }
        },
        OLD_CRASH( GenerationSafePointerPair.CRASH )
        {
            @Override
            long materialize( PageCursor cursor, long pointer )
            {
                GenerationSafePointer.write( cursor, OLD_CRASH_GENERATION, pointer );
                return pointer;
            }
        },
        CRASH( GenerationSafePointerPair.CRASH )
        {
            @Override
            long materialize( PageCursor cursor, long pointer )
            {
                GenerationSafePointer.write( cursor, CRASH_GENERATION, pointer );
                return pointer;
            }
        },
        OLD_STABLE( GenerationSafePointerPair.STABLE )
        {
            @Override
            long materialize( PageCursor cursor, long pointer )
            {
                GenerationSafePointer.write( cursor, OLD_STABLE_GENERATION, pointer );
                return pointer;
            }
        },
        STABLE( GenerationSafePointerPair.STABLE )
        {
            @Override
            long materialize( PageCursor cursor, long pointer )
            {
                GenerationSafePointer.write( cursor, STABLE_GENERATION, pointer );
                return pointer;
            }
        },
        UNSTABLE( GenerationSafePointerPair.UNSTABLE )
        {
            @Override
            long materialize( PageCursor cursor, long pointer )
            {
                GenerationSafePointer.write( cursor, UNSTABLE_GENERATION, pointer );
                return pointer;
            }
        };

        /**
         * Actual {@link GenerationSafePointerPair} pointer state value.
         */
        private final byte byteValue;

        State( byte byteValue )
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
         * @param result read-result from {@link GenerationSafePointerPair#read(PageCursor, long, long, int)}.
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
         * @param result write-result from {@link GenerationSafePointerPair#write(PageCursor, long, long, long)}.
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

        Success( long expectedPointer, boolean expectedSlot )
        {
            this.expectedPointer = expectedPointer;
            this.expectedSlot = expectedSlot;
        }

        @Override
        public void verifyRead( PageCursor cursor, long result, State stateA, State stateB,
                long preStatePointerA, long preStatePointerB, int logicalPos )
        {
            assertSuccess( result );
            long pointer = GenerationSafePointerPair.pointer( result );
            assertEquals( expectedPointer, pointer );
            assertEquals( expectedSlot == SLOT_A, GenerationSafePointerPair.resultIsFromSlotA( result ) );
            if ( logicalPos == NO_LOGICAL_POS )
            {
                assertFalse( GenerationSafePointerPair.isLogicalPos( result ) );
                assertEquals( GSPP_OFFSET, GenerationSafePointerPair.generationOffset( result ) );
            }
            else
            {
                assertTrue( GenerationSafePointerPair.isLogicalPos( result ) );
                assertEquals( logicalPos, GenerationSafePointerPair.generationOffset( result ) );
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
                    (result & GenerationSafePointerPair.SLOT_MASK) == GenerationSafePointerPair.FLAG_SLOT_A ? SLOT_A : SLOT_B;
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
            assertTrue( GenerationSafePointerPair.isSuccess( result ) );
        }
    }

    enum Fail implements Slot
    {
        GENERATION_DISREGARD( EXPECTED_GENERATION_DISREGARD ),
        GENERATION_B_BIG( EXPECTED_GENERATION_B_BIG ),
        GENERATION_EQUAL( EXPECTED_GENERATION_EQUAL ),
        GENERATION_A_BIG( EXPECTED_GENERATION_A_BIG );

        private final int generationComparison;

        Fail( int generationComparison )
        {
            this.generationComparison = generationComparison;
        }

        @Override
        public void verifyRead( PageCursor cursor, long result, State stateA, State stateB,
                long preStatePointerA, long preStatePointerB, int logicalPos )
        {
            assertFailure( result, FLAG_READ, generationComparison, stateA.byteValue, stateB.byteValue );
            stateA.verify( cursor, preStatePointerA, SLOT_A, logicalPos );
            stateB.verify( cursor, preStatePointerB, SLOT_B, logicalPos );
        }

        @Override
        public void verifyWrite( PageCursor cursor, long result, State stateA, State stateB,
                long preStatePointerA, long preStatePointerB )
        {
            assertFailure( result, FLAG_WRITE, generationComparison, stateA.byteValue, stateB.byteValue );
            stateA.verify( cursor, preStatePointerA, SLOT_A, NO_LOGICAL_POS /*Don't care*/ );
            stateB.verify( cursor, preStatePointerB, SLOT_B, NO_LOGICAL_POS /*Don't care*/ );
        }
    }
}
