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

import org.neo4j.io.pagecache.PageCursor;

import static java.lang.String.format;
import static org.neo4j.index.internal.gbptree.GenerationSafePointer.MIN_GENERATION;
import static org.neo4j.index.internal.gbptree.GenerationSafePointer.checksumOf;
import static org.neo4j.index.internal.gbptree.GenerationSafePointer.readChecksum;
import static org.neo4j.index.internal.gbptree.GenerationSafePointer.readGeneration;
import static org.neo4j.index.internal.gbptree.GenerationSafePointer.readPointer;

/**
 * Two {@link GenerationSafePointer} forming the basis for a B+tree becoming generate-aware.
 * <p>
 * Generally a GSP fall into one out of these categories:
 * <ul>
 * <li>STABLE: generation made durable and safe by a checkpoint</li>
 * <li>UNSTABLE: generation which is currently under evolution and isn't safe until next checkoint</li>
 * <li>EMPTY: have never been written</li>
 * </ul>
 * There are variations of pointers written in UNSTABLE generation:
 * <ul>
 * <li>BROKEN: written during a concurrent page cache flush and wasn't flushed after that point before crash</li>
 * <li>CRASH: pointers written as UNSTABLE before a crash happened (non-clean shutdown) are seen as
 * CRASH during recovery</li>
 * </ul>
 * <p>
 * Combinations of above mentioned states of the two pointers dictates which, if any, to read from or write to.
 * From the perspective of callers there's only "read" and "write", the two pointers are hidden.
 * <p>
 * All methods are static and all interaction is made with primitives.
 * <p>
 * Flags in results from read/write method calls. Pointer is 6B so msb 2B can be used for flags,
 * although the most common case (successful read) has its flag zeros so a successful read doesn't need
 * any masking to extract pointer.
 * <pre>
 *     WRITE
 * [_1__,____][___ ,    ][ ... 6B pointer data ... ]
 *  ▲ ▲▲ ▲▲▲▲  ▲▲▲
 *  │ ││ ││││  │││
 *  │ ││ ││││  └└└────────────────────────────────────── POINTER STATE B (on failure)
 *  │ ││ │└└└─────────────────────────────────────────── POINTER STATE A (on failure)
 *  │ │└─└────────────────────────────────────────────── GENERATION COMPARISON (on failure):{@link #FLAG_GENERATION_B_BIG},
 *  │ │                                                  {@link #FLAG_GENERATION_EQUAL}, {@link #FLAG_GENERATION_A_BIG}
 *  │ └───────────────────────────────────────────────── 0:{@link #FLAG_SLOT_A}/1:{@link #FLAG_SLOT_B} (on success)
 *  └─────────────────────────────────────────────────── 0:{@link #FLAG_SUCCESS}/1:{@link #FLAG_FAIL}
 * </pre>
 * <pre>
 *     READ failure
 * [10__,____][__  ,    ][ ... 6B pointer data ... ]
 *    ▲▲ ▲▲▲▲  ▲▲
 *    ││ ││││  ││
 *    ││ │││└──└└─────────────────────────────────────── POINTER STATE B
 *    ││ └└└──────────────────────────────────────────── POINTER STATE A
 *    └└──────────────────────────────────────────────── GENERATION COMPARISON:
 *                                                       {@link #FLAG_GENERATION_B_BIG}, {@link #FLAG_GENERATION_EQUAL},
 *                                                       {@link #FLAG_GENERATION_A_BIG}
 * </pre>
 * <pre>
 *     READ success
 * [00__,____][____,____][ ... 6B pointer data ... ]
 *  ▲ ▲▲ ▲             ▲
 *  │ ││ └──────┬──────┘
 *  │ ││        └─────────────────────────────────────── GENERATION OFFSET or CHILD POS
 *  │ │└──────────────────────────────────────────────── 0:{@link #FLAG_ABS_OFFSET}/1:{@link #FLAG_LOGICAL_POS}
 *  │ └───────────────────────────────────────────────── 0:{@link #FLAG_SLOT_A}/1:{@link #FLAG_SLOT_B}
 *  └─────────────────────────────────────────────────── 0:{@link #FLAG_SUCCESS}/1:{@link #FLAG_FAIL}
 * </pre>
 */
class GenerationSafePointerPair
{
    static final int SIZE = GenerationSafePointer.SIZE * 2;
    static final int NO_LOGICAL_POS = -1;
    static final String GENERATION_COMPARISON_NAME_B_BIG = "A < B";
    static final String GENERATION_COMPARISON_NAME_A_BIG = "A > B";
    static final String GENERATION_COMPARISON_NAME_EQUAL = "A == B";

    // Pointer states
    static final byte STABLE = 0;     // any previous generation made safe by a checkpoint
    static final byte UNSTABLE = 1;   // current generation, generation under evolution until next checkpoint
    static final byte CRASH = 2;      // pointer written as unstable and didn't make it to checkpoint before crashing
    static final byte BROKEN = 3;     // mismatching checksum
    static final byte EMPTY = 4;      // generation and pointer all zeros

    // Flags and failure information
    static final long FLAG_SUCCESS     = 0x00000000_00000000L;
    static final long FLAG_FAIL        = 0x80000000_00000000L;
    static final long FLAG_READ        = 0x00000000_00000000L;
    static final long FLAG_WRITE       = 0x40000000_00000000L;
    static final long FLAG_GENERATION_EQUAL = 0x00000000_00000000L;
    static final long FLAG_GENERATION_A_BIG = 0x08000000_00000000L;
    static final long FLAG_GENERATION_B_BIG = 0x10000000_00000000L;
    static final long FLAG_SLOT_A      = 0x00000000_00000000L;
    static final long FLAG_SLOT_B      = 0x20000000_00000000L;
    static final long FLAG_ABS_OFFSET  = 0x00000000_00000000L;
    static final long FLAG_LOGICAL_POS = 0x10000000_00000000L;
    static final int  SHIFT_STATE_A    = 56;
    static final int  SHIFT_STATE_B    = 53;
    static final int SHIFT_GENERATION_OFFSET = 48;

    // Aggregations
    static final long SUCCESS_WRITE_TO_B = FLAG_SUCCESS | FLAG_WRITE | FLAG_SLOT_B;
    static final long SUCCESS_WRITE_TO_A = FLAG_SUCCESS | FLAG_WRITE | FLAG_SLOT_A;

    // Masks
    static final long SUCCESS_MASK         = FLAG_SUCCESS | FLAG_FAIL;
    static final long READ_OR_WRITE_MASK   = FLAG_READ | FLAG_WRITE;
    static final long SLOT_MASK            = FLAG_SLOT_A | FLAG_SLOT_B;
    static final long STATE_MASK           = 0x7; // After shift
    static final long GENERATION_COMPARISON_MASK = FLAG_GENERATION_EQUAL | FLAG_GENERATION_A_BIG | FLAG_GENERATION_B_BIG;
    static final long POINTER_MASK         = 0x0000FFFF_FFFFFFFFL;
    static final long GENERATION_OFFSET_MASK = 0x0FFF0000_00000000L;
    static final long GENERATION_OFFSET_TYPE_MASK = FLAG_ABS_OFFSET | FLAG_LOGICAL_POS;
    static final long HEADER_MASK          = ~POINTER_MASK;
    static final long MAX_GENERATION_OFFSET_MASK = 0xFFF;

    private GenerationSafePointerPair()
    {
    }

    /**
     * Reads a GSPP, returning the read pointer or a failure. Check success/failure using {@link #isSuccess(long)}
     * and if failure extract more information using {@link #failureDescription(long)}.
     *
     * @param cursor {@link PageCursor} to read from, placed at the beginning of the GSPP.
     * @param stableGeneration stable index generation.
     * @param unstableGeneration unstable index generation.
     * @param logicalPos logical position to use in header-part of the read result. If {@link #NO_LOGICAL_POS}
     * then the {@link PageCursor#getOffset() cursor offset} is used. Header will also note whether or not
     * this is a logical pos or the offset was used. This fact will be used in {@link #isLogicalPos(long)}.
     * @return most recent readable pointer, or failure. Check result using {@link #isSuccess(long)}.
     */
    public static long read( PageCursor cursor, long stableGeneration, long unstableGeneration, int logicalPos )
    {
        int gsppOffset = cursor.getOffset();

        // Try A
        long generationA = readGeneration( cursor );
        long pointerA = readPointer( cursor );
        short readChecksumA = readChecksum( cursor );
        short checksumA = checksumOf( generationA, pointerA );
        boolean correctChecksumA = readChecksumA == checksumA;

        // Try B
        long generationB = readGeneration( cursor );
        long pointerB = readPointer( cursor );
        short readChecksumB = readChecksum( cursor );
        short checksumB = checksumOf( generationB, pointerB );
        boolean correctChecksumB = readChecksumB == checksumB;

        byte pointerStateA = pointerState( stableGeneration, unstableGeneration, generationA, pointerA, correctChecksumA );
        byte pointerStateB = pointerState( stableGeneration, unstableGeneration, generationB, pointerB, correctChecksumB );

        if ( pointerStateA == UNSTABLE )
        {
            if ( pointerStateB == STABLE || pointerStateB == EMPTY )
            {
                return buildSuccessfulReadResult( FLAG_SLOT_A, logicalPos, gsppOffset, pointerA );
            }
        }
        else if ( pointerStateB == UNSTABLE )
        {
            if ( pointerStateA == STABLE || pointerStateA == EMPTY )
            {
                return buildSuccessfulReadResult( FLAG_SLOT_B, logicalPos, gsppOffset, pointerB );
            }
        }
        else if ( pointerStateA == STABLE && pointerStateB == STABLE )
        {
            // compare generation
            if ( generationA > generationB )
            {
                return buildSuccessfulReadResult( FLAG_SLOT_A, logicalPos, gsppOffset, pointerA );
            }
            else if ( generationB > generationA )
            {
                return buildSuccessfulReadResult( FLAG_SLOT_B, logicalPos, gsppOffset, pointerB );
            }
        }
        else if ( pointerStateA == STABLE )
        {
            return buildSuccessfulReadResult( FLAG_SLOT_A, logicalPos, gsppOffset, pointerA );
        }
        else if ( pointerStateB == STABLE )
        {
            return buildSuccessfulReadResult( FLAG_SLOT_B, logicalPos, gsppOffset, pointerB );
        }

        return FLAG_FAIL | FLAG_READ | generationState( generationA, generationB ) |
               ((long) pointerStateA) << SHIFT_STATE_A | ((long) pointerStateB) << SHIFT_STATE_B;
    }

    private static long buildSuccessfulReadResult( long slot, int logicalPos, int gsppOffset, long pointer )
    {
        boolean isLogicalPos = logicalPos != NO_LOGICAL_POS;
        long offsetType = isLogicalPos ? FLAG_LOGICAL_POS : FLAG_ABS_OFFSET;
        long generationOffset = isLogicalPos ? logicalPos : gsppOffset;
        if ( (generationOffset & ~MAX_GENERATION_OFFSET_MASK) != 0 )
        {
            throw new IllegalArgumentException( "Illegal generationOffset:" + generationOffset + ", it would be too large, max is " +
                    MAX_GENERATION_OFFSET_MASK );
        }
        return FLAG_SUCCESS | FLAG_READ | slot | offsetType | generationOffset << SHIFT_GENERATION_OFFSET | pointer;
    }

    /**
     * Writes a GSP at one of the GSPP slots A/B, returning the result.
     * Check success/failure using {@link #isSuccess(long)} and if failure extract more information using
     * {@link #failureDescription(long)}.
     *
     * @param cursor {@link PageCursor} to write to, placed at the beginning of the GSPP.
     * @param pointer pageId to write.
     * @param stableGeneration stable index generation.
     * @param unstableGeneration unstable index generation, which will be the generation to write in the slot.
     * @return {@code true} on success, otherwise {@code false} on failure.
     */
    public static long write( PageCursor cursor, long pointer, long stableGeneration, long unstableGeneration )
    {
        // Later there will be a selection which "slot" of GSP out of the two to write into.
        int offset = cursor.getOffset();
        pointer = pointer( pointer );

        // Try A
        long generationA = readGeneration( cursor );
        long pointerA = readPointer( cursor );
        short readChecksumA = readChecksum( cursor );
        short checksumA = checksumOf( generationA, pointerA );
        boolean correctChecksumA = readChecksumA == checksumA;

        // Try B
        long generationB = readGeneration( cursor );
        long pointerB = readPointer( cursor );
        short readChecksumB = readChecksum( cursor );
        short checksumB = checksumOf( generationB, pointerB );
        boolean correctChecksumB = readChecksumB == checksumB;

        byte pointerStateA = pointerState( stableGeneration, unstableGeneration, generationA, pointerA, correctChecksumA );
        byte pointerStateB = pointerState( stableGeneration, unstableGeneration, generationB, pointerB, correctChecksumB );

        long writeResult = writeResult( pointerStateA, pointerStateB, generationA, generationB );

        if ( isSuccess( writeResult ) )
        {
            boolean writeToA = ( writeResult & SLOT_MASK) == FLAG_SLOT_A;
            int writeOffset = writeToA ? offset : offset + GenerationSafePointer.SIZE;
            cursor.setOffset( writeOffset );
            GenerationSafePointer.write( cursor, unstableGeneration, pointer );
        }
        return writeResult;
    }

    private static long writeResult( byte pointerStateA, byte pointerStateB, long generationA, long generationB )
    {
        if ( pointerStateA == STABLE )
        {
            if ( pointerStateB == STABLE )
            {
                if ( generationA > generationB )
                {
                    // Write to slot B
                    return SUCCESS_WRITE_TO_B;
                }
                else if ( generationB > generationA )
                {
                    // Write to slot A
                    return SUCCESS_WRITE_TO_A;
                }
            }
            else
            {
                // Write to slot B
                return SUCCESS_WRITE_TO_B;
            }
        }
        else if ( pointerStateB == STABLE )
        {
            // write to slot A
            return SUCCESS_WRITE_TO_A;
        }
        else if ( pointerStateA == UNSTABLE )
        {
            if ( pointerStateB == EMPTY )
            {
                // write to slot A
                return SUCCESS_WRITE_TO_A;
            }
        }
        else if ( pointerStateB == UNSTABLE )
        {
            if ( pointerStateA == EMPTY )
            {
                // write to slot B
                return SUCCESS_WRITE_TO_B;
            }
        }
        else if ( pointerStateA == EMPTY && pointerStateB == EMPTY )
        {
            // write to slot A
            return SUCCESS_WRITE_TO_A;
        }

        // Encode error
        return FLAG_FAIL | FLAG_WRITE | generationState( generationA, generationB ) |
               ((long) pointerStateA) << SHIFT_STATE_A | ((long) pointerStateB) << SHIFT_STATE_B;
    }

    private static long generationState( long generationA, long generationB )
    {
        return generationA > generationB ? FLAG_GENERATION_A_BIG : generationB > generationA ? FLAG_GENERATION_B_BIG
                                                                                             : FLAG_GENERATION_EQUAL;
    }

    /**
     * Pointer state of a GSP (generation, pointer, checksum). Can be any of:
     * <ul>
     * <li>{@link #STABLE}</li>
     * <li>{@link #UNSTABLE}</li>
     * <li>{@link #CRASH}</li>
     * <li>{@link #BROKEN}</li>
     * <li>{@link #EMPTY}</li>
     * </ul>
     *
     * @param stableGeneration stable generation.
     * @param unstableGeneration unstable generation.
     * @param generation GSP generation.
     * @param pointer GSP pointer.
     * @param checksumIsCorrect whether or not GSP checksum matches checksum of {@code generation} and {@code pointer}.
     * @return one of the available pointer states.
     */
    static byte pointerState( long stableGeneration, long unstableGeneration,
            long generation, long pointer, boolean checksumIsCorrect )
    {
        if ( GenerationSafePointer.isEmpty( generation, pointer ) )
        {
            return EMPTY;
        }
        if ( !checksumIsCorrect )
        {
            return BROKEN;
        }
        if ( generation < MIN_GENERATION )
        {
            return BROKEN;
        }
        if ( generation <= stableGeneration )
        {
            return STABLE;
        }
        if ( generation == unstableGeneration )
        {
            return UNSTABLE;
        }
        return CRASH;
    }

    /**
     * Checks to see if a result from read/write was successful. If not more failure information can be extracted
     * using {@link #failureDescription(long)}.
     *
     * @param result result from {@link #read(PageCursor, long, long, int)} or {@link #write(PageCursor, long, long, long)}.
     * @return {@code true} if successful read/write, otherwise {@code false}.
     */
    static boolean isSuccess( long result )
    {
        return (result & SUCCESS_MASK) == FLAG_SUCCESS;
    }

    /**
     * @param readResult whole read result from {@link #read(PageCursor, long, long, int)}, containing both
     * pointer as well as header information about the pointer.
     * @return the pointer-part of {@code readResult}.
     */
    static long pointer( long readResult )
    {
        return readResult & POINTER_MASK;
    }

    /**
     * Calling {@link #read(PageCursor, long, long, int)} (potentially also {@link #write(PageCursor, long, long, long)})
     * can fail due to seeing an unexpected state of the two GSPs. Failing right there and then isn't an option
     * due to how the page cache works and that something read from a {@link PageCursor} must not be interpreted
     * until after passing a {@link PageCursor#shouldRetry()} returning {@code false}. This creates a need for
     * including failure information in result returned from these methods so that, if failed, can have
     * the caller which interprets the result fail in a proper place. That place can make use of this method
     * by getting a human-friendly description about the failure.
     *
     * @param result result from {@link #read(PageCursor, long, long, int)} or
     * {@link #write(PageCursor, long, long, long)}.
     * @return a human-friendly description of the failure.
     */
    static String failureDescription( long result )
    {
        return "GSPP " + (isRead( result ) ? "READ" : "WRITE") + " failure" +
                format( "%n  Pointer state A: %s",
                        pointerStateName( pointerStateFromResult( result, SHIFT_STATE_A ) ) ) +
                format( "%n  Pointer state B: %s",
                        pointerStateName( pointerStateFromResult( result, SHIFT_STATE_B ) ) ) +
                format( "%n  Generations: " + generationComparisonFromResult( result ) );
    }

    /**
     * Asserts that a result is {@link #isSuccess(long) successful}, otherwise throws {@link IllegalStateException}.
     *
     * @param result result returned from {@link #read(PageCursor, long, long, int)} or
     * {@link #write(PageCursor, long, long, long)}
     * @return {@code true} if {@link #isSuccess(long) successful}, for interoperability with {@code assert}.
     */
    static boolean assertSuccess( long result )
    {
        if ( !isSuccess( result ) )
        {
            throw new TreeInconsistencyException( failureDescription( result ) );
        }
        return true;
    }

    private static String generationComparisonFromResult( long result )
    {
        long bits = result & GENERATION_COMPARISON_MASK;
        if ( bits == FLAG_GENERATION_EQUAL )
        {
            return GENERATION_COMPARISON_NAME_EQUAL;
        }
        else if ( bits == FLAG_GENERATION_A_BIG )
        {
            return GENERATION_COMPARISON_NAME_A_BIG;
        }
        else if ( bits == FLAG_GENERATION_B_BIG )
        {
            return GENERATION_COMPARISON_NAME_B_BIG;
        }
        else
        {
            return "Unknown[" + bits + "]";
        }
    }

    /**
     * Name of the provided {@code pointerState} gotten from {@link #pointerState(long, long, long, long, boolean)}.
     *
     * @param pointerState pointer state to get name for.
     * @return name of {@code pointerState}.
     */
    static String pointerStateName( byte pointerState )
    {
        switch ( pointerState )
        {
        case STABLE: return "STABLE";
        case UNSTABLE: return "UNSTABLE";
        case CRASH: return "CRASH";
        case BROKEN: return "BROKEN";
        case EMPTY: return "EMPTY";
        default: return "Unknown[" + pointerState + "]";
        }
    }

    static byte pointerStateFromResult( long result, int shift )
    {
        return (byte) ((result >>> shift) & STATE_MASK);
    }

    static boolean isRead( long result )
    {
        return (result & READ_OR_WRITE_MASK) == FLAG_READ;
    }

    static boolean resultIsFromSlotA( long result )
    {
        return (result & SLOT_MASK) == FLAG_SLOT_A;
    }

    static boolean isLogicalPos( long readResult )
    {
        return (readResult & GENERATION_OFFSET_TYPE_MASK) == FLAG_LOGICAL_POS;
    }

    static int generationOffset( long readResult )
    {
        if ( (readResult & HEADER_MASK) == 0 )
        {
            throw new IllegalArgumentException( "Expected a header in read result, but read result was " + readResult );
        }

        return Math.toIntExact( (readResult & GENERATION_OFFSET_MASK) >>> SHIFT_GENERATION_OFFSET );
    }
}
