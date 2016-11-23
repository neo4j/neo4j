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

import org.neo4j.io.pagecache.PageCursor;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.index.gbptree.GenSafePointerPair.BROKEN;
import static org.neo4j.index.gbptree.GenSafePointerPair.CRASH;
import static org.neo4j.index.gbptree.GenSafePointerPair.EMPTY;
import static org.neo4j.index.gbptree.GenSafePointerPair.GEN_COMPARISON_MASK;
import static org.neo4j.index.gbptree.GenSafePointerPair.READ;
import static org.neo4j.index.gbptree.GenSafePointerPair.READ_OR_WRITE_MASK;
import static org.neo4j.index.gbptree.GenSafePointerPair.STABLE;
import static org.neo4j.index.gbptree.GenSafePointerPair.STATE_SHIFT_A;
import static org.neo4j.index.gbptree.GenSafePointerPair.STATE_SHIFT_B;
import static org.neo4j.index.gbptree.GenSafePointerPair.UNSTABLE;
import static org.neo4j.index.gbptree.GenSafePointerPair.WRITE;
import static org.neo4j.index.gbptree.GenSafePointerPair.failureDescription;
import static org.neo4j.index.gbptree.GenSafePointerPair.isRead;
import static org.neo4j.index.gbptree.GenSafePointerPair.pointerStateFromResult;
import static org.neo4j.index.gbptree.GenSafePointerPair.pointerStateName;

public class GenSafePointerPairTest
{
    private static final int PAGE_SIZE = 128;
    private static final int OLDER_STABLE_GENERATION = 1;
    private static final int STABLE_GENERATION = 2;
    private static final int OLDER_CRASH_GENERATION = 3;
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

    private final PageCursor cursor = ByteArrayPageCursor.wrap( new byte[PAGE_SIZE] );

    @Test
    public void readEmptyEmptyShouldFail() throws Exception
    {
        // WHEN
        long result = read();

        // THEN
        assertFailure( result, READ, EXPECTED_GEN_DISREGARD, EMPTY, EMPTY );
    }

    @Test
    public void writeEmptyEmptyShouldWriteSlotA() throws Exception
    {
        // WHEN
        long written = write( WRITTEN_POINTER );

        // THEN
        assertWriteSuccess( SLOT_A, written );
        assertEquals( WRITTEN_POINTER, readSlotA() );
        assertEquals( EMPTY_POINTER, readSlotB() );
    }

    @Test
    public void readEmptyUnstableShouldReadSlotB() throws Exception
    {
        // GIVEN
        writeSlotB( UNSTABLE_GENERATION );

        // WHEN
        long result = read();

        // THEN
        assertReadSuccess( POINTER_B, result );
    }

    @Test
    public void writeEmptyUnstableShouldWriteSlotB() throws Exception
    {
        // GIVEN
        writeSlotB( UNSTABLE_GENERATION );

        // WHEN
        long written = write( WRITTEN_POINTER );

        // THEN
        assertWriteSuccess( SLOT_B, written );
        assertEquals( EMPTY_POINTER, readSlotA() );
        assertEquals( WRITTEN_POINTER, readSlotB() );
    }

    @Test
    public void readEmptyStableShouldReadSlotB() throws Exception
    {
        // GIVEN
        writeSlotB( STABLE_GENERATION );

        // WHEN
        long result = read();

        // THEN
        assertReadSuccess( POINTER_B, result );
    }

    @Test
    public void writeEmptyStableShouldWriteSlotA() throws Exception
    {
        // GIVEN
        writeSlotB( STABLE_GENERATION );

        // WHEN
        long written = write( WRITTEN_POINTER );

        // THEN
        assertWriteSuccess( SLOT_A, written );
        assertEquals( WRITTEN_POINTER, readSlotA() );
        assertEquals( POINTER_B, readSlotB() );
    }

    @Test
    public void readEmptyCrashShouldFail() throws Exception
    {
        // GIVEN
        writeSlotB( CRASH_GENERATION );

        // WHEN
        long result = read();

        // THEN
        assertFailure( result, READ, EXPECTED_GEN_DISREGARD, EMPTY, CRASH );
    }

    @Test
    public void writeEmptyCrashShouldFail() throws Exception
    {
        // GIVEN
        writeSlotB( CRASH_GENERATION );

        // WHEN
        long written = write( WRITTEN_POINTER );

        // THEN
        assertFailure( written, WRITE, EXPECTED_GEN_DISREGARD, EMPTY, CRASH );
        assertEquals( EMPTY_POINTER, readSlotA() );
        assertEquals( POINTER_B, readSlotB() );
    }

    @Test
    public void readEmptyBrokenShouldFail() throws Exception
    {
        // GIVEN
        writeBrokenSlotB();

        // WHEN
        long result = read();

        // THEN
        assertFailure( result, READ, EXPECTED_GEN_DISREGARD, EMPTY, BROKEN );
    }

    @Test
    public void writeEmptyBrokenShouldFail() throws Exception
    {
        // GIVEN
        writeBrokenSlotB();

        // WHEN
        long written = write( WRITTEN_POINTER );

        // THEN
        assertFailure( written, WRITE, EXPECTED_GEN_DISREGARD, EMPTY, BROKEN );
        assertEquals( EMPTY_POINTER, readSlotA() );
        assertBrokenB();
    }

    @Test
    public void readUnstableEmptyShouldReadSlotA() throws Exception
    {
        // GIVEN
        writeSlotA( UNSTABLE_GENERATION );

        // WHEN
        long result = read();

        // THEN
        assertReadSuccess( POINTER_A, result );
    }

    @Test
    public void writeUnstableEmptyShouldWriteSlotA() throws Exception
    {
        // GIVEN
        writeSlotA( UNSTABLE_GENERATION );

        // WHEN
        long written = write( WRITTEN_POINTER );

        // THEN
        assertWriteSuccess( SLOT_A, written );
        assertEquals( WRITTEN_POINTER, readSlotA() );
        assertEquals( EMPTY_POINTER, readSlotB() );
    }

    @Test
    public void readUnstableUnstableShouldFail() throws Exception
    {
        // GIVEN
        writeSlotA( UNSTABLE_GENERATION );
        writeSlotB( UNSTABLE_GENERATION );

        // WHEN
        long result = read();

        // THEN
        assertFailure( result, READ, EXPECTED_GEN_EQUAL, UNSTABLE, UNSTABLE );
    }

    @Test
    public void writeUnstableUnstableShouldFail() throws Exception
    {
        // GIVEN
        writeSlotA( UNSTABLE_GENERATION );
        writeSlotB( UNSTABLE_GENERATION );

        // WHEN
        long written = write( WRITTEN_POINTER );

        // THEN
        assertFailure( written, WRITE, EXPECTED_GEN_EQUAL, UNSTABLE, UNSTABLE );
        assertEquals( POINTER_A, readSlotA() );
        assertEquals( POINTER_B, readSlotB() );
    }

    @Test
    public void readUnstableCrashShouldFail() throws Exception
    {
        // GIVEN
        writeSlotA( UNSTABLE_GENERATION );
        writeSlotB( CRASH_GENERATION );

        // WHEN
        long result = read();

        // THEN
        assertFailure( result, READ, EXPECTED_GEN_A_BIG, UNSTABLE, CRASH );
    }

    @Test
    public void writeUnstableCrashShouldFail() throws Exception
    {
        // GIVEN
        writeSlotA( UNSTABLE_GENERATION );
        writeSlotB( CRASH_GENERATION );

        // WHEN
        long written = write( WRITTEN_POINTER );

        // THEN
        assertFailure( written, WRITE, EXPECTED_GEN_A_BIG, UNSTABLE, CRASH );
        assertEquals( POINTER_A, readSlotA() );
        assertEquals( POINTER_B, readSlotB() );
    }

    @Test
    public void readUnstableBrokenShouldFail() throws Exception
    {
        // GIVEN
        writeSlotA( UNSTABLE_GENERATION );
        writeBrokenSlotB();

        // WHEN
        long result = read();

        // THEN
        assertFailure( result, READ, EXPECTED_GEN_DISREGARD, UNSTABLE, BROKEN );
    }

    @Test
    public void writeUnstableBrokenShouldFail() throws Exception
    {
        // GIVEN
        writeSlotA( UNSTABLE_GENERATION );
        writeBrokenSlotB();

        // WHEN
        long written = write( WRITTEN_POINTER );

        // THEN
        assertFailure( written, WRITE, EXPECTED_GEN_DISREGARD, UNSTABLE, BROKEN );
        assertEquals( POINTER_A, readSlotA() );
        assertBrokenB();
    }

    @Test
    public void readStableEmptyShouldReadSlotA() throws Exception
    {
        // GIVEN
        writeSlotA( STABLE_GENERATION );

        // WHEN
        long result = read();

        // THEN
        assertReadSuccess( POINTER_A, result );
    }

    @Test
    public void writeStableEmptyShouldWriteSlotB() throws Exception
    {
        // GIVEN
        writeSlotA( STABLE_GENERATION );

        // WHEN
        long written = write( WRITTEN_POINTER );

        // THEN
        assertWriteSuccess( SLOT_B, written );
        assertEquals( POINTER_A, readSlotA() );
        assertEquals( WRITTEN_POINTER, readSlotB() );
    }

    @Test
    public void readStableUnstableShouldReadSlotB() throws Exception
    {
        // GIVEN
        writeSlotA( STABLE_GENERATION );
        writeSlotB( UNSTABLE_GENERATION );

        // WHEN
        long result = read();

        // THEN
        assertReadSuccess( POINTER_B, result );
    }

    @Test
    public void writeStableUnstableShouldWriteSlotB() throws Exception
    {
        // GIVEN
        writeSlotA( STABLE_GENERATION );
        writeSlotB( UNSTABLE_GENERATION );

        // WHEN
        long written = write( WRITTEN_POINTER );

        // THEN
        assertWriteSuccess( SLOT_B, written );
        assertEquals( POINTER_A, readSlotA() );
        assertEquals( WRITTEN_POINTER, readSlotB() );
    }

    @Test
    public void readStableStableShouldFail() throws Exception
    {
        // GIVEN
        writeSlotA( STABLE_GENERATION );
        writeSlotB( STABLE_GENERATION );

        // WHEN
        long result = read();

        // THEN
        assertFailure( result, READ, EXPECTED_GEN_EQUAL, STABLE, STABLE );
    }

    @Test
    public void writeStableStableShouldFail() throws Exception
    {
        // GIVEN
        writeSlotA( STABLE_GENERATION );
        writeSlotB( STABLE_GENERATION );

        // WHEN
        long written = write( WRITTEN_POINTER );

        // THEN
        assertFailure( written, WRITE, EXPECTED_GEN_EQUAL, STABLE, STABLE );
        assertEquals( POINTER_A, readSlotA() );
        assertEquals( POINTER_B, readSlotB() );
    }

    @Test
    public void readStableOlderStableShouldReadSlotA() throws Exception
    {
        // GIVEN
        writeSlotA( STABLE_GENERATION );
        writeSlotB( OLDER_STABLE_GENERATION );

        // WHEN
        long result = read();

        // THEN
        assertReadSuccess( POINTER_A, result );
    }

    @Test
    public void writeStableOlderStableShouldWriteSlotB() throws Exception
    {
        // GIVEN
        writeSlotA( STABLE_GENERATION );
        writeSlotB( OLDER_STABLE_GENERATION );

        // WHEN
        long written = write( WRITTEN_POINTER );

        // THEN
        assertWriteSuccess( SLOT_B, written );
        assertEquals( POINTER_A, readSlotA() );
        assertEquals( WRITTEN_POINTER, readSlotB() );
    }

    @Test
    public void readOlderStableStableShouldReadSlotB() throws Exception
    {
        // GIVEN
        writeSlotA( OLDER_STABLE_GENERATION );
        writeSlotB( STABLE_GENERATION );

        // WHEN
        long result = read();

        // THEN
        assertReadSuccess( POINTER_B, result );
    }

    @Test
    public void writeOlderStableStableShouldWriteSlotA() throws Exception
    {
        // GIVEN
        writeSlotA( OLDER_STABLE_GENERATION );
        writeSlotB( STABLE_GENERATION );

        // WHEN
        long written = write( WRITTEN_POINTER );

        // THEN
        assertWriteSuccess( SLOT_A, written );
        assertEquals( WRITTEN_POINTER, readSlotA() );
        assertEquals( POINTER_B, readSlotB() );
    }

    @Test
    public void readStableCrashShouldReadSlotA() throws Exception
    {
        // GIVEN
        writeSlotA( STABLE_GENERATION );
        writeSlotB( CRASH_GENERATION );

        // WHEN
        long result = read();

        // THEN
        assertReadSuccess( POINTER_A, result );
    }

    @Test
    public void writeStableCrashShouldWriteSlotB() throws Exception
    {
        // GIVEN
        writeSlotA( STABLE_GENERATION );
        writeSlotB( CRASH_GENERATION );

        // WHEN
        long written = write( WRITTEN_POINTER );

        // THEN
        assertWriteSuccess( SLOT_B, written );
        assertEquals( POINTER_A, readSlotA() );
        assertEquals( WRITTEN_POINTER, readSlotB() );
    }

    @Test
    public void readStableBrokenShouldReadSlotA() throws Exception
    {
        // GIVEN
        writeSlotA( STABLE_GENERATION );
        writeBrokenSlotB();

        // WHEN
        long result = read();

        // THEN
        assertReadSuccess( POINTER_A, result );
    }

    @Test
    public void writeStableBrokenShouldWriteSlotB() throws Exception
    {
        // GIVEN
        writeSlotA( STABLE_GENERATION );
        writeBrokenSlotB();

        // WHEN
        long written = write( WRITTEN_POINTER );

        // THEN
        assertWriteSuccess( SLOT_B, written );
        assertEquals( POINTER_A, readSlotA() );
        assertEquals( WRITTEN_POINTER, readSlotB() );
    }

    @Test
    public void readCrashEmptyShouldFail() throws Exception
    {
        // GIVEN
        writeSlotA( CRASH_GENERATION );

        // WHEN
        long result = read();

        // THEN
        assertFailure( result, READ, EXPECTED_GEN_DISREGARD, CRASH, EMPTY );
    }

    @Test
    public void writeCrashEmptyShouldFail() throws Exception
    {
        // GIVEN
        writeSlotA( CRASH_GENERATION );

        // WHEN
        long written = write( WRITTEN_POINTER );

        // THEN
        assertFailure( written, WRITE, EXPECTED_GEN_DISREGARD, CRASH, EMPTY );
        assertEquals( POINTER_A, readSlotA() );
        assertEquals( EMPTY_POINTER, readSlotB() );
    }

    @Test
    public void readCrashUnstableShouldFail() throws Exception
    {
        // GIVEN
        writeSlotA( CRASH_GENERATION );
        writeSlotB( UNSTABLE_GENERATION );

        // WHEN
        long result = read();

        // THEN
        assertFailure( result, READ, EXPECTED_GEN_B_BIG, CRASH, UNSTABLE );
    }

    @Test
    public void writeCrashUnstableShouldFail() throws Exception
    {
        // GIVEN
        writeSlotA( CRASH_GENERATION );
        writeSlotB( UNSTABLE_GENERATION );

        // WHEN
        long written = write( WRITTEN_POINTER );

        // THEN
        assertFailure( written, WRITE, EXPECTED_GEN_B_BIG, CRASH, UNSTABLE );
        assertEquals( POINTER_A, readSlotA() );
        assertEquals( POINTER_B, readSlotB() );
    }

    @Test
    public void readCrashStableShouldReadSlotB() throws Exception
    {
        // GIVEN
        writeSlotA( CRASH_GENERATION );
        writeSlotB( STABLE_GENERATION );

        // WHEN
        long result = read();

        // THEN
        assertReadSuccess( POINTER_B, result );
    }

    @Test
    public void writeCrashStableShouldWriteSlotA() throws Exception
    {
        // GIVEN
        writeSlotA( CRASH_GENERATION );
        writeSlotB( STABLE_GENERATION );

        // WHEN
        long written = write( WRITTEN_POINTER );

        // THEN
        assertWriteSuccess( SLOT_A, written );
        assertEquals( WRITTEN_POINTER, readSlotA() );
        assertEquals( POINTER_B, readSlotB() );
    }

    @Test
    public void readCrashCrashShouldFail() throws Exception
    {
        // GIVEN
        writeSlotA( CRASH_GENERATION );
        writeSlotB( CRASH_GENERATION );

        // WHEN
        long result = read();

        // THEN
        assertFailure( result, READ, EXPECTED_GEN_EQUAL, CRASH, CRASH );
    }

    @Test
    public void writeCrashCrashShouldFail() throws Exception
    {
        // GIVEN
        writeSlotA( CRASH_GENERATION );
        writeSlotB( CRASH_GENERATION );

        // WHEN
        long written = write( WRITTEN_POINTER );

        // THEN
        assertFailure( written, WRITE, EXPECTED_GEN_EQUAL, CRASH, CRASH );
        assertEquals( POINTER_A, readSlotA() );
        assertEquals( POINTER_B, readSlotB() );
    }

    @Test
    public void readCrashOlderCrashShouldFail() throws Exception
    {
        // GIVEN
        writeSlotA( CRASH_GENERATION );
        writeSlotB( OLDER_CRASH_GENERATION );

        // WHEN
        long result = read();

        // THEN
        assertFailure( result, READ, EXPECTED_GEN_A_BIG, CRASH, CRASH );
    }

    @Test
    public void writeCrashOlderCrashShouldFail() throws Exception
    {
        // GIVEN
        writeSlotA( CRASH_GENERATION );
        writeSlotB( OLDER_CRASH_GENERATION );

        // WHEN
        long written = write( WRITTEN_POINTER );

        // THEN
        assertFailure( written, WRITE, EXPECTED_GEN_A_BIG, CRASH, CRASH );
        assertEquals( POINTER_A, readSlotA() );
        assertEquals( POINTER_B, readSlotB() );
    }

    @Test
    public void readOlderCrashCrashShouldFail() throws Exception
    {
        // GIVEN
        writeSlotA( OLDER_CRASH_GENERATION );
        writeSlotB( CRASH_GENERATION );

        // WHEN
        long result = read();

        // THEN
        assertFailure( result, READ, EXPECTED_GEN_B_BIG, CRASH, CRASH );
    }

    @Test
    public void writeOlderCrashCrashShouldFail() throws Exception
    {
        // GIVEN
        writeSlotA( OLDER_CRASH_GENERATION );
        writeSlotB( CRASH_GENERATION );

        // WHEN
        long written = write( WRITTEN_POINTER );

        // THEN
        assertFailure( written, WRITE, EXPECTED_GEN_B_BIG, CRASH, CRASH );
        assertEquals( POINTER_A, readSlotA() );
        assertEquals( POINTER_B, readSlotB() );
    }

    @Test
    public void readCrashBrokenShouldFail() throws Exception
    {
        // GIVEN
        writeSlotA( CRASH_GENERATION );
        writeBrokenSlotB();

        // WHEN
        long result = read();

        // THEN
        assertFailure( result, READ, EXPECTED_GEN_DISREGARD, CRASH, BROKEN );
    }

    @Test
    public void writeCrashBrokenShouldFail() throws Exception
    {
        // GIVEN
        writeSlotA( CRASH_GENERATION );
        writeBrokenSlotB();

        // WHEN
        long written = write( WRITTEN_POINTER );

        // THEN
        assertFailure( written, WRITE, EXPECTED_GEN_DISREGARD, CRASH, BROKEN );
        assertEquals( POINTER_A, readSlotA() );
        assertBrokenB();
    }

    @Test
    public void readBrokenEmptyShouldFail() throws Exception
    {
        // GIVEN
        writeBrokenSlotA();

        // WHEN
        long result = read();

        // THEN
        assertFailure( result, READ, EXPECTED_GEN_DISREGARD, BROKEN, EMPTY );
    }

    @Test
    public void writeBrokenEmptyShouldFail() throws Exception
    {
        // GIVEN
        writeBrokenSlotA();

        // WHEN
        long written = write( WRITTEN_POINTER );

        // THEN
        assertFailure( written, WRITE, EXPECTED_GEN_DISREGARD, BROKEN, EMPTY );
        assertBrokenA();
        assertEquals( EMPTY_POINTER, readSlotB() );
    }

    @Test
    public void readBrokenUnstableShouldFail() throws Exception
    {
        // GIVEN
        writeBrokenSlotA();
        writeSlotB( UNSTABLE_GENERATION );

        // WHEN
        long result = read();

        // THEN
        assertFailure( result, READ, EXPECTED_GEN_DISREGARD, BROKEN, UNSTABLE );
    }

    @Test
    public void writeBrokenUnstableShouldFail() throws Exception
    {
        // GIVEN
        writeBrokenSlotA();
        writeSlotB( UNSTABLE_GENERATION );

        // WHEN
        long written = write( WRITTEN_POINTER );

        // THEN
        assertFailure( written, WRITE, EXPECTED_GEN_DISREGARD, BROKEN, UNSTABLE );
        assertBrokenA();
        assertEquals( POINTER_B, readSlotB() );
    }

    @Test
    public void readBrokenStableShouldReadSlotB() throws Exception
    {
        // GIVEN
        writeBrokenSlotA();
        writeSlotB( STABLE_GENERATION );

        // WHEN
        long result = read();

        // THEN
        assertReadSuccess( POINTER_B, result );
    }

    @Test
    public void writeBrokenStableShouldWriteSlotA() throws Exception
    {
        // GIVEN
        writeBrokenSlotA();
        writeSlotB( STABLE_GENERATION );

        // WHEN
        long written = write( WRITTEN_POINTER );

        // THEN
        assertWriteSuccess( SLOT_A, written );
        assertEquals( WRITTEN_POINTER, readSlotA() );
        assertEquals( POINTER_B, readSlotB() );
    }

    @Test
    public void readBrokenCrashShouldFail() throws Exception
    {
        // GIVEN
        writeBrokenSlotA();
        writeSlotB( CRASH_GENERATION );

        // WHEN
        long result = read();

        // THEN
        assertFailure( result, READ, EXPECTED_GEN_DISREGARD, BROKEN, CRASH );
    }

    @Test
    public void writeBrokenCrashShouldFail() throws Exception
    {
        // GIVEN
        writeBrokenSlotA();
        writeSlotB( CRASH_GENERATION );

        // WHEN
        long written = write( WRITTEN_POINTER );

        // THEN
        assertFailure( written, WRITE, EXPECTED_GEN_DISREGARD, BROKEN, CRASH );
        assertBrokenA();
        assertEquals( POINTER_B, readSlotB() );
    }

    @Test
    public void readBrokenBrokenShouldFail() throws Exception
    {
        // GIVEN
        writeBrokenSlotA();
        writeBrokenSlotB();

        // WHEN
        long result = read();

        // THEN
        assertFailure( result, READ, EXPECTED_GEN_DISREGARD, BROKEN, BROKEN );
    }

    @Test
    public void writeBrokenBrokenShouldFail() throws Exception
    {
        // GIVEN
        writeBrokenSlotA();
        writeBrokenSlotB();

        // WHEN
        long written = write( WRITTEN_POINTER );

        // THEN
        assertFailure( written, WRITE, EXPECTED_GEN_DISREGARD, BROKEN, BROKEN );
        assertBrokenA();
        assertBrokenB();
    }

    private void assertFailure( long result, long readOrWrite, int genComparison,
            byte pointerStateA, byte pointerStateB )
    {
        assertFalse( GenSafePointerPair.isSuccess( result ) );

        // Raw failure bits
        assertEquals( readOrWrite, result & READ_OR_WRITE_MASK );
        if ( genComparison != EXPECTED_GEN_DISREGARD )
        {
            assertEquals( genComparisonBits( genComparison ), result & GEN_COMPARISON_MASK );
        }
        assertEquals( pointerStateA, pointerStateFromResult( result, STATE_SHIFT_A ) );
        assertEquals( pointerStateB, pointerStateFromResult( result, STATE_SHIFT_B ) );

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

    private String genComparisonName( int genComparison )
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

    private long genComparisonBits( int genComparison )
    {
        switch ( genComparison )
        {
        case EXPECTED_GEN_B_BIG:
            return GenSafePointerPair.GEN_B_BIG;
        case EXPECTED_GEN_EQUAL:
            return GenSafePointerPair.GEN_EQUAL;
        case EXPECTED_GEN_A_BIG:
            return GenSafePointerPair.GEN_A_BIG;
        default:
            throw new UnsupportedOperationException( String.valueOf( genComparison ) );
        }
    }

    private long readSlotA()
    {
        cursor.setOffset( 0 );
        return readSlot();
    }

    private long readSlotB()
    {
        cursor.setOffset( GenSafePointer.SIZE );
        return readSlot();
    }

    private long readSlot()
    {
        long generation = GenSafePointer.readGeneration( cursor );
        long pointer = GenSafePointer.readPointer( cursor );
        short checksum = GenSafePointer.readChecksum( cursor );
        assertEquals( GenSafePointer.checksumOf( generation, pointer ), checksum );
        return pointer;
    }

    private long read()
    {
        cursor.setOffset( 0 );
        return GenSafePointerPair.read( cursor, STABLE_GENERATION, UNSTABLE_GENERATION );
    }

    private long write( long pointer )
    {
        cursor.setOffset( 0 );
        return GenSafePointerPair.write( cursor, pointer, STABLE_GENERATION, UNSTABLE_GENERATION );
    }

    private void writeSlotA( long generation )
    {
        cursor.setOffset( 0 );
        writeSlot( generation, POINTER_A );
    }

    private void writeSlotB( long generation )
    {
        cursor.setOffset( GenSafePointer.SIZE );
        writeSlot( generation, POINTER_B );
    }

    private void writeSlot( long generation, long pointer )
    {
        GenSafePointer.write( cursor, generation, pointer );
    }

    private void writeBrokenSlotA()
    {
        cursor.setOffset( 0 );
        writeBrokenSlot();
    }

    private void writeBrokenSlotB()
    {
        cursor.setOffset( GenSafePointer.SIZE );
        writeBrokenSlot();
    }

    private void writeBrokenSlot()
    {
        int offset = cursor.getOffset();
        writeSlot( 10, 20 );
        cursor.setOffset( offset + GenSafePointer.SIZE - GenSafePointer.CHECKSUM_SIZE );
        short checksum = GenSafePointer.readChecksum( cursor );
        cursor.setOffset( offset + GenSafePointer.SIZE - GenSafePointer.CHECKSUM_SIZE );
        cursor.putShort( (short) ~checksum );
    }

    private void assertBrokenA()
    {
        cursor.setOffset( 0 );
        assertBroken();
    }

    private void assertBrokenB()
    {
        cursor.setOffset( GenSafePointer.SIZE );
        assertBroken();
    }

    private void assertBroken()
    {
        long generation = GenSafePointer.readGeneration( cursor );
        long pointer = GenSafePointer.readPointer( cursor );
        short checksum = GenSafePointer.readChecksum( cursor );
        assertNotEquals( GenSafePointer.checksumOf( generation, pointer ), checksum );
    }

    private void assertSuccess( long result )
    {
        assertTrue( GenSafePointerPair.isSuccess( result ) );
    }

    private void assertWriteSuccess( boolean expectedSlot, long result )
    {
        assertSuccess( result );
        boolean actuallyWrittenSlot =
                (result & GenSafePointerPair.WRITE_TO_MASK) == GenSafePointerPair.WRITE_TO_A ? SLOT_A : SLOT_B;
        assertEquals( expectedSlot, actuallyWrittenSlot );
    }

    private void assertReadSuccess( long expectedPointer, long result )
    {
        assertSuccess( result );
        assertEquals( result, expectedPointer );
    }
}
