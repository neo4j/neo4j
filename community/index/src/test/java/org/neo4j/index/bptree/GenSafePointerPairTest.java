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
package org.neo4j.index.bptree;

import org.junit.Test;

import org.neo4j.io.pagecache.PageCursor;

import static org.junit.Assert.assertEquals;
import static org.neo4j.index.bptree.GenSafePointerPair.POINTER_UNDECIDED_BOTH_EMPTY;

public class GenSafePointerPairTest
{
    private static final int PAGE_SIZE = 128;
    private static final int STABLE_GENERATION = 1;
    private static final int CRASH_GENERATION = 2;
    private static final int UNSTABLE_GENERATION = 3;

    private final PageCursor cursor = ByteArrayPageCursor.wrap( new byte[PAGE_SIZE] );

    @Test
    public void readEmptyEmptyShouldFail() throws Exception
    {
        // WHEN
        long result = read();

        // THEN
        assertEquals( POINTER_UNDECIDED_BOTH_EMPTY, result );
    }

    @Test
    public void writeEmptyEmptyShouldWriteSlotA() throws Exception
    {
        // GIVEN
        long pointer = 5;
        write( pointer );

        // WHEN
        long result = read();

        // THEN
        assertEquals( pointer, result );
        assertEquals( pointer, readSlotA() );
    }

    @Test
    public void readEmptyUnstableShouldReadSlotB() throws Exception
    {
        // GIVEN
        long pointer = 5;
        writeSlotB( UNSTABLE_GENERATION, pointer );

        // WHEN
        long result = read();

        // THEN
        assertEquals( pointer, result );
    }

    @Test
    public void writeEmptyUnstableShouldWriteSlotB() throws Exception
    {
        // GIVEN
        long pointer = 5;
        writeSlotB( UNSTABLE_GENERATION, pointer );

        // WHEN
        long secondPointer = 10;
        write( secondPointer );

        // THEN
        assertEquals( 0, readSlotA() );
        assertEquals( secondPointer, readSlotB() );
    }

    @Test
    public void readEmptyStableShouldReadSlotB() throws Exception
    {
        // GIVEN
        long pointer = 5;
        writeSlotB( STABLE_GENERATION, pointer );

        // WHEN
        long result = read();

        // THEN
        assertEquals( pointer, result );
    }

    @Test
    public void writeEmptyStableShouldWriteSlotA() throws Exception
    {
        // GIVEN
        long pointer = 5;
        writeSlotB( STABLE_GENERATION, pointer );

        // WHEN
        long secondPointer = 10;
        write( secondPointer );

        // THEN
        assertEquals( secondPointer, readSlotA() );
        assertEquals( pointer, readSlotB() );
    }

    @Test
    public void readEmptyCrashShouldReadSlotB() throws Exception
    {
        // GIVEN
        long pointer = 5;
        writeSlotB( STABLE_GENERATION, pointer );

        // WHEN
        long result = read();

        // THEN
        assertEquals( pointer, result );
    }

    @Test
    public void writeEmptyCrashShouldWriteSlotA() throws Exception
    {
        // GIVEN
        long pointer = 5;
        writeSlotB( STABLE_GENERATION, pointer );

        // WHEN
        long secondPointer = 10;
        write( secondPointer );

        // THEN
        assertEquals( secondPointer, readSlotA() );
        assertEquals( pointer, readSlotB() );
    }

    // EMPTY,BROKEN
    // UNSTABLE,EMPTY
    // UNSTABLE,UNSTABLE
    // UNSTABLE,STABLE
    // UNSTABLE,CRASH
    // UNSTABLE,BROKEN
    // STABLE,EMPTY
    // STABLE,UNSTABLE
    // STABLE,STABLE
    // STABLE,CRASH
    // STABLE,BROKEN
    // CRASH,EMPTY
    // CRASH,UNSTABLE
    // CRASH,STABLE
    // CRASH,CRASH
    // CRASH,BROKEN
    // BROKEN,EMPTY
    // BROKEN,UNSTABLE
    // BROKEN,STABLE
    // BROKEN,CRASH
    // BROKEN,BROKEN

    // for every ^^^ state:
    // - read should select correct one
    // - overwrite should select correct one

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

    private void write( long pointer )
    {
        cursor.setOffset( 0 );
        GenSafePointerPair.write( cursor, pointer, STABLE_GENERATION, UNSTABLE_GENERATION );
    }

    private void writeSlotA( int generation, long pointer )
    {
        cursor.setOffset( 0 );
        writeSlot( generation, pointer );
    }

    private void writeSlotB( int generation, long pointer )
    {
        cursor.setOffset( GenSafePointer.SIZE );
        writeSlot( generation, pointer );
    }

    private void writeSlot( int generation, long pointer )
    {
        GenSafePointer.write( cursor, generation, pointer );
    }
}
