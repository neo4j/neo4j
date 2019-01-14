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

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.io.pagecache.ByteArrayPageCursor;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.test.rule.RandomRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class GenerationSafePointerTest
{
    private static final int PAGE_SIZE = GenerationSafePointer.SIZE * 2;
    private final PageCursor cursor = ByteArrayPageCursor.wrap( new byte[PAGE_SIZE] );
    private final GSP read = new GSP();

    @Rule
    public final RandomRule random = new RandomRule();

    @Test
    public void shouldWriteAndReadGsp()
    {
        // GIVEN
        int offset = 3;
        GSP expected = gsp( 10, 110 );

        // WHEN
        write( cursor, offset, expected );

        // THEN
        boolean matches = read( cursor, offset, read );
        assertTrue( matches );
        assertEquals( expected, read );
    }

    @Test
    public void shouldReadGspWithZeroValues()
    {
        // GIVEN
        int offset = 3;
        GSP expected = gsp( 0, 0 );

        // THEN
        boolean matches = read( cursor, offset, read );
        assertTrue( matches );
        assertEquals( expected, read );
    }

    @Test
    public void shouldDetectInvalidChecksumOnReadDueToChangedGeneration()
    {
        // GIVEN
        int offset = 0;
        GSP initial = gsp( 123, 456 );
        write( cursor, offset, initial );

        // WHEN
        cursor.putInt( offset, (int) (initial.generation + 5) );

        // THEN
        boolean matches = read( cursor, offset, read );
        assertFalse( matches );
    }

    @Test
    public void shouldDetectInvalidChecksumOnReadDueToChangedChecksum()
    {
        // GIVEN
        int offset = 0;
        GSP initial = gsp( 123, 456 );
        write( cursor, offset, initial );

        // WHEN
        cursor.putShort( offset + GenerationSafePointer.SIZE - GenerationSafePointer.CHECKSUM_SIZE,
                (short) (checksumOf( initial ) - 2) );

        // THEN
        boolean matches = read( cursor, offset, read );
        assertFalse( matches );
    }

    @Test
    public void shouldWriteAndReadGspCloseToGenerationMax()
    {
        // GIVEN
        long generation = GenerationSafePointer.MAX_GENERATION;
        GSP expected = gsp( generation, 12345 );
        write( cursor, 0, expected );

        // WHEN
        GSP read = new GSP();
        boolean matches = read( cursor, 0, read );

        // THEN
        assertTrue( matches );
        assertEquals( expected, read );
        assertEquals( generation, read.generation );
    }

    @Test
    public void shouldWriteAndReadGspCloseToPointerMax()
    {
        // GIVEN
        long pointer = GenerationSafePointer.MAX_POINTER;
        GSP expected = gsp( 12345, pointer );
        write( cursor, 0, expected );

        // WHEN
        GSP read = new GSP();
        boolean matches = read( cursor, 0, read );

        // THEN
        assertTrue( matches );
        assertEquals( expected, read );
        assertEquals( pointer, read.pointer );
    }

    @Test
    public void shouldWriteAndReadGspCloseToGenerationAndPointerMax()
    {
        // GIVEN
        long generation = GenerationSafePointer.MAX_GENERATION;
        long pointer = GenerationSafePointer.MAX_POINTER;
        GSP expected = gsp( generation, pointer );
        write( cursor, 0, expected );

        // WHEN
        GSP read = new GSP();
        boolean matches = read( cursor, 0, read );

        // THEN
        assertTrue( matches );
        assertEquals( expected, read );
        assertEquals( generation, read.generation );
        assertEquals( pointer, read.pointer );
    }

    @Test
    public void shouldThrowIfPointerToLarge()
    {
        // GIVEN
        long generation = GenerationSafePointer.MIN_GENERATION;
        long pointer = GenerationSafePointer.MAX_POINTER + 1;
        GSP broken = gsp( generation, pointer );

        // WHEN
        try
        {
            write( cursor, 0, broken );
            fail( "Expected to throw" );
        }
        catch ( IllegalArgumentException e )
        {
            // THEN
            // good
        }
    }

    @Test
    public void shouldThrowIfPointerToSmall()
    {
        // GIVEN
        long generation = GenerationSafePointer.MIN_GENERATION;
        long pointer = GenerationSafePointer.MIN_POINTER - 1;
        GSP broken = gsp( generation, pointer );

        // WHEN
        try
        {
            write( cursor, 0, broken );
            fail( "Expected to throw" );
        }
        catch ( IllegalArgumentException e )
        {
            // THEN
            // good
        }
    }

    @Test
    public void shouldThrowIfGenerationToLarge()
    {
        // GIVEN
        long generation = GenerationSafePointer.MAX_GENERATION + 1;
        long pointer = GenerationSafePointer.MIN_POINTER;
        GSP broken = gsp( generation, pointer );

        // WHEN
        try
        {
            write( cursor, 0, broken );
            fail( "Expected to throw" );
        }
        catch ( IllegalArgumentException e )
        {
            // THEN
            // good
        }
    }

    @Test
    public void shouldThrowIfGenerationToSmall()
    {
        // GIVEN
        long generation = GenerationSafePointer.MIN_GENERATION - 1;
        long pointer = GenerationSafePointer.MIN_POINTER;
        GSP broken = gsp( generation, pointer );

        // WHEN
        try
        {
            write( cursor, 0, broken );
            fail( "Expected to throw" );
        }
        catch ( IllegalArgumentException e )
        {
            // THEN
            // good
        }
    }

    @Test
    public void shouldHaveLowAccidentalChecksumCollision()
    {
        // GIVEN
        int count = 100_000;

        // WHEN
        GSP gsp = new GSP();
        int collisions = 0;
        short reference = 0;
        for ( int i = 0; i < count; i++ )
        {
            gsp.generation = random.nextLong( GenerationSafePointer.MAX_GENERATION );
            gsp.pointer = random.nextLong( GenerationSafePointer.MAX_POINTER );
            short checksum = checksumOf( gsp );
            if ( i == 0 )
            {
                reference = checksum;
            }
            else
            {
                boolean unique = checksum != reference;
                collisions += unique ? 0 : 1;
            }
        }

        // THEN
        assertTrue( (double) collisions / count < 0.0001 );
    }

    private GSP gsp( long generation, long pointer )
    {
        GSP gsp = new GSP();
        gsp.generation = generation;
        gsp.pointer = pointer;
        return gsp;
    }

    private boolean read( PageCursor cursor, int offset, GSP into )
    {
        cursor.setOffset( offset );
        into.generation = GenerationSafePointer.readGeneration( cursor );
        into.pointer = GenerationSafePointer.readPointer( cursor );
        return GenerationSafePointer.verifyChecksum( cursor, into.generation, into.pointer );
    }

    private void write( PageCursor cursor, int offset, GSP gsp )
    {
        cursor.setOffset( offset );
        GenerationSafePointer.write( cursor, gsp.generation, gsp.pointer );
    }

    private static short checksumOf( GSP gsp )
    {
        return GenerationSafePointer.checksumOf( gsp.generation, gsp.pointer );
    }

    /**
     * Data for a GSP, i.e. generation and pointer. Checksum is generated from those two fields and
     * so isn't a field in this struct - ahem class. The reason this class exists is that we, when reading,
     * want to read two fields and a checksum and match the two fields with the checksum. This class
     * is designed to be mutable and should be reused in as many places as possible.
     */
    private static class GSP
    {
        long generation; // unsigned int
        long pointer;

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + (int) (generation ^ (generation >>> 32));
            result = prime * result + (int) (pointer ^ (pointer >>> 32));
            return result;
        }
        @Override
        public boolean equals( Object obj )
        {
            if ( this == obj )
            {
                return true;
            }
            if ( obj == null )
            {
                return false;
            }
            if ( getClass() != obj.getClass() )
            {
                return false;
            }
            GSP other = (GSP) obj;
            if ( generation != other.generation )
            {
                return false;
            }
            return pointer == other.pointer;
        }

        @Override
        public String toString()
        {
            return "[generation:" + generation + ",p:" + pointer + "]";
        }
    }
}
