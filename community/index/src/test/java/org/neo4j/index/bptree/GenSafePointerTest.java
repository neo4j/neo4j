package org.neo4j.index.bptree;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.index.bptree.GenSafePointer.GSP;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.test.rule.RandomRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GenSafePointerTest
{
    private static final int PAGE_SIZE = GenSafePointer.SIZE * 2;
    private final PageCursor cursor = ByteArrayPageCursor.wrap( new byte[PAGE_SIZE] );
    private final GSP read = new GSP();

    @Rule
    public final RandomRule random = new RandomRule();

    @Test
    public void shouldWriteAndReadGsp() throws Exception
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
    public void shouldDetectInvalidChecksumOnReadDueToChangedData() throws Exception
    {
        // GIVEN
        int offset = 0;
        GSP initial = gsp( 123, 456 );
        initial.generation = 123;
        initial.pointer = 456;
        write( cursor, offset, initial );

        // WHEN
        cursor.putInt( offset, (int) (initial.generation + 5) );

        // THEN
        boolean matches = read( cursor, offset, read );
        assertFalse( matches );
    }

    @Test
    public void shouldDetectInvalidChecksumOnReadDueToChangedChecksum() throws Exception
    {
        // GIVEN
        int offset = 0;
        GSP initial = gsp( 123, 456 );
        initial.generation = 123;
        initial.pointer = 456;
        write( cursor, offset, initial );

        // WHEN
        cursor.putShort( offset + GenSafePointer.SIZE - GenSafePointer.CHECKSUM_SIZE,
                (short) (GenSafePointer.checksumOf( initial ) - 2) );

        // THEN
        boolean matches = read( cursor, offset, read );
        assertFalse( matches );
    }

    @Test
    public void shouldWriteAndReadGspCloseToGenerationMax() throws Exception
    {
        // GIVEN
        long generation = 0xFFFFFFFF;
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
    public void shouldWriteAndReadGspCloseToPointerMax() throws Exception
    {
        // GIVEN
        long pointer = 0xFFFF_FFFFFFFFL;
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
    public void shouldWriteAndReadGspCloseToGenerationAndPointerMax() throws Exception
    {
        // GIVEN
        int generation = 0xFFFFFFFF;
        long pointer = 0xFFFF_FFFFFFFFL;
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
    public void shouldHaveLowAccidentalChecksumCollision() throws Exception
    {
        // GIVEN
        int count = 100_000;

        // WHEN
        GSP gsp = new GSP();
        int collisions = 0;
        short reference = 0;
        for ( int i = 0; i < count; i++ )
        {
            gsp.generation = random.nextLong( 0xFFFFFFFFL );
            gsp.pointer = random.nextLong( 0xFFFF_FFFFFFFFL );
            short checksum = GenSafePointer.checksumOf( gsp );
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
        return GenSafePointer.read( cursor, into );
    }

    private void write( PageCursor cursor, int offset, GSP expected )
    {
        cursor.setOffset( offset );
        GenSafePointer.write( cursor, expected );
    }
}
