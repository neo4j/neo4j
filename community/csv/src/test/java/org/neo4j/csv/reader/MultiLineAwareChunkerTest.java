package org.neo4j.csv.reader;

import org.junit.Test;

import java.io.StringReader;

import org.neo4j.csv.reader.Source.Chunk;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static org.neo4j.csv.reader.ClosestNewLineChunkerTest.charactersOf;

public class MultiLineAwareChunkerTest
{
    @Test
    public void shouldBackUpChunkToClosestNewline() throws Exception
    {
        // GIVEN
        CharReadable reader = Readables.wrap( new StringReader(
                "1,2,3,4,5\n" +
                "6,7,8,9,10"  ) );
        try ( Chunker source = new MultiLineAwareChunker( reader, config( 15 ), 5, ',' ) )
        {
            // WHEN
            Chunk chunk = source.newChunk();
            assertTrue( source.nextChunk( chunk ) );
            assertArrayEquals( "1,2,3,4,5\n".toCharArray(), charactersOf( chunk ) );
            assertTrue( source.nextChunk( chunk ) );
            assertArrayEquals( "6,7,8,9,10".toCharArray(), charactersOf( chunk ) );

            // THEN
            assertFalse( source.nextChunk( chunk ) );
        }
    }

    private static Configuration config( int bufferSize )
    {
        return new Configuration.Default()
        {
            @Override
            public int bufferSize()
            {
                return bufferSize;
            }
        };
    }
}
