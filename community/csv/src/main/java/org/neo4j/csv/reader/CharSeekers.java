/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.csv.reader;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;

import static org.neo4j.csv.reader.BufferedCharSeeker.DEFAULT_BUFFER_SIZE;
import static org.neo4j.csv.reader.ThreadAheadReadable.threadAhead;

/**
 * Factory for common {@link CharSeeker} implementations.
 */
public class CharSeekers
{
    /**
     * Instantiates a {@link BufferedCharSeeker} with optional {@link ThreadAheadReadable read-ahead} capability.
     *
     * @param reader the {@link Readable} which is the source of data, f.ex. a {@link FileReader}.
     * @param bufferSize buffer size of the seeker and, if enabled, the read-ahead thread.
     * @param readAhead whether or not to start a {@link ThreadAheadReadable read-ahead thread}
     * which strives towards always keeping one buffer worth of data read and available from I/O when it's
     * time for the {@link BufferedCharSeeker} to read more data.
     * @param quotationCharacter character to interpret quotation character.
     * @return a {@link CharSeeker} with optional {@link ThreadAheadReadable read-ahead} capability.
     */
    public static CharSeeker charSeeker( Readable reader, int bufferSize, boolean readAhead, char quotationCharacter )
    {
        if ( readAhead )
        {   // Thread that always has one buffer read ahead
            reader = threadAhead( reader, bufferSize );
        }

        // Give the reader to the char seeker
        return new BufferedCharSeeker( reader, bufferSize, quotationCharacter );
    }

    /**
     * Instantiates a default {@link CharSeeker} capable of reading data in the specified {@code file}.
     *
     * @param file {@link File} to read data from.
     * @return {@link CharSeeker} reading and parsing data from {@code file}.
     * @throws FileNotFoundException if the specified {@code file} doesn't exist.
     */
    public static CharSeeker charSeeker( File file, char quotationCharacter ) throws FileNotFoundException
    {
        return charSeeker( new FileReader( file ), DEFAULT_BUFFER_SIZE, true, quotationCharacter );
    }

    /**
     * Instantiates a {@link CharSeeker} capable of reading from many files, continuing into the next
     * when one has been read through.
     *
     * @param readers {@link Iterator} of {@link Readable} instances which holds the data.
     * {@link Readable} instances provided by the {@link Iterator} should be lazily opened, upon {@link Iterator#next()}.
     * @param bufferSize buffer size of the seeker and, if enabled, the read-ahead thread.
     * @param readAhead whether or not to start a {@link ThreadAheadReadable read-ahead thread}
     * which strives towards always keeping one buffer worth of data read and available from I/O when it's
     * time for the {@link BufferedCharSeeker} to read more data.
     * @param quotationCharacter character to interpret quotation character.
     * @return a {@link CharSeeker} with optional {@link ThreadAheadReadable read-ahead} capability,
     * capable of reading, and bridging, multiple files.
     */
    public static CharSeeker charSeeker( final Iterator<Readable> readers, final int bufferSize,
            final boolean readAhead, final char quotationCharacter )
    {
        return new CharSeeker()
        {
            private CharSeeker current = CharSeeker.EMPTY;

            @Override
            public boolean seek( Mark mark, int[] untilOneOfChars ) throws IOException
            {
                while ( !current.seek( mark, untilOneOfChars ) )
                {
                    if ( !goToNextReader() )
                    {
                        return false;
                    }
                }
                return true;
            }

            private boolean goToNextReader() throws IOException
            {
                if ( !readers.hasNext() )
                {
                    return false;
                }
                current.close();
                current = charSeeker( readers.next(), bufferSize, readAhead, quotationCharacter );
                return true;
            }

            @Override
            public <EXTRACTOR extends Extractor<?>> EXTRACTOR extract( Mark mark, EXTRACTOR extractor )
            {
                return current.extract( mark, extractor );
            }

            @Override
            public void close() throws IOException
            {
                current.close();
            }
        };
    }
}
