/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import java.io.FileReader;

import static org.neo4j.csv.reader.Configuration.DEFAULT;
import static org.neo4j.csv.reader.ThreadAheadReadable.threadAhead;

/**
 * Factory for common {@link CharSeeker} implementations.
 */
public class CharSeekers
{
    /**
     * Instantiates a {@link BufferedCharSeeker} with optional {@link ThreadAheadReadable read-ahead} capability.
     *
     * @param reader the {@link CharReadable} which is the source of data, f.ex. a {@link FileReader}.
     * @param config {@link Configuration} for the resulting {@link CharSeeker}.
     * @param readAhead whether or not to start a {@link ThreadAheadReadable read-ahead thread}
     * which strives towards always keeping one buffer worth of data read and available from I/O when it's
     * time for the {@link BufferedCharSeeker} to read more data.
     * @return a {@link CharSeeker} with optional {@link ThreadAheadReadable read-ahead} capability.
     */
    public static CharSeeker charSeeker( CharReadable reader, Configuration config, boolean readAhead )
    {
        if ( readAhead )
        {   // Thread that always has one buffer read ahead
            reader = threadAhead( reader, config.bufferSize() );
        }

        // Give the reader to the char seeker
        return new BufferedCharSeeker( reader, config );
    }

    /**
     * Instantiates a {@link BufferedCharSeeker} with optional {@link ThreadAheadReadable read-ahead} capability.
     *
     * @param reader the {@link CharReadable} which is the source of data, f.ex. a {@link FileReader}.
     * @param bufferSize buffer size of the seeker and, if enabled, the read-ahead thread.
     * @param readAhead whether or not to start a {@link ThreadAheadReadable read-ahead thread}
     * which strives towards always keeping one buffer worth of data read and available from I/O when it's
     * time for the {@link BufferedCharSeeker} to read more data.
     * @param quotationCharacter character to interpret quotation character.
     * @return a {@link CharSeeker} with optional {@link ThreadAheadReadable read-ahead} capability.
     */
    public static CharSeeker charSeeker( CharReadable reader, final int bufferSize, boolean readAhead,
            final char quotationCharacter )
    {
        return charSeeker( reader, new Configuration.Overridden( DEFAULT )
        {
            @Override
            public char quotationCharacter()
            {
                return quotationCharacter;
            }

            @Override
            public int bufferSize()
            {
                return bufferSize;
            }
        }, readAhead );
    }
}
