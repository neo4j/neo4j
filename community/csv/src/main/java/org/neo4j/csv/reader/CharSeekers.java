/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.csv.reader;

import static org.neo4j.csv.reader.ThreadAheadReadable.threadAhead;

import java.io.FileReader;
import java.util.function.BiFunction;

/**
 * Factory for common {@link CharSeeker} implementations.
 */
public final class CharSeekers {
    private CharSeekers() {}

    /**
     * @see #charSeeker(CharReadable, Configuration, boolean, BiFunction) where the {@link Source}
     * is {@link AutoReadingSource}.
     */
    public static CharSeeker charSeeker(CharReadable reader, Configuration config, boolean readAhead) {
        return charSeeker(reader, config, readAhead, (r, c) -> new AutoReadingSource(r, c.bufferSize()));
    }

    /**
     * Instantiates a {@link BufferedCharSeeker} with optional {@link ThreadAheadReadable read-ahead} capability.
     *
     * @param reader the {@link CharReadable} which is the source of data, f.ex. a {@link FileReader}.
     * @param config {@link Configuration} for the resulting {@link CharSeeker}.
     * @param readAhead whether to start a {@link ThreadAheadReadable read-ahead thread}
     * which strives towards always keeping one buffer worth of data read and available from I/O when it's
     * time for the {@link BufferedCharSeeker} to read more data.
     * @param sourceFactory factory for the internal {@link Source} reading more data.
     * @return a {@link CharSeeker} with optional {@link ThreadAheadReadable read-ahead} capability.
     */
    public static CharSeeker charSeeker(
            CharReadable reader,
            Configuration config,
            boolean readAhead,
            BiFunction<CharReadable, Configuration, Source> sourceFactory) {
        if (readAhead) { // Thread that always has one buffer read ahead
            reader = threadAhead(reader, config.bufferSize());
        }

        // Give the reader to the char seeker
        return new BufferedCharSeeker(sourceFactory.apply(reader, config), config);
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
    public static CharSeeker charSeeker(
            CharReadable reader, final int bufferSize, boolean readAhead, final char quotationCharacter) {
        final var config = Configuration.newBuilder()
                .withQuotationCharacter(quotationCharacter)
                .withBufferSize(bufferSize)
                .build();
        return charSeeker(reader, config, readAhead);
    }
}
