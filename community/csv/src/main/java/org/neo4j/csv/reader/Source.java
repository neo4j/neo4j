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

import java.io.Closeable;
import java.io.IOException;

/**
 * Source of data chunks to read.
 */
public interface Source extends Closeable {
    Chunk nextChunk(int seekStartPos) throws IOException;

    /**
     * One chunk of data to read.
     */
    interface Chunk {
        /**
         * @return character data to read
         */
        char[] data();

        /**
         * @return number of effective characters in the {@link #data()}
         */
        int length();

        /**
         * @return effective capacity of the {@link #data()} array
         */
        int maxFieldSize();

        /**
         * @return source description of the source this chunk was read from
         */
        String sourceDescription();

        /**
         * @return position in the {@link #data()} array to start reading from
         */
        int startPosition();

        /**
         * @return position in the {@link #data()} array where the current field which is being
         * read starts. Some characters of the current field may have started in the previous chunk
         * and so those characters are transferred over to this data array before {@link #startPosition()}
         */
        int backPosition();
    }

    record GivenChunk(
            char[] data, int length, int maxFieldSize, String sourceDescription, int startPosition, int backPosition)
            implements Chunk {}

    Chunk EMPTY_CHUNK = new GivenChunk(null, 0, 0, SourceTraceability.EMPTY.sourceDescription(), 0, 0);

    static Source singleChunk(Chunk chunk) {
        return new Source() {
            private boolean returned;

            @Override
            public void close() { // Nothing to close
            }

            @Override
            public Chunk nextChunk(int seekStartPos) {
                if (!returned) {
                    returned = true;
                    return chunk;
                }
                return EMPTY_CHUNK;
            }
        };
    }
}
