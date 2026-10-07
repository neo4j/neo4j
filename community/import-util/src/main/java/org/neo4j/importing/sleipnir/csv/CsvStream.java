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
package org.neo4j.importing.sleipnir.csv;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public interface CsvStream {

    Charset TARGET_ENCODING = StandardCharsets.UTF_8;

    /**
     * Index all separators in CSV data. Separators includes field and row delimiters.
     *
     * @param buffer      a UTF-8 encoded string containing the CSV data to parse.
     * @param baseOffset  the amount to adjust the offsets with, e.g. if the {@code buffer} does not
     *                    contain part of the total data.
     * @param destination array to insert all indexes into.
     * @return the number of indexes inserted into the {@code destionation} array.
     */
    int indexSeparators(ByteBuffer buffer, int baseOffset, int[] destination);

    /**
     * Called at end of stream to verify all state is correct.
     *
     * @throws IllegalStateException if there are unclosed quotes and unmatched escape.
     */
    void validateEnd();

    /**
     * Reset all internal state kept between invocations of {@link #indexSeparators(ByteBuffer, int, int[])}.
     */
    void reset();
}
