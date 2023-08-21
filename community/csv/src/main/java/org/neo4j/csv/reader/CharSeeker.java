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
import org.neo4j.values.storable.CSVHeaderInformation;

/**
 * Seeks for specific characters in a stream of characters, e.g. a {@link CharReadable}. Uses a {@link Mark}
 * as keeper of position. Once a {@link #seek(Mark, int)} has succeeded the characters specified by
 * the mark can be {@link #extract(Mark, Extractor, CSVHeaderInformation) extracted} into a value of an arbitrary type.
 *
 * Typical usage is:
 *
 * <pre>
 * CharSeeker seeker = ...
 * Mark mark = new Mark();
 * int[] delimiters = new int[] {'\t',','};
 *
 * while ( seeker.seek( mark, delimiters ) )
 * {
 *     String value = seeker.extract( mark, Extractors.STRING );
 *     // ... somehow manage the value
 *     if ( mark.isEndOfLine() )
 *     {
 *         // ... end of line, put some logic to handle that here
 *     }
 * }
 * </pre>
 *
 * Any {@link Closeable} resource that gets passed in will be closed in {@link #close()}.
 */
public interface CharSeeker extends Closeable, SourceTraceability {
    /**
     * Seeks the next occurrence of any of the characters in {@code untilOneOfChars}, or if end-of-line,
     * or even end-of-file.
     *
     * @param mark the mutable {@link Mark} which will be updated with the findings, if any.
     * @param untilChar array of characters to seek.
     * @return {@code false} if the end was reached and hence no value found, otherwise {@code true}.
     * @throws IOException in case of I/O error.
     */
    boolean seek(Mark mark, int untilChar) throws IOException;

    /**
     * Extracts the value specified by the {@link Mark}, previously populated by a call to {@link #seek(Mark, int)}.
     * @param mark the {@link Mark} specifying which part of a bigger piece of data contains the found value.
     * @param extractor {@link Extractor} capable of extracting the value.
     * @param optionalData holds additional information for spatial and temporal values or null
     * @return the extracted value.
     * @throws IllegalStateException if the {@link Extractor#extract(char[], int, int, boolean, org.neo4j.values.storable.CSVHeaderInformation) extraction}
     * extracted no value.
     */
    <T> T extract(Mark mark, Extractor<T> extractor, CSVHeaderInformation optionalData);

    /**
     * Extracts the value specified by the {@link Mark}, previously populated by a call to {@link #seek(Mark, int)}.
     * @param mark the {@link Mark} specifying which part of a bigger piece of data contains the found value.
     * @param extractor {@link Extractor} capable of extracting the value.
     * @return the extracted value.
     * @throws IllegalStateException if the {@link Extractor#extract(char[], int, int, boolean, org.neo4j.values.storable.CSVHeaderInformation) extraction}
     * extracted no value.
     */
    <T> T extract(Mark mark, Extractor<T> extractor);

    /**
     * Extracts the value specified by the {@link Mark}, previously populated by a call to {@link #seek(Mark, int)}.
     * @param mark the {@link Mark} specifying which part of a bigger piece of data contains the found value.
     * @param extractor {@link Extractor} capable of extracting the value.
     * @param optionalData holds additional information for spatial and temporal values or null
     * @return the extracted value, or {@code null} (or similar) if no value was extracted. Different extractors
     * can return other values than {@code null} to represent empty value, see {@link Extractor#isEmpty(Object)}.
     */
    <T> T tryExtract(Mark mark, Extractor<T> extractor, CSVHeaderInformation optionalData);

    /**
     * Extracts the value specified by the {@link Mark}, previously populated by a call to {@link #seek(Mark, int)}.
     * @param mark the {@link Mark} specifying which part of a bigger piece of data contains the found value.
     * @param extractor {@link Extractor} capable of extracting the value.
     * @return the extracted value, or {@code null} (or similar) if no value was extracted. Different extractors
     * can return other values than {@code null} to represent empty value, see {@link Extractor#isEmpty(Object)}.
     */
    <T> T tryExtract(Mark mark, Extractor<T> extractor);
}
