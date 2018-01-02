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

import java.io.Closeable;
import java.io.IOException;

/**
 * Seeks for specific characters in a stream of characters, e.g. a {@link CharReadable}. Uses a {@link Mark}
 * as keeper of position. Once a {@link #seek(Mark, int)} has succeeded the characters specified by
 * the mark can be {@link #extract(Mark, Extractor) extracted} into a value of an arbitrary type.
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
 *
 * @author Mattias Persson
 */
public interface CharSeeker extends Closeable, SourceTraceability
{
    /**
     * Seeks the next occurrence of any of the characters in {@code untilOneOfChars}, or if end-of-line,
     * or even end-of-file.
     *
     * @param mark the mutable {@link Mark} which will be updated with the findings, if any.
     * @param untilChar array of characters to seek.
     * @return {@code false} if the end was reached and hence no value found, otherwise {@code true}.
     * @throws IOException in case of I/O error.
     */
    boolean seek( Mark mark, int untilChar ) throws IOException;

    /**
     * Extracts the value specified by the {@link Mark}, previously populated by a call to {@link #seek(Mark, int)}.
     * @param mark the {@link Mark} specifying which part of a bigger piece of data contains the found value.
     * @param extractor {@link Extractor} capable of extracting the value.
     * @return the supplied {@link Extractor}, which after the call carries the extracted value itself,
     * where either {@link Extractor#value()} or a more specific accessor method can be called to access the value.
     * @throws IllegalStateException if the {@link Extractor#extract(char[], int, int, boolean) extraction}
     * returns {@code false}.
     */
    <EXTRACTOR extends Extractor<?>> EXTRACTOR extract( Mark mark, EXTRACTOR extractor );

    /**
     * Extracts the value specified by the {@link Mark}, previously populated by a call to {@link #seek(Mark, int)}.
     * @param mark the {@link Mark} specifying which part of a bigger piece of data contains the found value.
     * @param extractor {@link Extractor} capable of extracting the value.
     * @return {@code true} if a value was extracted, otherwise {@code false}. Probably the only reason for
     * returning {@code false} would be if the data to extract was empty.
     */
    boolean tryExtract( Mark mark, Extractor<?> extractor );
}
