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

import org.neo4j.values.storable.CSVHeaderInformation;

/**
 * Extracts a value from a part of a {@code char[]} into any type of value, f.ex. a {@link Extractors#string()},
 * {@link Extractors#long_() long} or {@link Extractors#intArray()}.
 *
 * An {@link Extractor} is mutable for the single purpose of ability to reuse its value instance. Consider extracting
 * a primitive int -
 *
 * Sub-interfaces and implementations can and should specify specific accessors for the purpose
 * of performance and less garbage, f.ex. where an IntExtractor could have an accessor method for
 * getting the extracted value as primitive int.
 *
 * @see Extractors for a collection of very common extractors.
 */
public interface Extractor<T> extends Cloneable {
    /**
     * Extracts value of type {@code T} from the given character data.
     * @param data characters in a buffer.
     * @param offset offset into the buffer where the value starts.
     * @param length number of characters from the offset to extract.
     * @param hadQuotes whether there were skipped characters, f.ex. quotation.
     * @param optionalData optional data to be used for spatial or temporal values or null if csv header did not use it
     * @return the extracted value, or {@code null} (or similar) if no value was extracted.
     */
    T extract(char[] data, int offset, int length, boolean hadQuotes, CSVHeaderInformation optionalData);

    /**
     * Extracts value of type {@code T} from the given character data.
     * @param data characters in a buffer.
     * @param offset offset into the buffer where the value starts.
     * @param length number of characters from the offset to extract.
     * @param hadQuotes whether there were skipped characters, f.ex. quotation.
     * @return the extracted value, or {@code null} (or similar) if no value was extracted.
     */
    T extract(char[] data, int offset, int length, boolean hadQuotes);

    /**
     * @return string representation of what type of value of produces. Also used as key in {@link Extractors}.
     */
    String name();

    /**
     * Normalizes this extractor to that of a broader type, if possible. E.g. an extractor for {@code int} becomes {@code long}.
     * This normalization should match higher levels of type systems, like Cypher.
     *
     * @return an extractor which potentially is of a broader type than this extractor.
     */
    Extractor<?> normalize();

    /**
     * Asks whether a certain extracted value is empty. Some extractors may not return {@code null} as their
     * "there's nothing here" value.
     *
     * @param value a value from a previous {@link #extract(char[], int, int, boolean) extraction}.
     * @return whether the value is empty.
     */
    boolean isEmpty(Object value);
}
