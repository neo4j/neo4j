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

/**
 * Extracts a value from a part of a {@code char[]} into any type of value, f.ex. a {@link Extractors#STRING string},
 * {@link Extractors#LONG long} or {@link Extractors#intArray()}.
 *
 * An {@link Extractor} is mutable for the single purpose of ability to reuse its value instance. Consider extracting
 * a primitive int -
 *
 * Sub-interfaces and implementations can and should specify specific accessors for the purpose
 * of performance and less garbage, f.ex. where an IntExtractor could have an accessor method for
 * getting the extracted value as primitive int, to avoid auto-boxing which would arise from calling {@link #value()}.
 *
 * @see Extractors for a collection of very common extractors.
 */
public interface Extractor<T>
{
    void extract( char[] data, int offset, int length );

    T value();

    /**
     * @return string representation of what type of value of produces. Also used as key in {@link Extractors}.
     */
    @Override
    String toString();
}
