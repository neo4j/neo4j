/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * Provides information about a source of data.
 *
 * An example usage would be reading a text file where {@link #sourceDescription()} would say the name of the file,
 * and {@link #position()} the byte offset the reader is currently at.
 *
 * Another example could be reading from a relationship db table where {@link #sourceDescription()} would
 * say the name of the database and table and {@link #position()} some sort of absolute position saying
 * the byte offset to the field.
 */
public interface SourceTraceability
{
    /**
     * @return description of the source being read from.
     */
    String sourceDescription();

    /**
     * @return a low-level byte-like position e.g. byte offset. This position is an "uncompressed" position.
     */
    long position();

    /**
     * @return the observed compression ratio to use when compensating between situations where reported data source length
     * is a compressed length and {@link #position()} is decompressed position. Usually this value is 1.0, which means that
     * source length and position are aligned, but in some cases e.g. for GZIP this isn't efficiently possible and therefore
     * this ratio can be used like so:
     *
     * <pre>
     * actualSourceLength = position() / compressionRatio()
     * </pre>
     */
    default float compressionRatio()
    {
        return 1f;
    }

    abstract class Adapter implements SourceTraceability
    {
        @Override
        public long position()
        {
            return 0;
        }
    }

    class Empty extends Adapter
    {
        @Override
        public String sourceDescription()
        {
            return "EMPTY";
        }
    }

    SourceTraceability EMPTY = new Empty();
}
