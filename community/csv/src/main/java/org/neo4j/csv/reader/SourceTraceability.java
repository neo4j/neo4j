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

/**
 * Provides information about a source of data.
 *
 * An example usage would be reading a text file where {@link #sourceDescription()} would say the name of the file,
 * {@link #lineNumber()} the line number and {@link #position()} the byte position the reader is currently at.
 *
 * Another example could be reading from a relationship db table where {@link #sourceDescription()} would
 * say the name of the database and table, or similar, {@link #lineNumber()} the ordinal of the row we're
 * currently at and {@link #position()} some sort of absolute position saying how many bytes we've read from the
 * data source.
 */
public interface SourceTraceability
{
    /**
     * @return description of the source being read from.
     */
    String sourceDescription();

    /**
     * 1-based line number of the current data source.
     *
     * @return current line number in the current data source.
     */
    long lineNumber();

    /**
     * @return a low-level byte-like position of f.ex. total number of read bytes.
     */
    long position();

    public static abstract class Adapter implements SourceTraceability
    {
        @Override
        public long lineNumber()
        {
            return 1;
        }

        @Override
        public long position()
        {
            return 0;
        }
    }
}
