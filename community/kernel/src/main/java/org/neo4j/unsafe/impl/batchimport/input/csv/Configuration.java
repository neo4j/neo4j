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
package org.neo4j.unsafe.impl.batchimport.input.csv;

/**
 * Configuration for {@link CsvInput}.
 */
public interface Configuration
{
    /**
     * Delimiting character between each values in a CSV input line.
     * Typical character is '\t' (TAB) or ',' (it is Comma Separated Values after all).
     */
    char delimiter();

    /**
     * Character separating array values from one another for values that represent arrays.
     */
    char arrayDelimiter();

    /**
     * Whether or not the data parser is quote aware. If it is then {@link #quotationCharacter()} is
     * used as the quote character.
     */
    boolean quoteAware();

    /**
     * Character to regard as quotes. Quoted values can contain newline characters and even delimiters.
     */
    char quotationCharacter();

    public static abstract class Default implements Configuration
    {
        @Override
        public boolean quoteAware()
        {
            return true;
        }

        @Override
        public char quotationCharacter()
        {
            return '"';
        }
    }

    public static final Configuration COMMAS = new Default()
    {
        @Override
        public char delimiter()
        {
            return ',';
        }

        @Override
        public char arrayDelimiter()
        {
            return ';';
        }
    };

    public static final Configuration TABS = new Default()
    {
        @Override
        public char delimiter()
        {
            return '\t';
        }

        @Override
        public char arrayDelimiter()
        {
            return ',';
        }
    };
}
