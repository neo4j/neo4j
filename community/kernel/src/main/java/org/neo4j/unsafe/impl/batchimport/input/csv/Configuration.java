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
package org.neo4j.unsafe.impl.batchimport.input.csv;

/**
 * Configuration for {@link CsvInput}.
 */
public interface Configuration extends org.neo4j.csv.reader.Configuration
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

    abstract class Default extends org.neo4j.csv.reader.Configuration.Default implements Configuration
    {
    }

    Configuration COMMAS = new Default()
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

    Configuration TABS = new Default()
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

    class Overriden extends org.neo4j.csv.reader.Configuration.Overridden implements Configuration
    {
        private final Configuration defaults;

        public Overriden( Configuration defaults )
        {
            super( defaults );
            this.defaults = defaults;
        }

        @Override
        public char delimiter()
        {
            return defaults.delimiter();
        }

        @Override
        public char arrayDelimiter()
        {
            return defaults.arrayDelimiter();
        }
    }
}
