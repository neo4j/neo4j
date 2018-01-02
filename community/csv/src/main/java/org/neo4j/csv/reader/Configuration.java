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
 * Configuration options around reading CSV data, or similar.
 */
public interface Configuration
{
    /**
     * Character to regard as quotes. Quoted values can contain newline characters and even delimiters.
     */
    char quotationCharacter();

    /**
     * Data buffer size.
     */
    int bufferSize();

    /**
     * Whether or not fields are allowed to have newline characters in them, i.e. span multiple lines.
     */
    boolean multilineFields();

    /**
     * @return {@code true} for treating empty strings, i.e. {@code ""} as null, instead of an empty string.
     */
    boolean emptyQuotedStringsAsNull();

    static int KB = 1024, MB = KB * KB;

    class Default implements Configuration
    {
        @Override
        public char quotationCharacter()
        {
            return '"';
        }

        @Override
        public int bufferSize()
        {
            return 4 * MB;
        }

        @Override
        public boolean multilineFields()
        {
            return false;
        }

        @Override
        public boolean emptyQuotedStringsAsNull()
        {
            return false;
        }
    }

    Configuration DEFAULT = new Default();

    class Overridden implements Configuration
    {
        private final Configuration defaults;

        public Overridden( Configuration defaults )
        {
            this.defaults = defaults;
        }

        @Override
        public char quotationCharacter()
        {
            return defaults.quotationCharacter();
        }

        @Override
        public int bufferSize()
        {
            return defaults.bufferSize();
        }

        @Override
        public boolean multilineFields()
        {
            return defaults.multilineFields();
        }

        @Override
        public boolean emptyQuotedStringsAsNull()
        {
            return defaults.emptyQuotedStringsAsNull();
        }
    }
}
