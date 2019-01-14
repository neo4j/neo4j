/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
 * Configuration options around reading CSV data, or similar.
 */
public interface Configuration
{
    /**
     * TODO: Our intention is to flip this to false (which means to comply with RFC4180) at some point
     * because of how it better complies with common expectancy of behavior. It may be least disruptive
     * to do this when changing major version of the product.
     */
    boolean DEFAULT_LEGACY_STYLE_QUOTING = true;

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
     * Whether or not strings should be trimmed for whitespaces.
     */
    boolean trimStrings();

    /**
     * @return {@code true} for treating empty strings, i.e. {@code ""} as null, instead of an empty string.
     */
    boolean emptyQuotedStringsAsNull();

    /**
     * Adds a default implementation returning {@link #DEFAULT_LEGACY_STYLE_QUOTING}, this to not requiring
     * any change to other classes using this interface.
     *
     * @return whether or not the parsing will interpret <code>\"</code> (see {@link #quotationCharacter()})
     * as an inner quote. Reason why this is configurable is that this interpretation conflicts with
     * "standard" RFC for CSV parsing, see https://tools.ietf.org/html/rfc4180. This also makes it impossible
     * to enter some combinations of characters, e.g. <code>"""abc\"""</code>, when expecting <code>"abc\"</code>.
     */
    default boolean legacyStyleQuoting()
    {
        return DEFAULT_LEGACY_STYLE_QUOTING;
    }
    int KB = 1024;
    int MB = KB * KB;
    int DEFAULT_BUFFER_SIZE_4MB = 4 * MB;

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
            return DEFAULT_BUFFER_SIZE_4MB;
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

        @Override
        public boolean trimStrings()
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

        @Override
        public boolean trimStrings()
        {
            return defaults.trimStrings();
        }

        @Override
        public boolean legacyStyleQuoting()
        {
            return defaults.legacyStyleQuoting();
        }
    }
}
