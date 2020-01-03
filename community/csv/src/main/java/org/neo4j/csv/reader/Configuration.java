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

import static org.neo4j.io.ByteUnit.mebiBytes;

/**
 * Configuration options around reading CSV data, or similar.
 */
public class Configuration
{
    public static final Configuration COMMAS = newBuilder()
            .withDelimiter( ',' )
            .withArrayDelimiter( ';' )
            .build();

    public static final Configuration TABS = newBuilder()
            .withDelimiter( '\t' )
            .withArrayDelimiter( ',' )
            .build();

    public static final boolean DEFAULT_LEGACY_STYLE_QUOTING = false;

    private final char quotationCharacter;
    private final char delimiter;
    private final char arrayDelimiter;
    private final int bufferSize;
    private final boolean multilineFields;
    private final boolean trimStrings;
    private final boolean emptyQuotedStringsAsNull;
    private final boolean legacyStyleQuoting;

    private Configuration( Builder b )
    {
        this.quotationCharacter = b.quotationCharacter;
        this.delimiter = b.delimiter;
        this.arrayDelimiter = b.arrayDelimiter;
        this.bufferSize = b.bufferSize;
        this.multilineFields = b.multilineFields;
        this.trimStrings = b.trimStrings;
        this.emptyQuotedStringsAsNull = b.emptyQuotedStringsAsNull;
        this.legacyStyleQuoting = b.legacyStyleQuoting;
    }

    public char quotationCharacter()
    {
        return quotationCharacter;
    }

    public char delimiter()
    {
        return delimiter;
    }

    public char arrayDelimiter()
    {
        return arrayDelimiter;
    }

    /**
     * Data buffer size.
     */
    public int bufferSize()
    {
        return bufferSize;
    }

    /**
     * Whether or not fields are allowed to have newline characters in them, i.e. span multiple lines.
     */
    public boolean multilineFields()
    {
        return multilineFields;
    }

    /**
     * Whether or not strings should be trimmed for whitespaces.
     */
    public boolean trimStrings()
    {
        return trimStrings;
    }

    /**
     * @return {@code true} for treating empty strings, i.e. {@code ""} as null, instead of an empty string.
     */
    public boolean emptyQuotedStringsAsNull()
    {
        return emptyQuotedStringsAsNull;
    }

    /**
     * Adds a default implementation returning {@link #DEFAULT_LEGACY_STYLE_QUOTING}, this to not requiring
     * any change to other classes using this interface.
     *
     * @return whether or not the parsing will interpret <code>\"</code> (see {@link #quotationCharacter()})
     * as an inner quote. Reason why this is configurable is that this interpretation conflicts with
     * "standard" RFC for CSV parsing, see https://tools.ietf.org/html/rfc4180. This also makes it impossible
     * to enter some combinations of characters, e.g. <code>"""abc\"""</code>, when expecting <code>"abc\"</code>.
     */
    public boolean legacyStyleQuoting()
    {
        return legacyStyleQuoting;
    }

    public Builder toBuilder()
    {
        return new Builder()
                .withQuotationCharacter( quotationCharacter )
                .withDelimiter( delimiter )
                .withArrayDelimiter( arrayDelimiter )
                .withBufferSize( bufferSize )
                .withMultilineFields( multilineFields )
                .withTrimStrings( trimStrings )
                .withEmptyQuotedStringsAsNull( emptyQuotedStringsAsNull )
                .withLegacyStyleQuoting( legacyStyleQuoting );
    }

    public static Builder newBuilder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private char quotationCharacter = '"';
        private char delimiter = ',';
        private char arrayDelimiter = ';';
        private int bufferSize = (int) mebiBytes( 4 );
        private boolean multilineFields;
        private boolean trimStrings;
        private boolean emptyQuotedStringsAsNull;
        private boolean legacyStyleQuoting = DEFAULT_LEGACY_STYLE_QUOTING;

        public Builder withQuotationCharacter( char quotationCharacter )
        {
            this.quotationCharacter = quotationCharacter;
            return this;
        }

        public Builder withDelimiter( char delimiter )
        {
            this.delimiter = delimiter;
            return this;
        }

        public Builder withArrayDelimiter( char arrayDelimiter )
        {
            this.arrayDelimiter = arrayDelimiter;
            return this;
        }

        public Builder withBufferSize( int bufferSize )
        {
            this.bufferSize = bufferSize;
            return this;
        }

        public Builder withMultilineFields( boolean multilineFields )
        {
            this.multilineFields = multilineFields;
            return this;
        }

        public Builder withTrimStrings( boolean trimStrings )
        {
            this.trimStrings = trimStrings;
            return this;
        }

        public Builder withEmptyQuotedStringsAsNull( boolean emptyQuotedStringsAsNull )
        {
            this.emptyQuotedStringsAsNull = emptyQuotedStringsAsNull;
            return this;
        }

        public Builder withLegacyStyleQuoting( boolean legacyStyleQuoting )
        {
            this.legacyStyleQuoting = legacyStyleQuoting;
            return this;
        }

        public Configuration build()
        {
            return new Configuration( this );
        }
    }
}
