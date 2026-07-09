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

import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.io.ByteUnit.mebiBytes;

import java.util.function.Predicate;
import org.neo4j.function.Predicates;

/**
 * Configuration options around reading CSV data, or similar.
 */
public class Configuration {
    public static final Configuration COMMAS = newBuilder()
            .withDelimiter(',')
            .withArrayDelimiter(';')
            .withVectorDelimiter(';')
            .build();

    public static final Configuration TABS = newBuilder()
            .withDelimiter('\t')
            .withArrayDelimiter(',')
            .withVectorDelimiter(',')
            .build();

    public static final boolean DEFAULT_LEGACY_STYLE_QUOTING = false;

    public static final boolean DEFAULT_READ_IS_FOR_SAMPLING = false;

    private final char quotationCharacter;
    private final char delimiter;
    private final char arrayDelimiter;
    private final char vectorDelimiter;
    private final int bufferSize;
    private final Predicate<String> multilineDocuments;
    private final boolean legacyMultilineFields;
    private final boolean trimStrings;
    private final boolean emptyQuotedStringsAsNull;
    private final boolean legacyStyleQuoting;
    private final boolean readIsForSampling;

    private Configuration(Builder b) {
        this.quotationCharacter = b.quotationCharacter;
        this.delimiter = b.delimiter;
        this.arrayDelimiter = b.arrayDelimiter;
        this.vectorDelimiter = b.vectorDelimiter;
        this.bufferSize = b.bufferSize;
        this.multilineDocuments = b.multilineDocuments;
        this.legacyMultilineFields = b.legacyMultilineFields;
        this.trimStrings = b.trimStrings;
        this.emptyQuotedStringsAsNull = b.emptyQuotedStringsAsNull;
        this.legacyStyleQuoting = b.legacyStyleQuoting;
        this.readIsForSampling = b.readIsForSampling;
    }

    public char quotationCharacter() {
        return quotationCharacter;
    }

    public char delimiter() {
        return delimiter;
    }

    public char arrayDelimiter() {
        return arrayDelimiter;
    }

    public char vectorDelimiter() {
        return vectorDelimiter;
    }

    /**
     * Data buffer size.
     */
    public int bufferSize() {
        return bufferSize;
    }

    /**
     * Whether or not fields for a specific source are allowed to have newline characters in them, i.e. span multiple lines.
     */
    public Predicate<String> multilineDocuments() {
        return multilineDocuments;
    }

    /**
     * Whether or not strings should be trimmed for whitespaces.
     */
    public boolean trimStrings() {
        return trimStrings;
    }

    /**
     * @return {@code true} for treating empty strings, i.e. {@code ""} as null, instead of an empty string.
     */
    public boolean emptyQuotedStringsAsNull() {
        return emptyQuotedStringsAsNull;
    }

    /**
     * Adds a default implementation returning {@link #DEFAULT_LEGACY_STYLE_QUOTING}, this to not requiring
     * any change to other classes using this interface.
     *
     * @return whether or not the parsing will interpret <code>\"</code> (see {@link #quotationCharacter()})
     * as an inner quote. Reason why this is configurable is that this interpretation conflicts with
     * "standard" RFC for CSV parsing, see <a href="https://tools.ietf.org/html/rfc4180">RFC4180</a>. This also makes
     * it impossible to enter some combinations of characters, e.g. <code>"""abc\"""</code>, when expecting <code>"abc\"</code>.
     */
    public boolean legacyStyleQuoting() {
        return legacyStyleQuoting;
    }

    /**
     * Whether or not fields are allowed to have newline characters in them, i.e. span multiple lines. This is applied to
     * all source documents, irrespective of whether the source in question has any multiline fields ot not.
     */
    public boolean legacyMultilineFields() {
        return legacyMultilineFields;
    }

    /**
     * @return {@code true} when the expected read behaviour is to only sample some initial fraction of the data
     */
    public boolean readIsForSampling() {
        return readIsForSampling;
    }

    @Override
    public String toString() {
        return "Configuration{ delimiter="
                + delimiter
                + " arrayDelimiter="
                + arrayDelimiter
                + " vectorDelimiter="
                + vectorDelimiter
                + " quotationCharacter="
                + quotationCharacter
                + " bufferSize="
                + bufferSize
                + " legacyMultilineFields="
                + legacyMultilineFields
                + " trimStrings="
                + trimStrings
                + " emptyQuotedStringsAsNull="
                + emptyQuotedStringsAsNull
                + " legacyStyleQuoting="
                + legacyStyleQuoting
                + " readIsForSampling="
                + readIsForSampling
                + " }";
    }

    public Builder toBuilder() {
        final var builder = new Builder()
                .withQuotationCharacter(quotationCharacter)
                .withDelimiter(delimiter)
                .withArrayDelimiter(arrayDelimiter)
                .withVectorDelimiter(vectorDelimiter)
                .withBufferSize(bufferSize)
                .withTrimStrings(trimStrings)
                .withEmptyQuotedStringsAsNull(emptyQuotedStringsAsNull)
                .withLegacyStyleQuoting(legacyStyleQuoting)
                .withReadIsForSampling(readIsForSampling);
        if (legacyMultilineFields) {
            return builder.withLegacyMultilineBehaviour();
        } else {
            return builder.withMultilineDocuments(multilineDocuments);
        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        public static final int DEFAULT_BUFFER_SIZE_IF_SKIDBLADNIR = (int) kibiBytes(64);

        private char quotationCharacter = '"';
        private char delimiter = ',';
        private char arrayDelimiter = ';';
        private char vectorDelimiter = ';';
        private int bufferSize = (int) mebiBytes(4);
        private boolean legacyMultilineFields;
        private Predicate<String> multilineDocuments = Predicates.alwaysFalse();
        private boolean trimStrings;
        private boolean emptyQuotedStringsAsNull;
        private boolean legacyStyleQuoting = DEFAULT_LEGACY_STYLE_QUOTING;
        private boolean readIsForSampling = DEFAULT_READ_IS_FOR_SAMPLING;

        public Builder withQuotationCharacter(char quotationCharacter) {
            this.quotationCharacter = quotationCharacter;
            return this;
        }

        public Builder withDelimiter(char delimiter) {
            this.delimiter = delimiter;
            return this;
        }

        public Builder withArrayDelimiter(char arrayDelimiter) {
            this.arrayDelimiter = arrayDelimiter;
            return this;
        }

        public Builder withVectorDelimiter(char vectorDelimiter) {
            this.vectorDelimiter = vectorDelimiter;
            return this;
        }

        public Builder withBufferSize(int bufferSize) {
            this.bufferSize = bufferSize;
            return this;
        }

        public Builder withLegacyMultilineBehaviour() {
            this.legacyMultilineFields = true;
            this.multilineDocuments = Predicates.alwaysFalse();
            return this;
        }

        public Builder withMultilineDocuments(Predicate<String> multilineDocuments) {
            this.legacyMultilineFields = false;
            this.multilineDocuments = multilineDocuments;
            return this;
        }

        public Builder withTrimStrings(boolean trimStrings) {
            this.trimStrings = trimStrings;
            return this;
        }

        public Builder withEmptyQuotedStringsAsNull(boolean emptyQuotedStringsAsNull) {
            this.emptyQuotedStringsAsNull = emptyQuotedStringsAsNull;
            return this;
        }

        public Builder withLegacyStyleQuoting(boolean legacyStyleQuoting) {
            this.legacyStyleQuoting = legacyStyleQuoting;
            return this;
        }

        public Builder withReadIsForSampling(boolean readIsForSampling) {
            this.readIsForSampling = readIsForSampling;
            return this;
        }

        public Configuration build() {
            return new Configuration(this);
        }
    }
}
