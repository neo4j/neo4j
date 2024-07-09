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
package org.neo4j.internal.batchimport.input.csv;

import static java.lang.String.format;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import org.neo4j.batchimport.api.input.Group;
import org.neo4j.batchimport.api.input.IdType;
import org.neo4j.csv.reader.CharSeeker;
import org.neo4j.csv.reader.Configuration;
import org.neo4j.csv.reader.Extractor;
import org.neo4j.internal.batchimport.input.Groups;
import org.neo4j.values.storable.CSVHeaderInformation;

/**
 * Header of tabular/csv data input, specifying meta data about values in each "column", for example
 * semantically of which {@link Type} they are and which {@link Extractor type of value} they are.
 */
public class Header {
    public interface Factory {
        default Header create(CharSeeker dataSeeker, Configuration configuration, IdType idType, Groups groups) {
            return create(dataSeeker, configuration, idType, groups, NO_MONITOR);
        }

        /**
         * @param dataSeeker {@link CharSeeker} containing the data. Usually there's a header for us
         * to read at the very top of it.
         * @param configuration {@link Configuration} specific to the format of the data.
         * @param idType type of values we expect the ids to be.
         * @param groups {@link Groups} to register groups in.
         * @return the created {@link Header}.
         */
        Header create(
                CharSeeker dataSeeker, Configuration configuration, IdType idType, Groups groups, Monitor monitor);

        /**
         * @return whether or not this header is already defined. If this returns {@code false} then the header
         * will be read from the top of the data stream.
         */
        boolean isDefined();
    }

    private final Entry[] entries;

    public Header(Entry... entries) {
        this.entries = entries;
    }

    public Entry[] entries() {
        return entries;
    }

    @Override
    public String toString() {
        return Arrays.toString(entries);
    }

    public record Entry(
            String rawEntry,
            String name,
            Type type,
            Group group,
            Extractor<?> extractor,
            Map<String, String> rawOptions,
            // This can be used to encapsulate the parameters set in the header for spatial and temporal columns
            // It's a more optimized, or 'compiled' version of the rawOptions
            CSVHeaderInformation optionalParameter) {
        public Entry(String name, Type type, Group group, Extractor<?> extractor) {
            this(null, name, type, group, extractor);
        }

        public Entry(String rawEntry, String name, Type type, Group group, Extractor<?> extractor) {
            this(rawEntry, name, type, group, extractor, Collections.emptyMap(), null);
        }

        @Override
        public String toString() {
            if (rawEntry != null) {
                return rawEntry;
            }
            return format(
                    "Entry[name:%s, type:%s, group:%s, options:%s, optionalParameter:%s]",
                    name, type, group, rawOptions, optionalParameter);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Entry entry = (Entry) o;
            return Objects.equals(name, entry.name)
                    && type == entry.type
                    && Objects.equals(group, entry.group)
                    && Objects.equals(extractor, entry.extractor)
                    && rawOptions.equals(entry.rawOptions)
                    && Objects.equals(optionalParameter, entry.optionalParameter);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, type, group, extractor, rawOptions, optionalParameter);
        }
    }

    public interface Monitor {
        /**
         * Notifies that a type has been normalized.
         *
         * @param sourceDescription description of source file or stream that the header entry is defined in.
         * @param header name of the header entry.
         * @param fromType the type specified in the header in the source.
         * @param toType the type which will be used instead of the specified type.
         */
        void typeNormalized(String sourceDescription, String header, String fromType, String toType);
    }

    public static final Monitor NO_MONITOR = (source, header, from, to) -> {};

    public static class PrintingMonitor implements Monitor {
        private final PrintStream out;
        private boolean first = true;

        PrintingMonitor(PrintStream out) {
            this.out = out;
        }

        @Override
        public void typeNormalized(String sourceDescription, String name, String fromType, String toType) {
            if (first) {
                out.println("Cypher type normalization is enabled (disable with --normalize-types=false):");
                first = false;
            }

            out.printf(
                    "  Property type of '%s' normalized from '%s' --> '%s' in %s%n",
                    name, fromType, toType, sourceDescription);
        }
    }
}
