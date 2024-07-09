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
import static java.time.ZoneOffset.UTC;
import static org.neo4j.csv.reader.Readables.individualFiles;
import static org.neo4j.csv.reader.Readables.iterator;
import static org.neo4j.internal.batchimport.input.csv.CsvInput.idExtractor;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.neo4j.batchimport.api.input.Group;
import org.neo4j.batchimport.api.input.IdType;
import org.neo4j.collection.RawIterator;
import org.neo4j.csv.reader.CharReadable;
import org.neo4j.csv.reader.CharSeeker;
import org.neo4j.csv.reader.Configuration;
import org.neo4j.csv.reader.Extractor;
import org.neo4j.csv.reader.Extractors;
import org.neo4j.csv.reader.Mark;
import org.neo4j.function.Factory;
import org.neo4j.internal.batchimport.input.DuplicateHeaderException;
import org.neo4j.internal.batchimport.input.Groups;
import org.neo4j.internal.batchimport.input.HeaderException;
import org.neo4j.internal.batchimport.input.csv.Header.Entry;
import org.neo4j.internal.batchimport.input.csv.Header.Monitor;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.util.Preconditions;
import org.neo4j.values.storable.CSVHeaderInformation;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TemporalValue;
import org.neo4j.values.storable.Value;

/**
 * Provides common implementations of factories required by f.ex {@link CsvInput}.
 */
public class DataFactories {
    private static final Supplier<ZoneId> DEFAULT_TIME_ZONE = () -> UTC;

    private static final Set<String> POINT_VALUE_CSV_HEADER_TYPES = new HashSet<>(Arrays.asList("Point", "Point[]"));
    private static final Set<String> TEMPORAL_VALUE_CSV_HEADER_TYPES =
            new HashSet<>(Arrays.asList("Time", "Time[]", "DateTime", "DateTime[]"));

    private DataFactories() {}

    /**
     * Creates a {@link DataFactory} where data exists in multiple files. If the first line of the first file is a header,
     * E.g. {@link #defaultFormatNodeFileHeader()} can be used to extract that.
     *
     * @param decorator Decorator for this data.
     * @param charset {@link Charset} to read data in.
     * @param files the files making up the data.
     *
     * @return {@link DataFactory} that returns a {@link CharSeeker} over all the supplied {@code files}.
     */
    public static DataFactory data(final Decorator decorator, final Charset charset, final Path... files) {
        if (files.length == 0) {
            throw new IllegalArgumentException("No files specified");
        }

        return config -> new Data() {
            @Override
            public RawIterator<CharReadable, IOException> stream() {
                return individualFiles(charset, files);
            }

            @Override
            public Decorator decorator() {
                return decorator;
            }
        };
    }

    /**
     * @param decorator Decorator for this data.
     * @param readable we need to have this as a {@link Factory} since one data file may be opened and scanned
     * multiple times.
     * @return {@link DataFactory} that returns a {@link CharSeeker} over the supplied {@code readable}
     */
    public static DataFactory data(final Decorator decorator, final Supplier<CharReadable> readable) {
        return config -> new Data() {
            @Override
            public RawIterator<CharReadable, IOException> stream() {
                return iterator(reader -> reader, readable.get());
            }

            @Override
            public Decorator decorator() {
                return decorator;
            }
        };
    }

    /**
     * Header parser that will read header information, using the default node header format,
     * from the top of the data file.
     *
     * This header factory can be used even when the header exists in a separate file, if that file
     * is the first in the list of files supplied to {@link #data}.
     * @param defaultTimeZone A supplier of the time zone to be used for temporal values when not specified explicitly
     * @param normalizeTypes whether or not to normalize types.
     */
    public static Header.Factory defaultFormatNodeFileHeader(Supplier<ZoneId> defaultTimeZone, boolean normalizeTypes) {
        return new DefaultNodeFileHeaderParser(defaultTimeZone, normalizeTypes);
    }

    /**
     * Like {@link #defaultFormatNodeFileHeader(Supplier, boolean)}} with UTC as the default time zone.
     * @param normalizeTypes whether or not to normalize types.
     */
    public static Header.Factory defaultFormatNodeFileHeader(boolean normalizeTypes) {
        return defaultFormatNodeFileHeader(DEFAULT_TIME_ZONE, normalizeTypes);
    }

    /**
     * Like {@link #defaultFormatNodeFileHeader(boolean)}} with no normalization.
     */
    public static Header.Factory defaultFormatNodeFileHeader() {
        return defaultFormatNodeFileHeader(false);
    }

    /**
     * Header parser that will read header information, using the default relationship header format,
     * from the top of the data file.
     *
     * This header factory can be used even when the header exists in a separate file, if that file
     * is the first in the list of files supplied to {@link #data}.
     * @param defaultTimeZone A supplier of the time zone to be used for temporal values when not specified explicitly
     * @param normalizeTypes whether or not to normalize types.
     */
    public static Header.Factory defaultFormatRelationshipFileHeader(
            Supplier<ZoneId> defaultTimeZone, boolean normalizeTypes) {
        return new DefaultRelationshipFileHeaderParser(defaultTimeZone, normalizeTypes);
    }

    /**
     * Like {@link #defaultFormatRelationshipFileHeader(Supplier, boolean)} with UTC as the default time zone.
     * @param normalizeTypes whether or not to normalize types.
     */
    public static Header.Factory defaultFormatRelationshipFileHeader(boolean normalizeTypes) {
        return defaultFormatRelationshipFileHeader(DEFAULT_TIME_ZONE, normalizeTypes);
    }

    /**
     * Like {@link #defaultFormatRelationshipFileHeader(boolean)} with no normalization.
     */
    public static Header.Factory defaultFormatRelationshipFileHeader() {
        return defaultFormatRelationshipFileHeader(DEFAULT_TIME_ZONE, false);
    }

    public static Entry[] parseHeaderEntries(
            CharSeeker dataSeeker,
            Configuration config,
            IdType idType,
            Groups groups,
            Supplier<ZoneId> defaultTimeZone,
            HeaderEntryFactory entryFactory,
            Monitor monitor) {
        try {
            Mark mark = new Mark();
            Extractors extractors = new Extractors(
                    config.arrayDelimiter(), config.emptyQuotedStringsAsNull(), config.trimStrings(), defaultTimeZone);
            Extractor<?> idExtractor = idExtractor(idType, extractors);
            int delimiter = config.delimiter();
            List<Entry> columns = new ArrayList<>();
            for (int i = 0; !mark.isEndOfLine() && dataSeeker.seek(mark, delimiter); i++) {
                String rawEntry = dataSeeker.tryExtract(mark, extractors.string());
                HeaderEntrySpec spec = !extractors.string().isEmpty(rawEntry) ? parseHeaderEntrySpec(rawEntry) : null;
                if (spec == null || Type.IGNORE.name().equals(spec.type())) {
                    columns.add(new Entry(rawEntry, null, Type.IGNORE, null, null));
                } else {
                    columns.add(entryFactory.create(
                            dataSeeker.sourceDescription(), i, spec, extractors, idExtractor, groups, monitor));
                }
            }
            return columns.toArray(new Entry[0]);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private abstract static class AbstractDefaultFileHeaderParser implements Header.Factory, HeaderEntryFactory {
        private final Type[] mandatoryTypes;
        private final Supplier<ZoneId> defaultTimeZone;
        private final boolean normalizeTypes;

        AbstractDefaultFileHeaderParser(
                Supplier<ZoneId> defaultTimeZone, boolean normalizeTypes, Type... mandatoryTypes) {
            this.defaultTimeZone = defaultTimeZone;
            this.normalizeTypes = normalizeTypes;
            this.mandatoryTypes = mandatoryTypes;
        }

        @Override
        public Header create(
                CharSeeker dataSeeker, Configuration config, IdType idType, Groups groups, Monitor monitor) {
            Entry[] entries = parseHeaderEntries(dataSeeker, config, idType, groups, defaultTimeZone, this, monitor);
            validateHeader(entries, dataSeeker);
            return new Header(entries);
        }

        private void validateHeader(Entry[] entries, CharSeeker dataSeeker) {
            // This specific map exists to give a more specific exception for some cases
            Map<String, Entry> idProperties = new HashMap<>();
            Map<String, Entry> properties = new HashMap<>();
            EnumMap<Type, Entry> singletonEntries = new EnumMap<>(Type.class);
            for (Entry entry : entries) {
                switch (entry.type()) {
                    case ID, PROPERTY -> {
                        String propertyName = entry.name();
                        if (propertyName != null) {
                            if (entry.type() == Type.ID) {
                                Entry existingIdPropertyEntry = idProperties.put(propertyName, entry);
                                if (existingIdPropertyEntry != null) {
                                    throw new DuplicateHeaderException(
                                            existingIdPropertyEntry,
                                            entry,
                                            dataSeeker.sourceDescription(),
                                            "Cannot store composite IDs as properties, only individual part");
                                }
                            }

                            Entry existingPropertyEntry = properties.put(propertyName, entry);
                            if (existingPropertyEntry != null) {
                                throw new DuplicateHeaderException(
                                        existingPropertyEntry, entry, dataSeeker.sourceDescription());
                            }
                        }
                    }
                    case START_ID, END_ID, TYPE -> {
                        Entry existingSingletonEntry = singletonEntries.get(entry.type());
                        if (existingSingletonEntry != null) {
                            throw new DuplicateHeaderException(
                                    existingSingletonEntry, entry, dataSeeker.sourceDescription());
                        }
                        singletonEntries.put(entry.type(), entry);
                    }
                    default -> {}
                        // No need to validate other headers
                }
            }

            for (Type type : mandatoryTypes) {
                if (!singletonEntries.containsKey(type)) {
                    throw new HeaderException(
                            format("Missing header of type %s, among entries %s", type, Arrays.toString(entries)));
                }
            }
        }

        static boolean isRecognizedType(String typeSpec) {
            for (Type type : Type.values()) {
                if (type.name().equalsIgnoreCase(typeSpec)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public boolean isDefined() {
            return false;
        }

        Extractor<?> propertyExtractor(
                String sourceDescription, String name, String typeSpec, Extractors extractors, Monitor monitor) {
            Extractor<?> extractor = parsePropertyType(typeSpec, extractors);
            if (normalizeTypes) {
                // This basically mean that e.g. a specified type "float" will actually be "double", "int", "short" and
                // all that will be "long".
                String fromType = extractor.name();
                Extractor<?> normalized = extractor.normalize();
                if (!normalized.equals(extractor)) {
                    String toType = normalized.name();
                    monitor.typeNormalized(sourceDescription, name, fromType, toType);
                    return normalized;
                }
            }
            return extractor;
        }
    }

    private static HeaderEntrySpec parseHeaderEntrySpec(String rawEntry) {
        // rawEntry specification: <name><:type>(<group>){<options>}
        // example: id:ID(persons){option1:something,option2:'something else'}

        String rawHeaderField = rawEntry;
        String name;
        String type = null;
        String groupName = null;
        Map<String, String> options = new HashMap<>();

        // The options
        {
            int optionsStartIndex = rawHeaderField.indexOf('{');
            if (optionsStartIndex != -1) {
                int optionsEndIndex = rawHeaderField.lastIndexOf('}');
                Preconditions.checkState(
                        optionsEndIndex != -1 && optionsEndIndex > optionsStartIndex,
                        "Expected a closing '}' in header %s",
                        rawHeaderField);
                String rawOptions =
                        rawHeaderField.substring(optionsStartIndex, optionsEndIndex + 1); // including the curlies
                options = Value.parseStringMap(rawOptions);
                rawHeaderField = cutOut(rawHeaderField, optionsStartIndex, optionsEndIndex);
            }
        }

        // The group
        {
            int groupStartIndex = rawHeaderField.indexOf('(');
            if (groupStartIndex != -1) {
                int groupEndIndex = rawHeaderField.lastIndexOf(')');
                Preconditions.checkState(
                        groupEndIndex != -1 && groupEndIndex > groupStartIndex, "Expected a closing ')'");
                groupName = rawHeaderField.substring(groupStartIndex + 1, groupEndIndex);
                rawHeaderField = cutOut(rawHeaderField, groupStartIndex, groupEndIndex);
            }
        }

        // The type
        {
            int typeIndex = rawHeaderField.lastIndexOf(':');
            if (typeIndex != -1) {
                type = rawHeaderField.substring(typeIndex + 1);
                rawHeaderField = rawHeaderField.substring(0, typeIndex);
            }
        }

        // The name
        name = rawHeaderField.isEmpty() ? null : rawHeaderField;

        return new HeaderEntrySpec(rawEntry, name, type, groupName, options);
    }

    private static String cutOut(String string, int startIndex, int endIndex) {
        var result = new StringBuilder();
        if (startIndex > 0) {
            result.append(string, 0, startIndex);
        }
        if (endIndex + 1 < string.length()) {
            result.append(string.substring(endIndex + 1));
        }
        return result.toString();
    }

    record HeaderEntrySpec(String rawEntry, String name, String type, String group, Map<String, String> options) {}

    interface HeaderEntryFactory {
        Entry create(
                String sourceDescription,
                int entryIndex,
                HeaderEntrySpec spec,
                Extractors extractors,
                Extractor<?> idExtractor,
                Groups groups,
                Monitor monitor);
    }

    private static class DefaultNodeFileHeaderParser extends AbstractDefaultFileHeaderParser {
        DefaultNodeFileHeaderParser(Supplier<ZoneId> defaultTimeZone, boolean normalizeTypes) {
            super(defaultTimeZone, normalizeTypes);
        }

        @Override
        public Entry create(
                String sourceDescription,
                int entryIndex,
                HeaderEntrySpec spec,
                Extractors extractors,
                Extractor<?> defaultIdExtractor,
                Groups groups,
                Monitor monitor) {
            // For nodes it's simply ID,LABEL,PROPERTY. typeSpec can be either ID,LABEL or a type of property,
            // like 'int' or 'string_array' or similar, or empty for 'string' property.
            Type type;
            Extractor<?> extractor;
            CSVHeaderInformation optionalParameter = null;
            Group group = null;
            if (spec.type() == null) {
                type = Type.PROPERTY;
                extractor = extractors.string();
            } else {
                if (spec.type().equalsIgnoreCase(Type.ID.name())) {
                    type = Type.ID;
                    group = groups.getOrCreate(spec.group(), spec.options().get("id-type"));
                    extractor = group.specificIdType() != null
                            ? parsePropertyType(group.specificIdType(), extractors)
                            : defaultIdExtractor;
                } else if (spec.type().equalsIgnoreCase(Type.LABEL.name())) {
                    type = Type.LABEL;
                    extractor = extractors.stringArray();
                } else if (isRecognizedType(spec.type())) {
                    throw new HeaderException("Unexpected node header type '" + spec.type() + "'");
                } else {
                    type = Type.PROPERTY;
                    extractor = propertyExtractor(sourceDescription, spec.name(), spec.type(), extractors, monitor);
                    optionalParameter = parseOptionalParameter(extractor, spec.options());
                }
            }
            return new Header.Entry(
                    spec.rawEntry(), spec.name(), type, group, extractor, spec.options(), optionalParameter);
        }
    }

    private static class DefaultRelationshipFileHeaderParser extends AbstractDefaultFileHeaderParser {
        DefaultRelationshipFileHeaderParser(Supplier<ZoneId> defaultTimeZone, boolean normalizeTypes) {
            // Don't have TYPE as mandatory since a decorator could provide that
            super(defaultTimeZone, normalizeTypes, Type.START_ID, Type.END_ID);
        }

        @Override
        public Entry create(
                String sourceDescription,
                int entryIndex,
                HeaderEntrySpec spec,
                Extractors extractors,
                Extractor<?> defaultIdExtractor,
                Groups groups,
                Monitor monitor) {
            Type type;
            Extractor<?> extractor;
            CSVHeaderInformation optionalParameter = null;
            Group group = null;
            if (spec.type() == null) { // Property
                type = Type.PROPERTY;
                extractor = extractors.string();
            } else {
                if (spec.type().equalsIgnoreCase(Type.START_ID.name())
                        || spec.type().equalsIgnoreCase(Type.END_ID.name())) {
                    type = Type.valueOf(spec.type().toUpperCase(Locale.ROOT));
                    group = groups.get(spec.group());
                    extractor = group.specificIdType() != null
                            ? parsePropertyType(group.specificIdType(), extractors)
                            : defaultIdExtractor;
                } else if (spec.type().equalsIgnoreCase(Type.TYPE.name())) {
                    type = Type.TYPE;
                    extractor = extractors.string();
                } else if (isRecognizedType(spec.type())) {
                    throw new HeaderException("Unexpected relationship header type '" + spec.type() + "'");
                } else {
                    type = Type.PROPERTY;
                    extractor = propertyExtractor(sourceDescription, spec.name(), spec.type(), extractors, monitor);
                    optionalParameter = parseOptionalParameter(extractor, spec.options());
                }
            }
            return new Header.Entry(
                    spec.rawEntry(), spec.name(), type, group, extractor, spec.options(), optionalParameter);
        }
    }

    private static CSVHeaderInformation parseOptionalParameter(Extractor<?> extractor, Map<String, String> options) {
        if (!options.isEmpty()) {
            if (POINT_VALUE_CSV_HEADER_TYPES.contains(extractor.name())) {
                return PointValue.parseHeaderInformation(options);
            } else if (TEMPORAL_VALUE_CSV_HEADER_TYPES.contains(extractor.name())) {
                return TemporalValue.parseHeaderInformation(options);
            }
        }
        return null;
    }

    private static Extractor<?> parsePropertyType(String typeSpec, Extractors extractors) {
        try {
            return extractors.valueOf(typeSpec);
        } catch (IllegalArgumentException e) {
            throw new HeaderException("Unable to parse header, unknown property type '" + typeSpec + "'", e);
        }
    }

    public static Iterable<DataFactory> datas(DataFactory... factories) {
        return Iterables.iterable(factories);
    }
}
