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
import static java.util.Arrays.copyOf;
import static org.neo4j.csv.reader.Readables.individualFiles;
import static org.neo4j.internal.batchimport.input.csv.CsvInput.idExtractor;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.factory.Maps;
import org.eclipse.collections.api.factory.primitive.IntIntMaps;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.api.map.primitive.MutableIntIntMap;
import org.neo4j.batchimport.api.input.Group;
import org.neo4j.batchimport.api.input.IdType;
import org.neo4j.collection.RawIterator;
import org.neo4j.csv.reader.CharReadable;
import org.neo4j.csv.reader.CharReadableChunker.ChunkImpl;
import org.neo4j.csv.reader.CharSeeker;
import org.neo4j.csv.reader.Configuration;
import org.neo4j.csv.reader.Extractor;
import org.neo4j.csv.reader.Extractors;
import org.neo4j.csv.reader.Mark;
import org.neo4j.csv.reader.VectorExtractor;
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

    private static final Set<String> POINT_VALUE_CSV_HEADER_TYPES = new HashSet<>(Arrays.asList("point", "point[]"));
    private static final Set<String> TEMPORAL_VALUE_CSV_HEADER_TYPES =
            new HashSet<>(Arrays.asList("time", "time[]", "datetime", "datetime[]"));
    private static final String VECTOR_VALUE_CSV_HEADER_TYPE = "vector";

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

        return new DataFactory() {
            @Override
            public Data create(Configuration config) {
                return new Data() {
                    @Override
                    public RawIterator<CharReadable, IOException> stream() {
                        return individualFiles(config, charset, files);
                    }

                    @Override
                    public Decorator decorator() {
                        return decorator;
                    }
                };
            }

            @Override
            public Path[] files() {
                return files;
            }
        };
    }

    /**
     * Header parser that will read header information, using the default node header format,
     * from the top of the data file.
     * <br>
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
     * <br>
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

    public static List<String> parseRawHeaderEntries(
            String sourceDescription, Configuration config, Supplier<ZoneId> defaultTimeZone, char... data)
            throws IOException {
        var chunk = new ChunkImpl(copyOf(data, data.length + 1));
        chunk.initialize(0, data.length, sourceDescription, 0);

        try (var dataSeeker = CsvInputIterator.seeker(chunk, config)) {
            var mark = new Mark();
            var extractors = new Extractors(
                    config.arrayDelimiter(),
                    config.vectorDelimiter(),
                    config.emptyQuotedStringsAsNull(),
                    config.trimStrings(),
                    defaultTimeZone);
            var headers = Lists.mutable.<String>empty();
            while (!mark.isEndOfLine() && dataSeeker.seek(mark, config.delimiter())) {
                headers.add(dataSeeker.tryExtract(mark, extractors.string()));
            }
            return headers;
        }
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
                    config.arrayDelimiter(),
                    config.vectorDelimiter(),
                    config.emptyQuotedStringsAsNull(),
                    config.trimStrings(),
                    defaultTimeZone);
            Extractor<?> idExtractor = idExtractor(idType, extractors);
            int delimiter = config.delimiter();
            List<Entry> columns = new ArrayList<>();
            for (int i = 0; !mark.isEndOfLine() && dataSeeker.seek(mark, delimiter); i++) {
                String rawEntry = dataSeeker.tryExtract(mark, extractors.string());
                HeaderEntrySpec spec = !extractors.string().isEmpty(rawEntry)
                        ? parseHeaderEntrySpec(dataSeeker.sourceDescription(), rawEntry)
                        : null;
                if (spec == null || Type.IGNORE.name().equals(spec.type())) {
                    columns.add(new Entry(rawEntry, null, Type.IGNORE, null, null));
                } else if (Type.ACTION.name().equals(spec.type())) {
                    columns.add(new Entry(rawEntry, null, Type.ACTION, null, extractors.string()));
                } else {
                    columns.add(entryFactory.create(
                            dataSeeker.sourceDescription(), i, spec, extractors, idExtractor, groups, monitor));
                }
            }
            return columns.toArray(new Entry[0]);
        } catch (IOException ex) {
            throw new UncheckedIOException("Unable to parse header entries: " + dataSeeker.sourceDescription(), ex);
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

        @Override
        public Entry create(
                String sourceDescription,
                int entryIndex,
                HeaderEntrySpec spec,
                Extractors extractors,
                Extractor<?> idExtractor,
                Groups groups,
                Monitor monitor) {
            if (spec.type() == null) {
                return new Header.Entry(spec.rawEntry(), spec.name(), Type.PROPERTY, null, extractors.string());
            }

            var specificEntry =
                    createSpecific(sourceDescription, entryIndex, spec, extractors, idExtractor, groups, monitor);
            if (specificEntry != null) {
                return specificEntry;
            }

            Type type;
            Extractor<?> extractor;
            CSVHeaderInformation optionalParameter = null;
            Group group = null;
            if (Type.REMOVE_PROPERTY.matches(spec.type())) {
                type = Type.REMOVE_PROPERTY;
                if (spec.name() != null) {
                    extractor = extractors.string();
                } else {
                    extractor = extractors.stringArray();
                }
            } else if (isRecognizedType(spec.type())) {
                throw new HeaderException(
                        "Unexpected header type '%s' in file '%s'".formatted(spec.type(), sourceDescription));
            } else {
                type = Type.PROPERTY;
                try {
                    optionalParameter = parseOptionalParameter(spec.type(), spec.options());
                } catch (IllegalArgumentException e) {
                    throw new HeaderException(
                            "Unable to parse header in file '%s'. %s".formatted(sourceDescription, e.getMessage()), e);
                }
                extractor = propertyExtractor(
                        sourceDescription, spec.name(), spec.type(), optionalParameter, extractors, monitor);
            }
            return new Header.Entry(
                    spec.rawEntry(), spec.name(), type, group, extractor, spec.options(), optionalParameter);
        }

        protected abstract Entry createSpecific(
                String sourceDescription,
                int entryIndex,
                HeaderEntrySpec spec,
                Extractors extractors,
                Extractor<?> idExtractor,
                Groups groups,
                Monitor monitor);

        private void validateHeader(Entry[] entries, CharSeeker dataSeeker) {
            // This specific map exists to give a more specific exception for some cases
            Map<String, Entry> idProperties = new HashMap<>();
            Map<String, Entry> properties = new HashMap<>();
            EnumMap<Type, Entry> singletonEntries = new EnumMap<>(Type.class);
            EnumSet<Type> multiEntries = EnumSet.noneOf(Type.class);
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
                    case START_ID, END_ID ->
                        // No specific validation of these, and basically ignore their "property name"
                        // because that doesn't really mean anything here.
                        multiEntries.add(entry.type());
                    case TYPE -> {
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
                if (!singletonEntries.containsKey(type) && !multiEntries.contains(type)) {
                    throw new HeaderException(format(
                            "Missing header of type %s, among entries %s in '%s'",
                            type, Arrays.toString(entries), dataSeeker.sourceDescription()));
                }
            }
        }

        static boolean isRecognizedType(String typeSpec) {
            for (Type type : Type.values()) {
                if (type.matches(typeSpec)) {
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
                String sourceDescription,
                String name,
                String typeSpec,
                CSVHeaderInformation optionalParameter,
                Extractors extractors,
                Monitor monitor) {
            Extractor<?> extractor = parsePropertyType(sourceDescription, typeSpec, optionalParameter, extractors);
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

    private static HeaderEntrySpec parseHeaderEntrySpec(String sourceDescription, String rawEntry) {
        // rawEntry specification: <name><:type>(<group>){<options>}
        // example: id:ID(persons){option1:something,option2:'something else'}

        String rawHeaderField = rawEntry;
        String name;
        String type = null;
        String groupName = null;
        Map<String, String> options = new HashMap<>();

        // The options
        int optionsStartIndex = rawHeaderField.lastIndexOf('{');
        if (optionsStartIndex != -1) {
            int optionsEndIndex = rawHeaderField.lastIndexOf('}');
            Preconditions.checkState(
                    optionsEndIndex != -1 && optionsEndIndex > optionsStartIndex,
                    "Expected a closing '}' in header %s of '%s'",
                    rawHeaderField,
                    sourceDescription);
            String rawOptions =
                    rawHeaderField.substring(optionsStartIndex, optionsEndIndex + 1); // including the curlies
            options = Value.parseStringMap(rawOptions);
            rawHeaderField = cutOut(rawHeaderField, optionsStartIndex, optionsEndIndex);
        }

        int typeIndex = rawHeaderField.lastIndexOf(':');

        // The group
        int groupStartIndex = rawHeaderField.lastIndexOf('(');
        if (groupStartIndex != -1 && typeIndex != -1 && groupStartIndex > typeIndex) {
            int groupEndIndex = rawHeaderField.lastIndexOf(')');
            Preconditions.checkState(
                    groupEndIndex != -1 && groupEndIndex > groupStartIndex,
                    "Expected a closing ')' in header of '%s'",
                    sourceDescription);
            groupName = rawHeaderField.substring(groupStartIndex + 1, groupEndIndex);
            rawHeaderField = cutOut(rawHeaderField, groupStartIndex, groupEndIndex);
        }

        // The type
        if (typeIndex != -1) {
            type = rawHeaderField.substring(typeIndex + 1);
            rawHeaderField = rawHeaderField.substring(0, typeIndex);
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

    public record HeaderEntrySpec(
            String rawEntry, String name, String type, String group, Map<String, String> options) {}

    public interface HeaderEntryFactory {
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
        public Entry createSpecific(
                String sourceDescription,
                int entryIndex,
                HeaderEntrySpec spec,
                Extractors extractors,
                Extractor<?> defaultIdExtractor,
                Groups groups,
                Monitor monitor) {
            var name = spec.name();
            // For nodes it's simply ID,LABEL,PROPERTY. typeSpec can be either ID,LABEL or a type of property,
            // like 'int' or 'string_array' or similar, or empty for 'string' property.
            Type type;
            Extractor<?> extractor;
            Group group = null;
            if (Type.ID.matches(spec.type())) {
                type = Type.ID;
                var idType = spec.options().get("id-type");
                if (idType != null && VectorExtractor.COL_NAME.equals(idType.toUpperCase(Locale.ROOT))) {
                    throw new HeaderException("vector is not allowed as an id-type");
                }

                group = groups.getOrCreate(spec.group());
                groups.bindIdType(group, name, idType);
                extractor = (idType == null)
                        ? defaultIdExtractor
                        : parsePropertyType(sourceDescription, idType, null, extractors);
            } else if (Type.LABEL.matches(spec.type())) {
                type = Type.LABEL;
                extractor = extractors.stringArray();
            } else if (Type.REMOVE_LABEL.matches(spec.type())) {
                type = Type.REMOVE_LABEL;
                extractor = extractors.stringArray();
            } else {
                return null;
            }
            return new Header.Entry(spec.rawEntry(), name, type, group, extractor, spec.options(), null);
        }
    }

    private static class DefaultRelationshipFileHeaderParser extends AbstractDefaultFileHeaderParser {

        private final MutableMap<RelIdGroup, MutableIntIntMap> relIdGroupTypes = Maps.mutable.empty();

        DefaultRelationshipFileHeaderParser(Supplier<ZoneId> defaultTimeZone, boolean normalizeTypes) {
            // Don't have TYPE as mandatory since a decorator could provide that
            super(defaultTimeZone, normalizeTypes, Type.START_ID, Type.END_ID);
        }

        @Override
        public Header create(
                CharSeeker dataSeeker, Configuration config, IdType idType, Groups groups, Monitor monitor) {
            // Per-header state: idIndex is derived from column ordinal within this file, so reset before
            // each file. Otherwise stale indexes make getSpecificIdType(...) return null and the extractor
            // falls back to the global id-type instead of inheriting the id space's declared type.
            relIdGroupTypes.clear();
            return super.create(dataSeeker, config, idType, groups, monitor);
        }

        @Override
        public Entry createSpecific(
                String sourceDescription,
                int entryIndex,
                HeaderEntrySpec spec,
                Extractors extractors,
                Extractor<?> defaultIdExtractor,
                Groups groups,
                Monitor monitor) {
            Type type;
            Extractor<?> extractor;
            Group group = null;
            if (Type.START_ID.matches(spec.type()) || Type.END_ID.matches(spec.type())) {
                type = Type.START_ID.matches(spec.type()) ? Type.START_ID : Type.END_ID;
                group = groups.get(spec.group());

                // Here we don't need to protect against vector as an id-type, wince we just read
                // existing groups, we don't create new groups.
                var entryIndexIdTypes = relIdGroupTypes.getIfAbsentPut(
                        new RelIdGroup(group, type == Type.START_ID), IntIntMaps.mutable::empty);
                var idIndex = entryIndexIdTypes.getIfAbsentPut(entryIndex, entryIndexIdTypes::size);
                var specificIdType = groups.getSpecificIdType(group, idIndex);
                extractor = (specificIdType == null)
                        ? defaultIdExtractor
                        : parsePropertyType(sourceDescription, specificIdType, null, extractors);
            } else if (Type.TYPE.matches(spec.type())) {
                type = Type.TYPE;
                extractor = extractors.string();
            } else {
                return null;
            }
            return new Header.Entry(spec.rawEntry(), spec.name(), type, group, extractor, spec.options(), null);
        }

        private record RelIdGroup(Group group, boolean isStart) {}
    }

    private static CSVHeaderInformation parseOptionalParameter(String typeSpec, Map<String, String> options) {
        final var typeSpecLowerCase = typeSpec.toLowerCase(Locale.ROOT);
        if (!options.isEmpty()) {
            if (POINT_VALUE_CSV_HEADER_TYPES.contains(typeSpecLowerCase)) {
                return PointValue.parseHeaderInformation(options);
            } else if (TEMPORAL_VALUE_CSV_HEADER_TYPES.contains(typeSpecLowerCase)) {
                return TemporalValue.parseHeaderInformation(options);
            } else if (VECTOR_VALUE_CSV_HEADER_TYPE.equals(typeSpecLowerCase)) {
                return VectorExtractor.parseHeaderInformation(options);
            }
        }
        return null;
    }

    private static Extractor<?> parsePropertyType(
            String sourceDescription, String typeSpec, CSVHeaderInformation optionalParameter, Extractors extractors) {
        try {
            return extractors.valueOf(typeSpec, optionalParameter);
        } catch (IllegalArgumentException e) {
            throw new HeaderException(
                    "Unable to parse header in '%s'. %s".formatted(sourceDescription, e.getMessage()), e);
        }
    }

    public static Iterable<DataFactory> datas(DataFactory... factories) {
        return Iterables.iterable(factories);
    }
}
