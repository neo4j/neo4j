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
package org.neo4j.importer;

import static java.lang.String.format;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toCollection;
import static org.eclipse.collections.impl.tuple.Tuples.pair;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.SequencedSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.eclipse.collections.api.tuple.Pair;
import org.neo4j.batchimport.api.input.IdType;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction.PatternStyle;
import org.neo4j.kernel.impl.util.Converters;
import org.neo4j.util.VisibleForTesting;
import picocli.CommandLine.ITypeConverter;
import picocli.CommandLine.TypeConversionException;

public final class ImportConverters {
    /**
     * Delimiter used between files in an input group.
     */
    private static final String MULTI_FILE_DELIMITER = ",";

    private static final Function<String, Character> CHARACTER_CONVERTER = new CharacterConverter();

    private ImportConverters() {}

    public abstract static class InputFilesGroup<T> {
        public final T key;
        public final String files;

        InputFilesGroup(T key, String files) {
            this.key = key;
            this.files = files;
        }

        public Path[] toPathArray(FileSystemAbstraction fs, PatternStyle patternStyle) {
            return parseFilesList(fs, files, patternStyle);
        }
    }

    public static class NodeFilesGroup extends InputFilesGroup<SequencedSet<String>> {
        NodeFilesGroup(SequencedSet<String> key, String files) {
            super(key, files);
        }
    }

    public static class RelationshipFilesGroup extends InputFilesGroup<String> {
        RelationshipFilesGroup(String key, String files) {
            super(key, files);
        }
    }

    public static class EscapedCharacterConverter implements ITypeConverter<Character> {
        @Override
        public Character convert(String value) {
            return CHARACTER_CONVERTER.apply(value);
        }
    }

    public static class NodeFilesConverter implements ITypeConverter<NodeFilesGroup> {
        @Override
        public NodeFilesGroup convert(String value) {
            try {
                return parseNodeFilesGroup(value);
            } catch (Exception e) {
                throw new TypeConversionException(format("Invalid nodes file: %s (%s)", value, e));
            }
        }
    }

    public static class RelationshipFilesConverter implements ITypeConverter<RelationshipFilesGroup> {
        @Override
        public RelationshipFilesGroup convert(String value) {
            try {
                return parseRelationshipFilesGroup(value);
            } catch (Exception e) {
                throw new TypeConversionException(format("Invalid relationships file: %s (%s)", value, e));
            }
        }
    }

    public static class IdTypeConverter implements ITypeConverter<IdType> {
        @Override
        public IdType convert(String in) {
            try {
                return IdType.valueOf(in.toUpperCase(Locale.ROOT));
            } catch (Exception e) {
                throw new TypeConversionException(format("Invalid id type: %s (%s)", in, e));
            }
        }
    }

    public static class FileInputTypeConverter implements ITypeConverter<FileImporter.FileInputType> {
        @Override
        public FileImporter.FileInputType convert(String in) {
            try {
                return FileImporter.FileInputType.valueOf(in.toUpperCase(Locale.ROOT));
            } catch (Exception e) {
                throw new TypeConversionException(format(
                        "Invalid file import type: %s. Only %s are supported as values",
                        in,
                        Arrays.stream(FileImporter.FileInputType.values())
                                .map(value -> String.format("'%s'", value.name().toLowerCase(Locale.ROOT)))
                                .collect(Collectors.joining(", "))));
            }
        }
    }

    @VisibleForTesting
    static NodeFilesGroup parseNodeFilesGroup(String str) {
        final var p = parseInputFilesGroup(str, s -> {
            if (s == null) {
                return new LinkedHashSet<String>();
            }
            // The same import command must always apply the additional labels in the same order, so that their
            // tokens are created in the same order too, therefore a LinkedHashSet
            return stream(s.split(":"))
                    .map(String::trim)
                    .filter(x -> !x.isEmpty())
                    .collect(toCollection(LinkedHashSet::new));
        });
        return new NodeFilesGroup(p.getOne(), p.getTwo());
    }

    @VisibleForTesting
    static RelationshipFilesGroup parseRelationshipFilesGroup(String str) {
        final var p = parseInputFilesGroup(str, s -> s == null ? null : s.trim());
        return new RelationshipFilesGroup(p.getOne(), p.getTwo());
    }

    public static Path[] parseFilesList(FileSystemAbstraction fs, String str, PatternStyle patternStyle) {
        return Converters.toFiles(MULTI_FILE_DELIMITER, Converters.patternMatchFiles(fs, true, patternStyle))
                .apply(str);
    }

    private static <T> Pair<T, String> parseInputFilesGroup(String str, Function<String, ? extends T> keyParser) {
        final var i = str.indexOf('=');
        if (i < 0) {
            return pair(keyParser.apply(null), str);
        }
        if (i == 0 || i == str.length() - 1) {
            throw new IllegalArgumentException("illegal `=` position: " + str);
        }
        final var keyStr = str.substring(0, i);
        return pair(keyParser.apply(keyStr), str.substring(i + 1));
    }
}
