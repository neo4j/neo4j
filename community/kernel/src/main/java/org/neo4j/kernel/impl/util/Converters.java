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
package org.neo4j.kernel.impl.util;

import static org.neo4j.util.Preconditions.checkState;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import org.neo4j.internal.helpers.collection.NumberAwareStringComparator;
import org.neo4j.io.fs.FileSystemAbstraction;

public class Converters {
    private Converters() {}

    public static <T> Function<String, T> optional() {
        return from -> null;
    }

    private static final Comparator<Path> BY_FILE_NAME = Comparator.comparing(Path::getFileName);

    private static final Comparator<Path> BY_FILE_NAME_WITH_CLEVER_NUMBERS =
            (o1, o2) -> NumberAwareStringComparator.INSTANCE.compare(
                    o1.toAbsolutePath().toString(), o2.toAbsolutePath().toString());

    public static Function<String, Path[]> regexFiles(FileSystemAbstraction fs, boolean cleverNumberRegexSort) {
        return name -> {
            Comparator<Path> sorting = cleverNumberRegexSort ? BY_FILE_NAME_WITH_CLEVER_NUMBERS : BY_FILE_NAME;
            List<Path> files = Validators.matchingFiles(fs, name.trim());
            files.sort(sorting);
            return files.toArray(new Path[0]);
        };
    }

    public static Function<String, Path[]> toFiles(
            final String delimiter, final Function<String, Path[]> eachFileConverter) {
        return from -> {
            if (from == null) {
                return new Path[0];
            }

            String[] names = quotationAwareSplit(from, delimiter);
            List<Path> files = new ArrayList<>();
            for (String name : names) {
                files.addAll(Arrays.asList(eachFileConverter.apply(name)));
            }
            return files.toArray(new Path[0]);
        };
    }

    /**
     * Splits a string by the delimiter, but will not split by delimiters inside ' quatation characters. Example:
     *
     * <pre>
     * The first part,'the second, but longer part',the third part
     * </pre>
     *
     * Will be split into:
     * <ol>
     *     <li>The first part</li>
     *     <li>the second, but longer part</li>
     *     <li>the third part</li>
     * </ol>
     *
     * @param from string to be split into smaller parts.
     * @param delimiter the delimiter to split on.
     * @return an array of parts split from the provided string, where delimiters inside quoted strings will not be split.
     */
    private static String[] quotationAwareSplit(String from, String delimiter) {
        String[] parts = from.split(delimiter);
        List<String> mendedParts = new ArrayList<>();
        for (int i = 0; i < parts.length; i++) {
            String part = parts[i];
            if (part.startsWith("'")) {
                // put back together the parts which were split by a comma, but where inside quotation
                while (!part.endsWith("'")) {
                    checkState(
                            i + 1 < parts.length,
                            "When splitting \"%s\" the inner start quote in part \"%s\" had no matching end quote",
                            from,
                            part);
                    part += delimiter + parts[++i];
                }
                part = part.substring(1, part.length() - 1); // remove the quotation
            }
            mendedParts.add(part);
        }
        return mendedParts.toArray(new String[0]);
    }
}
