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
package org.neo4j.dbms.archive;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import org.neo4j.function.Predicates;

/**
 * An ordered representation for a collection of files and directories.
 */
public final class Manifest {

    private final ManifestRecord[] files;

    private Manifest(ManifestRecord... files) {
        this.files = files;
    }

    public static Manifest of(ManifestRecord... records) {
        Map<Path, ManifestRecord> content = new HashMap<>(records.length);
        // Duplicate records are fine, we just ignore them
        for (var record : records) {
            var key = record.target();
            var existing = content.putIfAbsent(key, record);
            if (existing != null) {
                if (existing instanceof DirectoryRecord && record instanceof DirectoryRecord) {
                    // Merge overlapping directories
                } else {
                    throw new IllegalArgumentException(
                            "Duplicate target: %s in %s and %s".formatted(key, record, existing));
                }
            }
        }
        Arrays.sort(records, Comparator.comparing(ManifestRecord::target));
        return new Manifest(records);
    }

    @Override
    public String toString() {
        var sb = new StringBuilder();
        sb.append(getClass().getSimpleName()).append("[");
        for (var record : files) {
            sb.append("\n").append("\t").append(record);
        }
        sb.append("\n]");
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Manifest that = (Manifest) o;
        return Arrays.equals(files, that.files);
    }

    public ManifestRecord[] files() {
        return files;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final List<Path> locations = new ArrayList<>();
        private final List<Predicate<Path>> predicates = new ArrayList<>();

        private Builder() {}

        /**
         * Add folder(s) to the top level of the manifest
         * @param include Filter which files gets included.
         * @param sources The paths which to scan.
         * @return the builder
         */
        public Builder add(Predicate<Path> include, Path... sources) {
            for (Path source : sources) {
                locations.add(source);
                predicates.add(include);
            }
            return this;
        }

        /**
         * Add the folder to top-level of the manifest.
         * @param location The folder to include.
         * @return the builder
         */
        public Builder add(Path location) {
            return add(Predicates.alwaysTrue(), location);
        }

        /**
         * Add everything inside the folder to top-level of the manifest.
         * @param include Filter which files to include.
         * @param location The folder to include.
         * @return the builder
         * @throws IOException if unable to enumerate the content of the folder.
         */
        public Builder addContentsOf(Predicate<Path> include, Path location) throws IOException {
            try (var stream = Files.list(location)) {
                return add(include, stream.toArray(Path[]::new));
            }
        }

        /**
         * Add everything in the folder to top-level of the manifest.
         * @param location the folder to scan for content
         * @return the builder
         */
        public Builder addContentsOf(Path location) throws IOException {
            return addContentsOf(Predicates.alwaysTrue(), location);
        }

        public Manifest build() throws IOException {
            // Process top-level files/directories.
            ArrayList<ManifestRecord> records = new ArrayList<>();
            var include = predicates.iterator();

            for (Path source : locations) {
                // Note: We actively do not want the real path here, we just want to remove
                // any unnecessary `..` and `.` components.
                var absolute = source.normalize().toAbsolutePath();
                List<Path> files;
                try (var stream = Files.walk(absolute)) {
                    files = stream.filter(include.next()).toList();
                }

                for (var src : files) {
                    Path target = absolute.getFileName().resolve(absolute.relativize(src));
                    ManifestRecord record;
                    if (Files.isRegularFile(src, LinkOption.NOFOLLOW_LINKS)) {
                        record = new FileRecord(src, src.toFile().length(), target);
                    } else if (Files.isDirectory(src, LinkOption.NOFOLLOW_LINKS)) {
                        record = new DirectoryRecord(src, target);
                    } else {
                        continue;
                    }
                    records.add(record);
                }
            }
            return Manifest.of(records.toArray(ManifestRecord[]::new));
        }
    }

    public sealed interface ManifestRecord {
        Path source();

        Path target();
    }

    public record DirectoryRecord(Path source, Path target) implements ManifestRecord {
        public DirectoryRecord {
            requireAbsolute(source);
            requireRelative(target);
        }
    }

    public record FileRecord(Path source, long size, Path target) implements ManifestRecord {
        public FileRecord {
            requireAbsolute(source);
            requireRelative(target);
        }
    }

    private static void requireAbsolute(Path target) {
        if (!target.isAbsolute()) {
            throw new IllegalArgumentException("Required absolute path, but got: " + target);
        }
    }

    private static void requireRelative(Path target) {
        if (target.isAbsolute()) {
            throw new IllegalArgumentException("Required relative path, but got: " + target);
        }
    }
}
