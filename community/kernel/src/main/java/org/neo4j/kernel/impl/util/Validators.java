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

import static org.neo4j.cloud.storage.StorageSchemeResolver.isSchemeBased;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import org.neo4j.cloud.storage.SchemeFileSystemAbstraction;
import org.neo4j.common.Validator;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.util.Preconditions;

public final class Validators {

    private Validators() {}

    static List<Path> matchingFiles(FileSystemAbstraction fs, String fileWithRegexInName) {
        List<Path> paths;
        if (isSchemeBased(fileWithRegexInName)) {
            paths = matchingStorageFiles(fs, fileWithRegexInName);
        } else {
            paths = matchingLocalFiles(fileWithRegexInName);
        }

        if (paths.isEmpty()) {
            throw new IllegalArgumentException("File '" + fileWithRegexInName + "' doesn't exist");
        }

        return paths;
    }

    public static final Validator<Path> CONTAINS_EXISTING_DATABASE = dbDir -> {
        try (var fileSystem = new DefaultFileSystemAbstraction()) {
            if (!isExistingDatabase(fileSystem, DatabaseLayout.ofFlat(dbDir))) {
                throw new IllegalArgumentException("Directory '" + dbDir + "' does not contain a database");
            }
        }
    };

    public static boolean isExistingDatabase(FileSystemAbstraction fileSystem, DatabaseLayout layout) {
        return StorageEngineFactory.selectStorageEngine(fileSystem, layout).isPresent();
    }

    public static boolean isExistingDatabase(
            StorageEngineFactory.Selector storageEngineSelector,
            FileSystemAbstraction fileSystem,
            DatabaseLayout layout) {
        return storageEngineSelector.selectStorageEngine(fileSystem, layout).isPresent();
    }

    public static <T> Validator<T> emptyValidator() {
        return value -> {};
    }

    private static List<Path> matchingLocalFiles(String fileWithRegexInName) {
        // Special handling of regex patterns for Windows since Windows paths naturally contains \ characters and also
        // regex can contain those
        // so in order to support this on Windows then \\ will be required in regex patterns and will not be treated as
        // directory delimiter.
        // Get those double backslashes out of the way so that we can trust the File operations to return correct parent
        // etc.
        String parentSafeFileName = fileWithRegexInName.replace("\\\\", "__");
        File absoluteParentSafeFile = new File(parentSafeFileName).getAbsoluteFile();
        File parent = absoluteParentSafeFile.getParentFile();
        Preconditions.checkState(
                parent != null && parent.exists(), "Directory %s of %s doesn't exist", parent, fileWithRegexInName);

        // Then since we can't trust the file operations to do the right thing on Windows if there are regex backslashes
        // we instead
        // get the pattern by cutting off the parent directory from the name manually.
        int fileNameLength = absoluteParentSafeFile.getAbsolutePath().length()
                - parent.getAbsolutePath().length()
                - 1;
        String patternString = fileWithRegexInName
                .substring(fileWithRegexInName.length() - fileNameLength)
                .replace("\\\\", "\\");
        final Pattern pattern = Pattern.compile(patternString);
        List<Path> paths = new ArrayList<>();
        //noinspection DataFlowIssue
        for (File file : parent.listFiles()) {
            if (pattern.matcher(file.getName()).matches()) {
                paths.add(file.toPath());
            }
        }

        return paths;
    }

    private static List<Path> matchingStorageFiles(FileSystemAbstraction fs, String pathWithRegexInName) {
        Preconditions.checkArgument(
                fs instanceof SchemeFileSystemAbstraction,
                "File system provided is not scheme based and cannot resolve the path: " + pathWithRegexInName);
        final var system = (SchemeFileSystemAbstraction) fs;

        // can skip all the backslash-escaping dance as storage paths are always '/' based thankfully
        final var ix = pathWithRegexInName.lastIndexOf("/");
        Preconditions.checkArgument(ix != -1, "Invalid storage path provided: " + pathWithRegexInName);
        final var parentPath = pathWithRegexInName.substring(0, ix);
        final var patternString = pathWithRegexInName.substring(ix + 1);

        try {
            final var parent = system.resolve(parentPath);
            final var pattern = Pattern.compile(patternString);

            final var paths = new ArrayList<Path>();
            for (var child : system.listFiles(parent)) {
                if (pattern.matcher(child.getFileName().toString()).matches()) {
                    paths.add(child);
                }
            }

            return paths;
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }
}
